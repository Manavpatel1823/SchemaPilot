# db_introspect.py

from collections import defaultdict
from typing import Any, Dict, List, Optional
from psycopg2 import sql

FK_QUERY = """
SELECT
  tc.table_name            AS child_table,
  kcu.column_name          AS child_column,
  ccu.table_name           AS parent_table,
  ccu.column_name          AS parent_column,
  tc.constraint_name       AS constraint_name
FROM information_schema.table_constraints tc
JOIN information_schema.key_column_usage kcu
  ON tc.constraint_name = kcu.constraint_name
 AND tc.table_schema = kcu.table_schema
JOIN information_schema.constraint_column_usage ccu
  ON ccu.constraint_name = tc.constraint_name
 AND ccu.table_schema = tc.table_schema
WHERE tc.constraint_type = 'FOREIGN KEY'
  AND tc.table_schema = 'public'
ORDER BY child_table, child_column;
"""

COLUMNS_QUERY = """
SELECT
  table_name,
  column_name,
  data_type,
  is_nullable
FROM information_schema.columns
WHERE table_schema = 'public'
ORDER BY table_name, ordinal_position;
"""

PRIMARY_KEYS_QUERY = """
SELECT
  tc.table_name,
  kcu.column_name
FROM information_schema.table_constraints tc
JOIN information_schema.key_column_usage kcu
  ON tc.constraint_name = kcu.constraint_name
 AND tc.table_schema = kcu.table_schema
WHERE tc.constraint_type = 'PRIMARY KEY'
  AND tc.table_schema = 'public'
ORDER BY tc.table_name, kcu.ordinal_position;
"""

TABLES_QUERY = """
SELECT table_name
FROM information_schema.tables
WHERE table_schema = 'public'
  AND table_type = 'BASE TABLE'
ORDER BY table_name;
"""


def fetch_all_tables(conn) -> List[str]:
    with conn.cursor() as cur:
        cur.execute(TABLES_QUERY)
        rows = cur.fetchall()
    return [row[0] for row in rows]


def fetch_fk_edges(conn) -> List[Dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(FK_QUERY)
        rows = cur.fetchall()

    edges = []
    for child_table, child_column, parent_table, parent_column, cname in rows:
        edges.append(
            {
                "child_table": child_table,
                "child_column": child_column,
                "parent_table": parent_table,
                "parent_column": parent_column,
                "constraint_name": cname,
            }
        )
    return edges


def fetch_columns(conn) -> Dict[str, List[Dict[str, Any]]]:
    columns_by_table = defaultdict(list)

    with conn.cursor() as cur:
        cur.execute(COLUMNS_QUERY)
        rows = cur.fetchall()

    for table_name, column_name, data_type, is_nullable in rows:
        columns_by_table[table_name].append(
            {
                "column": column_name,
                "type": data_type,
                "nullable": is_nullable == "YES",
            }
        )

    return dict(columns_by_table)


def fetch_primary_keys(conn) -> Dict[str, List[str]]:
    pk_by_table = defaultdict(list)

    with conn.cursor() as cur:
        cur.execute(PRIMARY_KEYS_QUERY)
        rows = cur.fetchall()

    for table_name, column_name in rows:
        pk_by_table[table_name].append(column_name)

    return dict(pk_by_table)


def fetch_table_row_count(conn, table_name: str) -> int:
    query = sql.SQL("SELECT COUNT(*) FROM {}").format(sql.Identifier(table_name))
    with conn.cursor() as cur:
        cur.execute(query)
        row = cur.fetchone()
    return int(row[0]) if row else 0


def fetch_sample_rows(
    conn,
    table_name: str,
    limit: int = 10,
    order_by: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Fetch a few sample rows from a table.
    Use order_by if you want stable examples like latest rows.
    """
    if order_by:
        query = sql.SQL("SELECT * FROM {} ORDER BY {} LIMIT %s").format(
            sql.Identifier(table_name),
            sql.Identifier(order_by),
        )
        params = (limit,)
    else:
        query = sql.SQL("SELECT * FROM {} LIMIT %s").format(sql.Identifier(table_name))
        params = (limit,)

    with conn.cursor() as cur:
        cur.execute(query, params)
        rows = cur.fetchall()
        colnames = [desc[0] for desc in cur.description]

    sample_rows = []
    for row in rows:
        sample_rows.append(dict(zip(colnames, row)))

    return sample_rows


def fetch_low_cardinality_values(
    conn,
    table_name: str,
    columns: List[Dict[str, Any]],
    max_distinct: int = 20,
    top_k: int = 10,
) -> Dict[str, List[Any]]:
    """
    For text-like / boolean-like columns, collect common values when distinct count is small.
    Useful for status, category, segment, etc.
    """
    interesting_types = {
        "character varying",
        "varchar",
        "text",
        "char",
        "character",
        "boolean",
    }

    result = {}

    for col in columns:
        col_name = col["column"]
        data_type = col["type"]

        if data_type not in interesting_types:
            continue

        distinct_count_query = sql.SQL("""
            SELECT COUNT(DISTINCT {col})
            FROM {table}
            WHERE {col} IS NOT NULL
        """).format(
            col=sql.Identifier(col_name),
            table=sql.Identifier(table_name),
        )

        with conn.cursor() as cur:
            cur.execute(distinct_count_query)
            distinct_count = cur.fetchone()[0] or 0

        if distinct_count == 0 or distinct_count > max_distinct:
            continue

        values_query = sql.SQL("""
            SELECT {col}, COUNT(*) AS freq
            FROM {table}
            WHERE {col} IS NOT NULL
            GROUP BY {col}
            ORDER BY freq DESC, {col}
            LIMIT %s
        """).format(
            col=sql.Identifier(col_name),
            table=sql.Identifier(table_name),
        )

        with conn.cursor() as cur:
            cur.execute(values_query, (top_k,))
            rows = cur.fetchall()

        result[col_name] = [row[0] for row in rows]

    return result


def fetch_numeric_and_date_stats(
    conn,
    table_name: str,
    columns: List[Dict[str, Any]],
) -> Dict[str, Dict[str, Any]]:
    """
    Get min/max for numeric and date-like columns.
    This helps LLM understand which fields are useful for metrics and trends.
    """
    numeric_types = {
        "smallint",
        "integer",
        "bigint",
        "numeric",
        "real",
        "double precision",
        "decimal",
    }
    date_types = {
        "date",
        "timestamp without time zone",
        "timestamp with time zone",
    }

    stats = {}

    for col in columns:
        col_name = col["column"]
        data_type = col["type"]

        if data_type in numeric_types:
            query = sql.SQL("""
                SELECT
                  MIN({col}),
                  MAX({col}),
                  AVG({col}),
                  COUNT({col}),
                  COUNT(*) - COUNT({col}) AS null_count
                FROM {table}
            """).format(
                col=sql.Identifier(col_name),
                table=sql.Identifier(table_name),
            )

            with conn.cursor() as cur:
                cur.execute(query)
                min_v, max_v, avg_v, non_null_count, null_count = cur.fetchone()

            stats[col_name] = {
                "kind": "numeric",
                "min": min_v,
                "max": max_v,
                "avg": float(avg_v) if avg_v is not None else None,
                "non_null_count": non_null_count,
                "null_count": null_count,
            }

        elif data_type in date_types:
            query = sql.SQL("""
                SELECT
                  MIN({col}),
                  MAX({col}),
                  COUNT({col}),
                  COUNT(*) - COUNT({col}) AS null_count
                FROM {table}
            """).format(
                col=sql.Identifier(col_name),
                table=sql.Identifier(table_name),
            )

            with conn.cursor() as cur:
                cur.execute(query)
                min_v, max_v, non_null_count, null_count = cur.fetchone()

            stats[col_name] = {
                "kind": "date",
                "min": min_v.isoformat() if min_v is not None else None,
                "max": max_v.isoformat() if max_v is not None else None,
                "non_null_count": non_null_count,
                "null_count": null_count,
            }

    return stats


def fetch_null_ratios(
    conn,
    table_name: str,
    columns: List[Dict[str, Any]],
) -> Dict[str, float]:
    """
    Null ratio for each column.
    """
    row_count = fetch_table_row_count(conn, table_name)
    if row_count == 0:
        return {col["column"]: 0.0 for col in columns}

    ratios = {}
    for col in columns:
        col_name = col["column"]
        query = sql.SQL("""
            SELECT COUNT(*) - COUNT({col}) AS null_count
            FROM {table}
        """).format(
            col=sql.Identifier(col_name),
            table=sql.Identifier(table_name),
        )

        with conn.cursor() as cur:
            cur.execute(query)
            null_count = cur.fetchone()[0] or 0

        ratios[col_name] = round(null_count / row_count, 4)

    return ratios


def find_table_relationships(
    table_name: str,
    fk_edges: List[Dict[str, Any]],
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Split relationships into outgoing and incoming from the perspective of one table.
    """
    outgoing = []
    incoming = []

    for edge in fk_edges:
        if edge["child_table"] == table_name:
            outgoing.append(edge)
        if edge["parent_table"] == table_name:
            incoming.append(edge)

    return {
        "outgoing_fks": outgoing,
        "incoming_fks": incoming,
    }


def build_table_profile(
    conn,
    table_name: str,
    all_columns: Dict[str, List[Dict[str, Any]]],
    primary_keys: Dict[str, List[str]],
    fk_edges: List[Dict[str, Any]],
    sample_limit: int = 10,
) -> Dict[str, Any]:
    """
    Build one compact but rich profile for a table.
    This is the main artifact you send to the LLM first.
    """
    columns = all_columns.get(table_name, [])
    relationships = find_table_relationships(table_name, fk_edges)

    profile = {
        "table_name": table_name,
        "row_count": fetch_table_row_count(conn, table_name),
        "primary_keys": primary_keys.get(table_name, []),
        "columns": columns,
        "relationships": relationships,
        "sample_rows": fetch_sample_rows(conn, table_name, limit=sample_limit),
        "low_cardinality_values": fetch_low_cardinality_values(conn, table_name, columns),
        "numeric_and_date_stats": fetch_numeric_and_date_stats(conn, table_name, columns),
        "null_ratios": fetch_null_ratios(conn, table_name, columns),
    }

    return profile


def build_llm_table_summary_input(table_profile: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "table_name": table_profile["table_name"],
        "row_count": table_profile["row_count"],
        "primary_keys": table_profile["primary_keys"],
        "columns": table_profile["columns"],
        "relationships": table_profile["relationships"],
        "sample_rows": table_profile["sample_rows"],
        "low_cardinality_values": table_profile["low_cardinality_values"],
        "numeric_and_date_stats": table_profile["numeric_and_date_stats"],
        "null_ratios": table_profile["null_ratios"],
        "instructions": {
            "goal": (
                "Generate semantic metadata for this table. "
                "Infer table purpose, grain, important columns, useful metrics, "
                "common filters, warnings, and example business questions."
            ),
            "do_not_hallucinate": True,
            "base_only_on_evidence": True,
        },
    }


def build_vector_db_document(
    table_profile: Dict[str, Any],
    llm_metadata: Dict[str, Any],
) -> Dict[str, Any]:
    table_name = table_profile["table_name"]

    doc_text = f"""
Table: {table_name}

Description:
{llm_metadata.get("description", "")}

Grain:
{llm_metadata.get("grain", "")}

Purpose:
{llm_metadata.get("purpose", "")}

Primary Keys:
{", ".join(table_profile.get("primary_keys", []))}

Columns:
{chr(10).join(
    f"- {c['column']} ({c['type']}, nullable={c['nullable']})"
    for c in table_profile.get("columns", [])
)}

Relationships:
Outgoing foreign keys:
{chr(10).join(
    f"- {r['child_table']}.{r['child_column']} -> {r['parent_table']}.{r['parent_column']}"
    for r in table_profile.get("relationships", {}).get("outgoing_fks", [])
) or "- none"}

Incoming foreign keys:
{chr(10).join(
    f"- {r['child_table']}.{r['child_column']} -> {r['parent_table']}.{r['parent_column']}"
    for r in table_profile.get("relationships", {}).get("incoming_fks", [])
) or "- none"}

Common categorical values:
{llm_metadata.get("common_filters_summary", "")}

Important columns:
{chr(10).join(f"- {x}" for x in llm_metadata.get("important_columns", [])) or "- none"}

Possible metrics:
{chr(10).join(f"- {x}" for x in llm_metadata.get("possible_metrics", [])) or "- none"}

Warnings:
{chr(10).join(f"- {x}" for x in llm_metadata.get("warnings", [])) or "- none"}

Example questions:
{chr(10).join(f"- {x}" for x in llm_metadata.get("example_questions", [])) or "- none"}
""".strip()

    return {
    "id": f"table::{table_name}",
    "table_name": table_name,
    "text": doc_text,
    "metadata": {
        "table_name": table_name,
        "row_count": table_profile.get("row_count", 0),
        "primary_keys": ", ".join(table_profile.get("primary_keys", [])),
        "description": llm_metadata.get("description", ""),
        "grain": llm_metadata.get("grain", ""),
        "important_columns": ", ".join(llm_metadata.get("important_columns", [])),
        "possible_metrics": ", ".join(llm_metadata.get("possible_metrics", [])),
        "warnings": " | ".join(llm_metadata.get("warnings", [])),
    },
}


def build_database_profiles(conn, sample_limit: int = 10) -> Dict[str, Dict[str, Any]]:
    tables = fetch_all_tables(conn)
    columns = fetch_columns(conn)
    primary_keys = fetch_primary_keys(conn)
    fk_edges = fetch_fk_edges(conn)

    profiles = {}
    for table_name in tables:
        profiles[table_name] = build_table_profile(
            conn=conn,
            table_name=table_name,
            all_columns=columns,
            primary_keys=primary_keys,
            fk_edges=fk_edges,
            sample_limit=sample_limit,
        )

    return profiles