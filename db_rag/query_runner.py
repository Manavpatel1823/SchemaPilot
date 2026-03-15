# query_runner.py

from typing import Any, Dict, Tuple
import re
import psycopg2.extras


READ_ONLY_STARTS = ("select", "with")


def _normalize_sql(sql: str) -> str:
    if not sql:
        return ""

    s = sql.strip()

    while s.startswith("--"):
        newline_idx = s.find("\n")
        if newline_idx == -1:
            return ""
        s = s[newline_idx + 1 :].lstrip()

    # Remove leading block comments: /* ... */
    while s.startswith("/*"):
        end_idx = s.find("*/")
        if end_idx == -1:
            return ""
        s = s[end_idx + 2 :].lstrip()

    return s


def _is_read_only_sql(sql: str) -> bool:
    s = _normalize_sql(sql).lower()
    return s.startswith(READ_ONLY_STARTS)


def run_select(conn, sql: str, params: Tuple[Any, ...] = ()) -> Dict[str, Any]:
    if not _is_read_only_sql(sql):
        raise ValueError("Only read-only SELECT queries are allowed in run_select().")

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description] if cur.description else []

    return {
        "columns": columns,
        "rows": rows,
        "row_count": len(rows),
    }