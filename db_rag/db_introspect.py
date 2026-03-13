# db_introspect.py
from collections import defaultdict

# 1) Foreign key query:
# Each row = one FK column mapping: child_table.child_column -> parent_table.parent_column
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

# 2) Column query:
# Each row = one column with its type and nullable flag
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


def fetch_fk_edges(conn):
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


def fetch_columns(conn):
    columns_by_table = defaultdict(list)

    with conn.cursor() as cur:
        cur.execute(COLUMNS_QUERY)
        rows = cur.fetchall()

    for table_name, column_name, data_type, is_nullable in rows:
        columns_by_table[table_name].append(
            {
                "column": column_name,
                "type": data_type,
                "nullable": is_nullable, 
            }
        )

    return dict(columns_by_table)