from typing import Any, Dict, List, Tuple
import psycopg2

def run_select(conn, sql: str, params: Tuple[Any, ...] = ()) -> Dict[str, Any]:
    sql_stripped = sql.strip().lower()
    if not sql_stripped.startswith("select"):
        raise ValueError("Only SELECT queries are allowed in run_select().")
    
    with conn.cursor() as cur:
        cur.execute(sql, params)
        colnames = [d.name for d in cur.description]
        raw_rows = cur.fetchall()
    
    rows = [dict(zip(colnames, r)) for r in raw_rows]

    return {
        "columns": colnames,
        "rows": rows,
        "row_count": len(rows),
    }