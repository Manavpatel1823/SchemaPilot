import os
from collections import defaultdict, deque
from dotenv import load_dotenv
import psycopg2

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

def fetch_fk_edges(conn):
    with conn.cursor() as cur:
        cur.execute(FK_QUERY)
        row = cur.fetchall()

    edges = []

    for child_table, child_column, parent_table, parent_column, cname in row:
        edges.append(
            {
                "child_table" : child_table,
                "child_column" : child_column,
                "parent_table": parent_table,
                "parent_column" : parent_column,
                "constraint_name": cname
            }
        )

    return edges

def build_graph(edges):
    node = set()
    deps = defaultdict(list)
    refs = defaultdict(list)
    simple_edges = []

    for e in edges:
        p = e["parent_table"]
        c = e["child_table"] 
        node.add(c)
        node.add(p)

        deps[c].append((p, e["child_column"], e["parent_column"], e["constraint_name"]))
        refs[p].append((c, e["child_column"], e["parent_column"], e["constraint_name"]))
        simple_edges.append((c, p))
    
    return node, deps, refs, simple_edges

def print_edges(edges):
    for e in edges:
        print(f"{e['child_table']}.{e['child_column']} -> {e['parent_table']}.{e['parent_column']}")

def dependency_tree(table, deps, max_depth=10):
    if table not in deps:
        print("Table is not found!!")
        return 

    visited_table = set([table])
    q = deque([(table, 0)])

    while q:
        current, depth = q.popleft()
        if depth >= max_depth:
            continue

        parents = deps.get(current, [])
        for (parent, child_col, parent_col, cname) in parents:
            indent = "  " * (depth + 1)
            print(f"{indent}- {current}.{child_col} -> {parent}.{parent_col}  ({cname})")


            if parent not in visited_table:
                visited_table.add(parent)
                q.append((parent, depth+1))


def dependent_tree(table, refs, max_depth=10):
    if table not in refs:
        print(f"'{table}' is not referenced by other tables (no dependents) OR table not found in FK list.")
        return

    visited_tables = set([table])
    q = deque([(table, 0)])

    while q:
        current, depth = q.popleft()
        if depth >= max_depth:
            continue

        children = refs.get(current, [])
        for (child, child_col, parent_col, cname) in children:
            indent = "  " * (depth + 1)
            print(f"{indent}- {child}.{child_col} -> {current}.{parent_col}  ({cname})")

            if child not in visited_tables:
                visited_tables.add(child)
                q.append((child, depth + 1))

def export_graphviz_dot(simple_edges, out_path="schema_graph.dot"):
    lines = []
    lines.append("digraph schema {")
    lines.append('  rankdir=LR;')  # left to right
    lines.append('  node [shape=box];')

    for child, parent in sorted(set(simple_edges)):
        lines.append(f'  "{child}" -> "{parent}";')

    lines.append("}")
    dot = "\n".join(lines)

    with open(out_path, "w", encoding="utf-8") as f:
        f.write(dot)

    print(f"\nDOT file written: {out_path}")
    print("To render (if graphviz installed):")
    print(f"  dot -Tpng {out_path} -o schema_graph.png")


def main():

    load_dotenv()
    db_url = os.getenv("DATABASE_URL")
    if db_url:
        conn = psycopg2.connect(db_url)
    else:
        conn = psycopg2.connect(
            host="localhost",
            port=5432,
            dbname="data_analysis",
            user="manav",
            password="postgres",
        )
    
    try:
        edges = fetch_fk_edges(conn)
        print_edges(edges)

        node, deps, refs, simple_edges = build_graph(edges)
        dependency_tree("enrollments", deps)
        dependent_tree("departments", refs)

    finally:
        conn.close()

if __name__ == "__main__":
    main()

    
