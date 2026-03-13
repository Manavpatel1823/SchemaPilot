from dotenv import load_dotenv
from db_connect import get_conn
from db_introspect import fetch_fk_edges

def export_graphviz_dot(edges, out_path="schema_graph.dot"):
    lines = []
    lines.append("digraph schema {")

    for e in edges:
        child = e["child_table"]
        parent = e["parent_table"]
        child_col = e["child_column"]
        parent_col = e["parent_column"]

        # label edge with join columns
        label = f'{child_col}→{parent_col}'
        lines.append(f'  "{child}" -> "{parent}" [label="{label}"];')

    lines.append("}")
    dot = "\n".join(lines)

    with open(out_path, "w", encoding="utf-8") as f:
        f.write(dot)

    print(f" Wrote DOT file: {out_path}")
    print("To render PNG (if graphviz installed):")
    print(f"  dot -Tpng {out_path} -o schema_graph.png")


def main():
    load_dotenv()
    conn = get_conn()
    try:
        edges = fetch_fk_edges(conn)
    finally:
        conn.close()

    export_graphviz_dot(edges)


if __name__ == "__main__":
    main()