# main.py
import os
import json
from dotenv import load_dotenv
from google import genai

from db_connect import get_conn
from db_introspect import fetch_fk_edges, fetch_columns
from graph import build_graph, find_join_path, joins_to_sql
from rag_store import retrieve_top_docs
from llm_planner import plan_from_context
from llm_sql_writer import write_sql_from_plan
from query_runner import run_select
from chart_suggest import suggest_chart

def main():
    load_dotenv()

    api_key = os.environ.get("GEMINI_API_KEY")
    if not api_key:
        raise RuntimeError("GEMINI_API_KEY is missing in your .env file.")
    gclient = genai.Client(api_key=api_key)

    question = input("Ask a DB question: ").strip()
    if not question:
        print("Empty question. Exiting.")
        return
    
    conn = get_conn()
    try:
        fk_edges = fetch_fk_edges(conn)
        _columns = fetch_columns(conn)
    finally:
        conn.close()

    tables, deps, refs = build_graph(fk_edges)

    retrieved_docs = retrieve_top_docs(gclient, question, k=12)

    plan = plan_from_context(gclient, question, retrieved_docs)

    if not isinstance(plan, dict):
        print("\nPlanner returned non-dict plan:\n", plan)
        return

    if plan.get("missing_info"):
        print("\nMissing info:\n", plan["missing_info"])
        print("\nRaw output:\n", plan.get("raw_output"))
        return

    queries = plan.get("queries", [])
    if not queries:
        print("No queries found in plan.")
        return

    all_results = []

    for qplan in queries:
        print(f"\n--- Processing query: {qplan.get('label')} ---")

        start_table = qplan.get("start_table")
        target_table = qplan.get("target_table")

        join_path_text = ""
        join_skeleton_sql = ""

        if start_table and target_table:
            path, joins = find_join_path(start_table, target_table, deps, refs, max_depth=6)
            if path:
                join_path_text = " -> ".join(path)
                join_skeleton_sql = joins_to_sql(start_table, joins)

        llm_out = write_sql_from_plan(
            gclient=gclient,
            question=question,
            retrieved_docs=retrieved_docs,
            plan=qplan,
            join_path_text=join_path_text,
            join_skeleton_sql=join_skeleton_sql,
        )

        sql = llm_out.get("sql")
        params = tuple(llm_out.get("params") or [])

        if not sql:
            print("\nSQL generation failed:\n", json.dumps(llm_out, indent=2))
            continue

        conn = get_conn()
        try:
            data = run_select(conn, sql, params)
        finally:
            conn.close()

        chart = suggest_chart(data["columns"], data["rows"])

        result = {
            "label": qplan.get("label"),
            "sql": sql,
            "params": list(params),
            "data": data,
            "chart": chart,
        }
        all_results.append(result)

        # Print a nice preview for CLI
        print("\nSQL:\n", sql)
        print("\nRows:", data["row_count"])
        print("Chart suggestion:", chart)
        print("Preview rows:", data["rows"][:5])

    output = {"question": question, "plan": plan, "results": all_results}
    print("\n=== FULL OUTPUT JSON ===")
    print(json.dumps(output, indent=2))

    save_path = os.path.join("..", "frontend", "public", "last_result.json")
    with open(save_path, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2)

    print(f"\nSaved: {save_path}")


if __name__ == "__main__":
    main()