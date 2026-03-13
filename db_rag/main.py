# main.py
import os
import json
from dotenv import load_dotenv
from google import genai

from db_connect import get_conn
from db_introspect import fetch_fk_edges, fetch_columns
from graph import build_graph, find_join_path, joins_to_sql
from rag_store import embed_text, retrieve_top_docs
from llm_planner import plan_from_context
from llm_sql_writer import write_sql_from_plan


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
        columns = fetch_columns(conn)         
    finally:
        conn.close()

    tables, deps, refs = build_graph(fk_edges)
    retrieved_docs = retrieve_top_docs(gclient, question, k=12)

    plan = plan_from_context(gclient, question, retrieved_docs)

    if not isinstance(plan, dict):
        print("\nPlanner returned non-dict plan. Output was:\n", plan)
        return

    if plan.get("missing_info"):
        print("\n Missing info:\n", plan["missing_info"])
        return

    queries = plan.get("queries", [])

    if not queries:
        print("No queries found in plan.")
        return
    
    for qplan in queries:
        print(f"\n--- Processing query: {qplan.get('label')} ---")

        start_table = qplan.get("start_table")
        target_table = qplan.get("target_table")

        join_path_text = ""
        join_skeleton_sql = ""

        if start_table and target_table:
            path, joins = find_join_path(start_table, target_table, deps, refs, max_depth=6)

            if path:
                join_path_text = " -> ".join(path) + "\n" + "\n".join(
                    [f'{j["left_table"]}.{j["left_col"]} = {j["right_table"]}.{j["right_col"]}'
                    for j in joins]
                )

                join_skeleton_sql = joins_to_sql(start_table, joins)

        answer = write_sql_from_plan(
            gclient=gclient,
            question=question,
            retrieved_docs=retrieved_docs,
            plan=qplan,
            join_path_text=join_path_text,
            join_skeleton_sql=join_skeleton_sql,
        )

        print("\n=== SQL OUTPUT ===")
        print(answer)   


if __name__ == "__main__":
    main()