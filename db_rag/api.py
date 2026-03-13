# api.py
import os
import re
from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from google import genai

from db_connect import get_conn
from db_introspect import fetch_fk_edges, fetch_columns
from graph import build_graph, find_join_path, joins_to_sql
from rag_store import retrieve_top_docs
from llm_planner import plan_from_context
from llm_sql_writer import write_sql_from_plan  # should return dict: {"sql": "...", "params": [...]}
from query_runner import run_select
from chart_suggest import suggest_chart

load_dotenv()

app = FastAPI(title="AI Analytics Agent API")

# Allow React dev server to call this API
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "http://127.0.0.1:5173",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class AskRequest(BaseModel):
    question: str


def _extract_sql_fallback(text: str) -> str:
    """
    Fallback if LLM returns a plain text response (not JSON).
    Extract SQL from ```sql ...``` or from 'SQL:' prefix.
    """
    if not text:
        return ""

    t = text.strip()

    m = re.search(r"```sql\s*(.*?)```", t, re.DOTALL | re.IGNORECASE)
    if m:
        return m.group(1).strip()

    # If it starts with SQL:
    if t.lower().startswith("sql:"):
        t = t.split(":", 1)[1].strip()

    # Keep only first statement if multiple accidentally appear
    # (basic safety)
    semi = t.find(";")
    if semi != -1:
        return t[: semi + 1].strip()

    return t


# Create Gemini client once at startup
api_key = os.environ.get("GEMINI_API_KEY")
if not api_key:
    raise RuntimeError("GEMINI_API_KEY missing in .env")
gclient = genai.Client(api_key=api_key)

GEN_MODEL = os.getenv("GEN_MODEL", "gemini-2.5-flash")


@app.post("/ask")
def ask(req: AskRequest):
    question = (req.question or "").strip()
    if not question:
        return {"error": "Empty question"}

    # 1) Load schema live (FK graph + columns for retrieval context)
    conn = get_conn()
    try:
        fk_edges = fetch_fk_edges(conn)
        _cols = fetch_columns(conn)
    finally:
        conn.close()

    tables, deps, refs = build_graph(fk_edges)

    # 2) RAG retrieve schema docs
    retrieved_docs = retrieve_top_docs(gclient, question, k=12)

    # 3) Planner -> plan dict with "queries"
    plan = plan_from_context(gclient, question, retrieved_docs)

    if not isinstance(plan, dict):
        return {
            "question": question,
            "error": "Planner returned non-dict",
            "raw_plan": plan,
        }

    if plan.get("missing_info"):
        return {
            "question": question,
            "plan": plan,
            "missing_info": plan.get("missing_info"),
            "raw_output": plan.get("raw_output"),
        }

    queries = plan.get("queries") or []
    if not queries:
        return {
            "question": question,
            "plan": plan,
            "error": "No queries found in plan",
        }

    results = []

    # 4) For each planned query: compute join skeleton -> generate SQL -> execute -> chart
    for qplan in queries:
        label = qplan.get("label") or "query"

        start_table = qplan.get("start_table")
        target_table = qplan.get("target_table")

        join_path_text = ""
        join_skeleton_sql = ""

        if start_table and target_table:
            path, joins = find_join_path(start_table, target_table, deps, refs, max_depth=6)
            if path:
                join_path_text = " -> ".join(path)
                join_skeleton_sql = joins_to_sql(start_table, joins)

        # 5) SQL writer (prefer JSON output)
        llm_out = write_sql_from_plan(
            gclient=gclient,
            question=question,
            retrieved_docs=retrieved_docs,
            plan=qplan,
            join_path_text=join_path_text,
            join_skeleton_sql=join_skeleton_sql,
            model_name=GEN_MODEL,
        )

        # llm_out should be dict: {"sql": "...", "params": [...]}
        sql = None
        params = ()

        if isinstance(llm_out, dict):
            sql = llm_out.get("sql")
            params = tuple(llm_out.get("params") or [])
        else:
            # fallback if writer accidentally returns text
            sql = _extract_sql_fallback(str(llm_out))
            params = ()

        if not sql:
            results.append({
                "label": label,
                "error": "SQL generation failed",
                "llm_out": llm_out,
            })
            continue

        # 6) Execute SELECT safely
        conn = get_conn()
        try:
            data = run_select(conn, sql, params)
        except Exception as e:
            results.append({
                "label": label,
                "sql": sql,
                "params": list(params),
                "error": f"Execution error: {type(e).__name__}: {e}",
            })
            continue
        finally:
            conn.close()

        chart = suggest_chart(data["columns"], data["rows"])

        results.append({
            "label": label,
            "sql": sql,
            "params": list(params),
            "data": data,
            "chart": chart,
        })

    return {
        "question": question,
        "plan": plan,
        "results": results,
    }