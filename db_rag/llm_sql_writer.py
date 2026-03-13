import json

def write_sql_from_plan(
    gclient,
    question,
    retrieved_docs,
    plan,
    join_path_text="",
    join_skeleton_sql="",
    model_name="gemini-2.5-flash",
):

    plan_json = json.dumps(plan, indent=2)
    context_block = "\n".join(["---\n" + d for d in retrieved_docs])

    join_block = ""
    if join_skeleton_sql:
        join_block = f"""
JOIN INFORMATION (use exactly this join skeleton; do NOT change join keys):
{join_skeleton_sql}

JOIN PATH (for explanation only):
{join_path_text}
"""
    else:
        join_block = """
JOIN INFORMATION:
No join skeleton provided. If the question requires joins, say what is missing.
Only write single-table SQL if it is clearly possible from the context.
"""

    prompt = f"""
You are a PostgreSQL query writer.

RULES (very important):
- Use ONLY the tables/columns that appear in the CONTEXT below.
-Use proper name not give random table name like t1.column_name, t2.column_name, use table name.
- Do NOT invent table names or columns.
- If a join is required, use the provided JOIN SKELETON exactly.
- Use parameter placeholders like $1, $2 instead of embedding user values directly.
- Only generate SELECT queries.
- Return STRICT JSON only. Do not add markdown. Do not add explanations outside JSON.

CONTEXT:
{context_block}

PLAN (JSON):
{plan_json}

{join_block}

QUESTION:
{question}

OUTPUT FORMAT (STRICT JSON):

{{
  "sql": "single SELECT statement ending with semicolon",
  "params": [],
  "explanation": ["short bullet 1", "short bullet 2"]
}}
"""

    resp = gclient.models.generate_content(
        model=model_name,
        contents=prompt,
    )

    text = (resp.text or "").strip()

    # Safe JSON parsing
    try:
        return json.loads(text)
    except Exception:
        # attempt to extract JSON block
        start = text.find("{")
        end = text.rfind("}")
        if start != -1 and end != -1:
            try:
                return json.loads(text[start:end+1])
            except Exception:
                pass

        return {
            "sql": None,
            "params": [],
            "explanation": ["Invalid JSON from model"],
            "raw_output": text
        }