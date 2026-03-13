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

    # Convert plan to a stable string for prompt
    plan_json = json.dumps(plan, indent=2)

    # Make the context readable in the prompt
    context_block = "\n".join(["---\n" + d for d in retrieved_docs])

    # Join instructions: the LLM must reuse skeleton, not re-derive join keys.
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
- Do NOT invent table names or columns.
- If a join is required, use the provided JOIN SKELETON exactly.
- Use parameter placeholders like $1, $2 instead of embedding user values directly.
- Output SQL first, then a brief explanation.

CONTEXT:
{context_block}

PLAN (JSON):
{plan_json}

{join_block}

QUESTION:
{question}

OUTPUT FORMAT:
SQL:
<single/Multiple SQL statement>

Explanation:
- bullet 1
- bullet 2
"""

    resp = gclient.models.generate_content(
        model=model_name,
        contents=prompt,
    )

    return (resp.text or "").strip()