import json

def _safe_json_loads(text):
    text = text.strip()

    try:
        return json.loads(text)
    except Exception:
        pass

    start = text.find("{")
    end = text.rfind("}")
    if start != -1 and end != -1 and end > start:
        candidate = text[start : end + 1].strip()
        try:
            return json.loads(candidate)
        except Exception:
            return None

    return None


def plan_from_context(gclient, question, retrieved_docs):
    prompt = f"""
You are a PostgreSQL schema planner.

Your job:
- Read the QUESTION.
- Use ONLY the CONTEXT (retrieved schema docs) to infer the best tables and join endpoints.
- Output STRICT JSON only (no markdown, no explanations).

Important rules:
- Do NOT invent table names or columns not shown in CONTEXT.
- If multiple tables look similar, pick the best match and mention alternatives in "alternatives".
- If you cannot confidently choose start_table/target_table, set "missing_info" with what you need.

Definitions:
- start_table: the main "fact" table (events/transactions/enrollments/orders/logs).
- target_table: the entity table used for filtering/grouping (courses/users/departments/students).
- intent: one of ["count","list","sum","avg","group_by","other"].

CONTEXT:
{chr(10).join(["---\\n" + d for d in retrieved_docs])}

QUESTION:
{question}

Return JSON with this exact shape:
{{
  "tables": ["..."],

  "start_table": "optional",
  "target_table": "optional",

  "intent": "count|list|sum|avg|group_by|other",
  "measure": "optional like COUNT(*) or COUNT(DISTINCT enrollments.student_id)",
  "filters": ["optional human-readable filters"],
  "group_by": ["optional columns or table.column strings"],

  "alternatives": ["optional other table names if ambiguous"],
  "missing_info": "optional string if you cannot plan reliably"
}}
"""
    resp = gclient.models.generate_content(
        model="gemini-2.5-flash", 
        contents=prompt,
    )

    raw = (resp.text or "").strip()
    plan = _safe_json_loads(raw)

    if plan is None or not isinstance(plan, dict):
        return {
            "missing_info": "Planner did not return valid JSON.",
            "raw_output": raw,
        }

    plan.setdefault("tables", [])
    plan.setdefault("filters", [])
    plan.setdefault("group_by", [])
    plan.setdefault("alternatives", [])

    if "intent" not in plan or not plan["intent"]:
        plan["missing_info"] = plan.get("missing_info") or "Missing intent in plan."

    return plan