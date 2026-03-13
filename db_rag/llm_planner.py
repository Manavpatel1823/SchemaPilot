import json

def _safe_json_loads(text):
    text = text.strip()

    # Try direct parse
    try:
        return json.loads(text)
    except Exception:
        pass

    # Try to extract JSON block manually
    start = text.find("{")
    end = text.rfind("}")
    if start != -1 and end != -1 and end > start:
        candidate = text[start:end+1]
        try:
            return json.loads(candidate)
        except Exception:
            return None

    return None

def plan_from_context(gclient, question, retrieved_docs):

    prompt = f"""
You are a PostgreSQL schema planner.

Using ONLY the CONTEXT below, create a query plan.

If the QUESTION asks for multiple independent results
(e.g., students count AND departments count),
return multiple entries inside "queries".

Return STRICT JSON:

{{
  "queries": [
    {{
      "label": "short_name",
      "tables": ["..."],
      "start_table": "...",
      "target_table": "optional",
      "intent": "count|list|sum|avg|group_by|other",
      "measure": "optional",
      "filters": [],
      "group_by": []
    }}
  ],
  "missing_info": null
}}

CONTEXT:
{chr(10).join(retrieved_docs)}

QUESTION:
{question}
"""

    resp = gclient.models.generate_content(
        model="gemini-2.5-flash",
        contents=prompt,
    )

    plan = _safe_json_loads(resp.text)
    if not plan:
        return {
            "missing_info": "Invalid JSON",
            "raw_output": resp.text
        }
    return plan