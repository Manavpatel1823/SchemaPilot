import json
from typing import Any, Dict

SYSTEM_PROMPT = """
You are a database semantic analyst.

You will receive a structured profile of one database table.
Your job is to generate semantic metadata for retrieval and SQL planning.

Rules:
- Base everything only on the provided schema, relationships, sample rows, and stats.
- Do not hallucinate unsupported business meaning.
- Infer table grain carefully.
- Mention uncertainty if evidence is weak.

You MUST return valid JSON.

Required JSON structure:

{
  "description": "string",
  "purpose": "string",
  "grain": "string",
  "important_columns": ["string"],
  "possible_metrics": ["string"],
  "common_filters": ["string"],
  "warnings": ["string"],
  "example_questions": ["string"]
}

Return ONLY JSON.
Do not include explanations or markdown.
"""

def build_user_prompt(table_profile_payload: Dict[str, Any]) -> str:
    return f"""
Analyze the following database table profile and generate semantic metadata.

Return ONLY valid JSON with keys:
description
purpose
grain
important_columns
possible_metrics
common_filters
warnings
example_questions
common_filters_summary

Table profile:
{json.dumps(table_profile_payload, default=str, indent=2)}
""".strip()


def validate_table_metadata(data: Dict[str, Any]) -> Dict[str, Any]:
    required_list_fields = [
        "important_columns",
        "possible_metrics",
        "common_filters",
        "warnings",
        "example_questions",
    ]

    required_string_fields = [
        "description",
        "purpose",
        "grain",
        "common_filters_summary",
    ]

    for key in required_string_fields:
        if key not in data or not isinstance(data[key], str):
            data[key] = ""

    for key in required_list_fields:
        if key not in data or not isinstance(data[key], list):
            data[key] = []
        else:
            data[key] = [str(x).strip() for x in data[key] if x is not None and str(x).strip()]

    if not data["common_filters_summary"] and data["common_filters"]:
        data["common_filters_summary"] = ", ".join(data["common_filters"])

    return data