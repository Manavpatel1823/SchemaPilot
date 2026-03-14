import json
from typing import Any, Dict, List

from db_introspect import (
    build_database_profiles,
    build_llm_table_summary_input,
    build_vector_db_document,
)
from llm_semantics import (
    SYSTEM_PROMPT,
    build_user_prompt,
    validate_table_metadata,
)


def call_llm_for_table(client, model: str, table_payload: Dict[str, Any]) -> Dict[str, Any]:
    user_prompt = build_user_prompt(table_payload)

    fallback = {
        "description": "",
        "purpose": "",
        "grain": "",
        "important_columns": [],
        "possible_metrics": [],
        "common_filters": [],
        "warnings": ["LLM response failed or returned invalid JSON."],
        "example_questions": [],
    }

    try:
        response = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": user_prompt},
            ],
            temperature=0,
        )

        content = response.choices[0].message.content.strip()
        metadata = json.loads(content)
        metadata = validate_table_metadata(metadata)
        return metadata

    except Exception as e:
        fallback["warnings"].append(f"Exception during semantic generation: {str(e)}")
        return validate_table_metadata(fallback)


def build_semantic_documents(conn, client, model: str = "gemini-2.5-flash") -> List[Dict[str, Any]]:

    profiles = build_database_profiles(conn)
    documents: List[Dict[str, Any]] = []

    for table_name, table_profile in profiles.items():
        llm_input = build_llm_table_summary_input(table_profile)

        metadata = call_llm_for_table(
            client=client,
            model=model,
            table_payload=llm_input,
        )

        doc = build_vector_db_document(
            table_profile=table_profile,
            llm_metadata=metadata,
        )

        documents.append(doc)

    return documents


def upsert_documents_to_vector_db(vector_store, documents: List[Dict[str, Any]]) -> None:

    for doc in documents:
        vector_store.upsert(
            doc_id=doc["id"],
            text=doc["text"],
            metadata=doc["metadata"],
        )


def save_documents_locally(documents: List[Dict[str, Any]], output_path: str = "semantic_docs.json") -> None:

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(documents, f, indent=2, ensure_ascii=False, default=str)


def run_semantic_pipeline(conn, client, vector_store=None, model: str = "gemini-2.5-flash") -> List[Dict[str, Any]]:

    documents = build_semantic_documents(conn=conn, client=client, model=model)

    save_documents_locally(documents)

    if vector_store is not None:
        upsert_documents_to_vector_db(vector_store, documents)

    return documents