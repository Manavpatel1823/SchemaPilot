# index_semantic_schema.py

import os
import json
from dotenv import load_dotenv
import chromadb
from google import genai

from db_connect import get_conn
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

EMBED_MODEL = os.getenv("EMBED_MODEL", "gemini-embedding-2-preview")
GEN_MODEL = os.getenv("GEN_MODEL", "gemini-2.5-flash")


def get_collection():
    load_dotenv()
    chroma_dir = os.getenv("CHROMA_DIR", "./chroma_db")
    client = chromadb.PersistentClient(path=chroma_dir)
    return client.get_or_create_collection(name="semantic_schema")


def embed_texts(gclient, texts):
    res = gclient.models.embed_content(
        model=EMBED_MODEL,
        contents=texts,
    )
    return [e.values for e in res.embeddings]


def generate_table_metadata(gclient, table_payload):
    prompt = build_user_prompt(table_payload)

    full_prompt = f"""
{SYSTEM_PROMPT}

Table profile:
{prompt}
""".strip()

    fallback = {
        "description": "",
        "purpose": "",
        "grain": "",
        "important_columns": [],
        "possible_metrics": [],
        "common_filters": [],
        "warnings": ["LLM response failed or returned invalid JSON."],
        "example_questions": [],
        "common_filters_summary": "",
    }

    try:
        response = gclient.models.generate_content(
            model=GEN_MODEL,
            contents=full_prompt,
        )

        text = response.text.strip()
        data = json.loads(text)
        data = validate_table_metadata(data)

        if "common_filters_summary" not in data:
            data["common_filters_summary"] = ", ".join(data.get("common_filters", []))

        return data

    except Exception as e:
        fallback["warnings"].append(f"Semantic generation error: {str(e)}")
        return validate_table_metadata(fallback)


def main():
    load_dotenv()

    api_key = os.environ.get("GEMINI_API_KEY")
    if not api_key:
        raise RuntimeError("GEMINI_API_KEY is missing in your .env file.")
    gclient = genai.Client(api_key=api_key)

    conn = get_conn()
    try:
        profiles = build_database_profiles(conn, sample_limit=10)
    finally:
        conn.close()

    docs = []

    for table_name, table_profile in profiles.items():
        print(f"Generating semantic doc for table: {table_name}")

        llm_input = build_llm_table_summary_input(table_profile)
        metadata = generate_table_metadata(gclient, llm_input)

        if not metadata.get("common_filters_summary"):
            metadata["common_filters_summary"] = ", ".join(metadata.get("common_filters", []))

        doc = build_vector_db_document(
            table_profile=table_profile,
            llm_metadata=metadata,
        )

        docs.append(doc)

    if not docs:
        print("No semantic docs were created.")
        return

    vectors = embed_texts(gclient, [d["text"] for d in docs])

    col = get_collection()
    col.upsert(
        ids=[d["id"] for d in docs],
        documents=[d["text"] for d in docs],
        metadatas=[d["metadata"] for d in docs],
        embeddings=vectors,
    )

    print(f"Indexed {len(docs)} docs into Chroma collection 'semantic_schema'.")


if __name__ == "__main__":
    main()