import os
from dotenv import load_dotenv
import chromadb
from google import genai

from db_connect import get_conn
from db_introspect import fetch_fk_edges, fetch_columns

EMBED_MODEL = os.getenv("EMBED_MODEL", "gemini-embedding-2-preview")

def make_schema_docs(columns_by_table, fk_edges):
    docs = []

    # 1) TABLE docs: table name + columns + types
    for table, cols in columns_by_table.items():
        col_lines = "\n".join(
            [f'- {c["column"]} ({c["type"]}), nullable={c["nullable"]}' for c in cols]
        )
        text = f"TABLE: {table}\nCOLUMNS:\n{col_lines}"
        docs.append(
            {
                "id": f"table::{table}",
                "text": text,
                "meta": {"type": "table", "table": table},
            }
        )

    # 2) FK docs: each FK relation as one short doc
    for e in fk_edges:
        text = (
            f"FOREIGN KEY: {e['constraint_name']}\n"
            f"{e['child_table']}.{e['child_column']} -> "
            f"{e['parent_table']}.{e['parent_column']}"
        )
        docs.append(
            {
                "id": f"fk::{e['constraint_name']}",
                "text": text,
                "meta": {
                    "type": "fk",
                    "child_table": e["child_table"],
                    "parent_table": e["parent_table"],
                },
            }
        )

    return docs


def make_alias_docs():
    aliases = [
        ("alias::student_std", "ALIAS: student synonyms: std, learner, pupil."),
        ("alias::user_person", "ALIAS: user synonyms: person, customer, client."),
        ("alias::course_class", "ALIAS: course synonyms: class, subject, offering."),
        ("alias::enrollment_registration", "ALIAS: enrollment synonyms: registration, roster."),
    ]

    return [{"id": i, "text": t, "meta": {"type": "alias"}} for i, t in aliases]


def embed_texts(gclient, texts):
    res = gclient.models.embed_content(
        model=EMBED_MODEL,
        contents=texts,
    )
    return [e.values for e in res.embeddings]


def get_collection():
    load_dotenv()
    chroma_dir = os.getenv("CHROMA_DIR", "./chroma_db")

    client = chromadb.PersistentClient(path=chroma_dir)
    return client.get_or_create_collection(name="schema")


def main():
    load_dotenv()

    api_key = os.environ.get("GEMINI_API_KEY")
    if not api_key:
        raise RuntimeError("GEMINI_API_KEY is missing in your .env file.")
    gclient = genai.Client(api_key=api_key)

    # 1) Load schema from Postgres
    conn = get_conn()
    try:
        fk_edges = fetch_fk_edges(conn)
        columns_by_table = fetch_columns(conn)
    finally:
        conn.close()

    # 2) Convert schema to docs
    docs = make_schema_docs(columns_by_table, fk_edges)

    # 3) Add alias docs (optional but helpful)
    docs += make_alias_docs()

    # 4) Embed docs
    vectors = embed_texts(gclient, [d["text"] for d in docs])

    # 5) Store in Chroma
    col = get_collection()
    col.upsert(
        ids=[d["id"] for d in docs],
        documents=[d["text"] for d in docs],
        metadatas=[d["meta"] for d in docs],
        embeddings=vectors,
    )

    print(f"Indexed {len(docs)} docs into Chroma collection 'schema'.")


if __name__ == "__main__":
    main()
