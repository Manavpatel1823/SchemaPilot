# rag_store.py
import os
from dotenv import load_dotenv
import chromadb

EMBED_MODEL = os.getenv("EMBED_MODEL", "gemini-embedding-2-preview")


def _get_collection():

    load_dotenv()
    chroma_dir = os.getenv("CHROMA_DIR", "./chroma_db")

    client = chromadb.PersistentClient(path=chroma_dir)
    return client.get_or_create_collection(name="semantic_schema")


def embed_text(gclient, text):
    # Gemini embeddings endpoint
    res = gclient.models.embed_content(
        model=EMBED_MODEL,
        contents=[text],
    )
    return res.embeddings[0].values


def retrieve_top_docs(gclient, question, k=12):
    col = _get_collection()

    q_vec = embed_text(gclient, question)

    hits = col.query(
        query_embeddings=[q_vec],
        n_results=k,
        include=["documents"],  
    )

    return hits["documents"][0]