import os
from dotenv import load_dotenv
import psycopg2

def get_conn():
    load_dotenv()
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise RuntimeError("DATABASE_URL not set in .env")
    return psycopg2.connect(db_url)