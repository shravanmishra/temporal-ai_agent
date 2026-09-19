import sqlite3
import json

DB_NAME = "live_agents.db"

def init_db():
    with sqlite3.connect(DB_NAME) as conn:
        # Core chat message history
        conn.execute("""
            CREATE TABLE IF NOT EXISTS conversation_history (
                session_id TEXT,
                role TEXT,
                content TEXT
            )
        """)
        # Structured facts extraction table
        conn.execute("""
            CREATE TABLE IF NOT EXISTS user_facts (
                session_id TEXT,
                fact_key TEXT,
                fact_value TEXT,
                PRIMARY KEY(session_id, fact_key)
            )
        """)
        conn.commit()

def load_messages(session_id: str) -> list:
    with sqlite3.connect(DB_NAME) as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT role, content FROM conversation_history WHERE session_id = ? ORDER BY rowid ASC", 
            (session_id,)
        )
        return [{"role": r, "content": c} for r, c in cursor.fetchall()]

def append_message(session_id: str, role: str, content: str):
    with sqlite3.connect(DB_NAME) as conn:
        conn.execute(
            "INSERT INTO conversation_history (session_id, role, content) VALUES (?, ?, ?)",
            (session_id, role, content)
        )
        conn.commit()

def upsert_fact(session_id: str, key: str, value: str):
    with sqlite3.connect(DB_NAME) as conn:
        conn.execute("""
            INSERT INTO user_facts (session_id, fact_key, fact_value)
            VALUES (?, ?, ?)
            ON CONFLICT(session_id, fact_key) DO UPDATE SET fact_value = excluded.fact_value
        """, (session_id, key, value))
        conn.commit()

def load_facts(session_id: str) -> dict:
    with sqlite3.connect(DB_NAME) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT fact_key, fact_value FROM user_facts WHERE session_id = ?", (session_id,))
        return {r[0]: r[1] for r in cursor.fetchall()}
