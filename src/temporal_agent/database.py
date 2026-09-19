import sqlite3
from typing import Any

DB_NAME = "live_agents.db"

def init_db():
    with sqlite3.connect(DB_NAME) as conn:
        # Core chat message history
        conn.execute("""
            CREATE TABLE IF NOT EXISTS conversation_history (
                session_id TEXT,
                role TEXT,
                content TEXT,
                turn_id TEXT
            )
        """)
        columns = {row[1] for row in conn.execute("PRAGMA table_info(conversation_history)")}
        if "turn_id" not in columns:
            conn.execute("ALTER TABLE conversation_history ADD COLUMN turn_id TEXT")
        conn.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS idx_conversation_turn_role
            ON conversation_history(turn_id, role)
            WHERE turn_id IS NOT NULL
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
        conn.execute("""
            CREATE TABLE IF NOT EXISTS agent_turns (
                turn_id TEXT PRIMARY KEY,
                session_id TEXT NOT NULL,
                prompt TEXT NOT NULL,
                status TEXT NOT NULL,
                response TEXT,
                error TEXT
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS stream_chunks (
                turn_id TEXT NOT NULL,
                sequence INTEGER NOT NULL,
                content TEXT NOT NULL,
                PRIMARY KEY(turn_id, sequence)
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

def append_message(session_id: str, role: str, content: str, turn_id: str | None = None):
    with sqlite3.connect(DB_NAME) as conn:
        conn.execute(
            "INSERT OR IGNORE INTO conversation_history (session_id, role, content, turn_id) VALUES (?, ?, ?, ?)",
            (session_id, role, content, turn_id)
        )
        conn.commit()

def create_turn(turn_id: str, session_id: str, prompt: str) -> dict[str, Any]:
    with sqlite3.connect(DB_NAME) as conn:
        conn.execute(
            "INSERT OR IGNORE INTO agent_turns (turn_id, session_id, prompt, status) VALUES (?, ?, ?, 'pending')",
            (turn_id, session_id, prompt),
        )
        row = conn.execute(
            "SELECT turn_id, session_id, prompt, status, response, error FROM agent_turns WHERE turn_id = ?",
            (turn_id,),
        ).fetchone()
        conn.commit()
    if row is None:
        raise RuntimeError(f"Could not create turn {turn_id}")
    if row[1] != session_id or row[2] != prompt:
        raise ValueError(f"Turn {turn_id} already belongs to a different request")
    return dict(zip(("turn_id", "session_id", "prompt", "status", "response", "error"), row))

def get_turn(turn_id: str) -> dict[str, Any] | None:
    with sqlite3.connect(DB_NAME) as conn:
        row = conn.execute(
            "SELECT turn_id, session_id, prompt, status, response, error FROM agent_turns WHERE turn_id = ?",
            (turn_id,),
        ).fetchone()
    if row is None:
        return None
    return dict(zip(("turn_id", "session_id", "prompt", "status", "response", "error"), row))

def mark_turn_running(turn_id: str) -> None:
    with sqlite3.connect(DB_NAME) as conn:
        conn.execute(
            "UPDATE agent_turns SET status = 'running' WHERE turn_id = ? AND status = 'pending'",
            (turn_id,),
        )
        conn.commit()

def complete_turn(turn_id: str, response: str) -> None:
    with sqlite3.connect(DB_NAME) as conn:
        conn.execute(
            "UPDATE agent_turns SET status = 'completed', response = ?, error = NULL WHERE turn_id = ?",
            (response, turn_id),
        )
        conn.commit()

def fail_turn(turn_id: str, error: str) -> None:
    with sqlite3.connect(DB_NAME) as conn:
        conn.execute(
            "UPDATE agent_turns SET status = 'failed', error = ? WHERE turn_id = ?",
            (error, turn_id),
        )
        conn.commit()

def append_stream_chunk(turn_id: str, sequence: int, content: str) -> None:
    with sqlite3.connect(DB_NAME) as conn:
        conn.execute(
            "INSERT OR IGNORE INTO stream_chunks (turn_id, sequence, content) VALUES (?, ?, ?)",
            (turn_id, sequence, content),
        )
        conn.commit()

def load_stream_chunks(turn_id: str, after_sequence: int = 0) -> list[tuple[int, str]]:
    with sqlite3.connect(DB_NAME) as conn:
        return conn.execute(
            "SELECT sequence, content FROM stream_chunks WHERE turn_id = ? AND sequence >= ? ORDER BY sequence",
            (turn_id, after_sequence),
        ).fetchall()

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
