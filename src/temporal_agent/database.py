import sqlite3
import hashlib
import os
from pathlib import Path
from typing import Any

DB_DIR = os.getenv("TEMPORAL_DATA_DIR", "~/.temporal-agent/sessions")

def _data_dir() -> Path:
    return Path(DB_DIR).expanduser()

def _db_path(session_id: str) -> Path:
    digest = hashlib.sha256(session_id.encode("utf-8")).hexdigest()[:24]
    return _data_dir() / f"session_{digest}.db"

def init_db(session_id: str):
    _data_dir().mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(_db_path(session_id)) as conn:
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
    init_db(session_id)
    with sqlite3.connect(_db_path(session_id)) as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT role, content FROM conversation_history WHERE session_id = ? ORDER BY rowid ASC",
            (session_id,)
        )
        return [{"role": r, "content": c} for r, c in cursor.fetchall()]

def append_message(session_id: str, role: str, content: str, turn_id: str | None = None):
    init_db(session_id)
    with sqlite3.connect(_db_path(session_id)) as conn:
        conn.execute(
            "INSERT OR IGNORE INTO conversation_history (session_id, role, content, turn_id) VALUES (?, ?, ?, ?)",
            (session_id, role, content, turn_id)
        )
        conn.commit()

def create_turn(session_id: str, turn_id: str, prompt: str) -> dict[str, Any]:
    init_db(session_id)
    with sqlite3.connect(_db_path(session_id)) as conn:
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

def get_turn(session_id: str, turn_id: str) -> dict[str, Any] | None:
    init_db(session_id)
    with sqlite3.connect(_db_path(session_id)) as conn:
        row = conn.execute(
            "SELECT turn_id, session_id, prompt, status, response, error FROM agent_turns WHERE turn_id = ?",
            (turn_id,),
        ).fetchone()
    if row is None:
        return None
    return dict(zip(("turn_id", "session_id", "prompt", "status", "response", "error"), row))

def mark_turn_running(session_id: str, turn_id: str) -> None:
    init_db(session_id)
    with sqlite3.connect(_db_path(session_id)) as conn:
        conn.execute(
            "UPDATE agent_turns SET status = 'running' WHERE turn_id = ? AND status = 'pending'",
            (turn_id,),
        )
        conn.commit()

def complete_turn(session_id: str, turn_id: str, response: str) -> None:
    init_db(session_id)
    with sqlite3.connect(_db_path(session_id)) as conn:
        conn.execute(
            "UPDATE agent_turns SET status = 'completed', response = ?, error = NULL WHERE turn_id = ?",
            (response, turn_id),
        )
        conn.commit()

def fail_turn(session_id: str, turn_id: str, error: str) -> None:
    init_db(session_id)
    with sqlite3.connect(_db_path(session_id)) as conn:
        conn.execute(
            "UPDATE agent_turns SET status = 'failed', error = ? WHERE turn_id = ?",
            (error, turn_id),
        )
        conn.commit()

def append_stream_chunk(session_id: str, turn_id: str, sequence: int, content: str) -> None:
    append_stream_chunks(session_id, turn_id, [(sequence, content)])

def append_stream_chunks(session_id: str, turn_id: str, chunks: list[tuple[int, str]]) -> None:
    if not chunks:
        return
    init_db(session_id)
    with sqlite3.connect(_db_path(session_id)) as conn:
        conn.executemany(
            "INSERT OR IGNORE INTO stream_chunks (turn_id, sequence, content) VALUES (?, ?, ?)",
            [(turn_id, sequence, content) for sequence, content in chunks],
        )
        conn.commit()

def load_stream_chunks(session_id: str, turn_id: str, after_sequence: int = 0) -> list[tuple[int, str]]:
    init_db(session_id)
    with sqlite3.connect(_db_path(session_id)) as conn:
        return conn.execute(
            "SELECT sequence, content FROM stream_chunks WHERE turn_id = ? AND sequence >= ? ORDER BY sequence",
            (turn_id, after_sequence),
        ).fetchall()

def upsert_fact(session_id: str, key: str, value: str):
    init_db(session_id)
    with sqlite3.connect(_db_path(session_id)) as conn:
        conn.execute("""
            INSERT INTO user_facts (session_id, fact_key, fact_value)
            VALUES (?, ?, ?)
            ON CONFLICT(session_id, fact_key) DO UPDATE SET fact_value = excluded.fact_value
        """, (session_id, key, value))
        conn.commit()

def load_facts(session_id: str) -> dict:
    init_db(session_id)
    with sqlite3.connect(_db_path(session_id)) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT fact_key, fact_value FROM user_facts WHERE session_id = ?", (session_id,))
        return {r[0]: r[1] for r in cursor.fetchall()}
