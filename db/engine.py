"""
SQLite database engine with WAL mode for concurrent read/write access.
Singleton pattern ensures one connection pool across the app.
"""

import sqlite3
import threading
from pathlib import Path

from config import ROOT_DIR

_lock = threading.Lock()
_connection: sqlite3.Connection | None = None


def get_db_path() -> Path:
    """Resolve the database path."""
    import os
    from dotenv import load_dotenv
    load_dotenv()
    db_path = os.getenv("DB_PATH", "data/dashboard.db")
    path = Path(db_path)
    if not path.is_absolute():
        path = ROOT_DIR / path
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def get_connection() -> sqlite3.Connection:
    """Get or create the singleton SQLite connection with WAL mode."""
    global _connection
    if _connection is not None:
        return _connection

    with _lock:
        if _connection is not None:
            return _connection

        db_path = get_db_path()
        conn = sqlite3.connect(str(db_path), check_same_thread=False)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA busy_timeout=5000")
        conn.row_factory = sqlite3.Row
        _connection = conn
        return conn


def execute(sql: str, params: tuple = ()) -> sqlite3.Cursor:
    """Execute a SQL statement with thread-safe locking."""
    conn = get_connection()
    with _lock:
        cursor = conn.execute(sql, params)
        conn.commit()
        return cursor


def executemany(sql: str, params_list: list[tuple]) -> sqlite3.Cursor:
    """Execute a SQL statement for multiple parameter sets."""
    conn = get_connection()
    with _lock:
        cursor = conn.executemany(sql, params_list)
        conn.commit()
        return cursor


def query(sql: str, params: tuple = ()) -> list[dict]:
    """Execute a SELECT and return results as list of dicts."""
    conn = get_connection()
    cursor = conn.execute(sql, params)
    rows = cursor.fetchall()
    return [dict(row) for row in rows]


def query_one(sql: str, params: tuple = ()) -> dict | None:
    """Execute a SELECT and return first result as dict."""
    conn = get_connection()
    cursor = conn.execute(sql, params)
    row = cursor.fetchone()
    return dict(row) if row else None
