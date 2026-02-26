"""
Auto-migration: creates tables and indexes on startup.
Tracks schema version for future migrations.
"""

from db import engine
from db.models import TABLES, INDEXES, SCHEMA_VERSION


def initialize_database() -> None:
    """Create all tables and indexes if they don't exist."""
    conn = engine.get_connection()

    for table_sql in TABLES.values():
        conn.execute(table_sql)

    for index_sql in INDEXES:
        conn.execute(index_sql)

    # Set schema version
    conn.execute(
        "INSERT OR REPLACE INTO schema_version (version) VALUES (?)",
        (SCHEMA_VERSION,),
    )

    # Ensure circuit_breaker has a default row
    existing = conn.execute("SELECT COUNT(*) as cnt FROM circuit_breaker").fetchone()
    if existing["cnt"] == 0:
        conn.execute("INSERT INTO circuit_breaker (id, consecutive_losses) VALUES (1, 0)")

    conn.commit()


def get_schema_version() -> int:
    """Get current schema version, or 0 if not initialized."""
    try:
        row = engine.query_one("SELECT version FROM schema_version LIMIT 1")
        return row["version"] if row else 0
    except Exception:
        return 0
