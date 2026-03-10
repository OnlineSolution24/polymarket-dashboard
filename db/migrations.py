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

    # Run migrations if needed
    current = _get_schema_version_raw(conn)
    if current < 3:
        _upgrade_to_v3(conn)
    if current < 5:
        _upgrade_to_v5(conn)

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


def _get_schema_version_raw(conn) -> int:
    """Get schema version directly from connection."""
    try:
        row = conn.execute("SELECT version FROM schema_version LIMIT 1").fetchone()
        return row["version"] if row else 0
    except Exception:
        return 0


def _upgrade_to_v3(conn) -> None:
    """Add Gamma API fields to markets table."""
    new_columns = [
        ("yes_token_id", "TEXT"),
        ("no_token_id", "TEXT"),
        ("best_bid", "REAL"),
        ("best_ask", "REAL"),
        ("spread", "REAL"),
        ("volume_24h", "REAL"),
        ("volume_1w", "REAL"),
        ("volume_1m", "REAL"),
        ("last_trade_price", "REAL"),
        ("accepting_orders", "INTEGER DEFAULT 1"),
        ("bid_ask_spread", "REAL"),
        ("book_imbalance", "REAL"),
        ("bid_depth", "REAL"),
        ("ask_depth", "REAL"),
    ]
    for col_name, col_type in new_columns:
        try:
            conn.execute(f"ALTER TABLE markets ADD COLUMN {col_name} {col_type}")
        except Exception:
            pass  # Column already exists


def _upgrade_to_v5(conn) -> None:
    """Add whale/smart-money signal columns to markets table."""
    new_columns = [
        ("whale_buy_count", "INTEGER DEFAULT 0"),
        ("whale_sell_count", "INTEGER DEFAULT 0"),
        ("whale_net_flow", "REAL DEFAULT 0"),
        ("top_holder_concentration", "REAL"),
        ("open_interest", "REAL"),
        ("oi_change_24h", "REAL"),
        ("smart_money_score", "REAL"),
    ]
    for col_name, col_type in new_columns:
        try:
            conn.execute(f"ALTER TABLE markets ADD COLUMN {col_name} {col_type}")
        except Exception:
            pass  # Column already exists


def get_schema_version() -> int:
    """Get current schema version, or 0 if not initialized."""
    try:
        row = engine.query_one("SELECT version FROM schema_version LIMIT 1")
        return row["version"] if row else 0
    except Exception:
        return 0
