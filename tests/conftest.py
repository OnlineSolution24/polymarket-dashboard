"""
Pytest fixtures for the Polymarket Dashboard test suite.
"""

import os
import sys
import sqlite3
import tempfile
from pathlib import Path

import pytest

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

# Override DB path for tests
os.environ["DB_PATH"] = ":memory:"
os.environ["APP_PASSWORD"] = "test123"
os.environ["POLYMARKET_HOST"] = "https://clob.polymarket.com"


@pytest.fixture(autouse=True)
def reset_db():
    """Reset the database for each test."""
    import db.engine as eng

    # Reset singleton
    eng._connection = None

    # Create fresh in-memory DB
    conn = sqlite3.connect(":memory:", check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.row_factory = sqlite3.Row
    eng._connection = conn

    # Initialize tables
    from db.models import TABLES, INDEXES
    for sql in TABLES.values():
        conn.execute(sql)
    for sql in INDEXES:
        conn.execute(sql)
    conn.execute("INSERT INTO circuit_breaker (id, consecutive_losses) VALUES (1, 0)")
    conn.commit()

    yield conn

    conn.close()
    eng._connection = None


@pytest.fixture
def sample_markets(reset_db):
    """Insert sample market data."""
    from db import engine

    markets = [
        ("m1", "Will BTC reach 100k?", "btc-100k", 0.65, 0.35, 1000000, 500000, "2026-12-31", "crypto"),
        ("m2", "US shutdown Q1?", "us-shutdown", 0.30, 0.70, 500000, 200000, "2026-03-31", "politics"),
        ("m3", "SpaceX orbit?", "spacex-orbit", 0.82, 0.18, 300000, 100000, "2026-06-30", "science"),
    ]
    for m in markets:
        engine.execute(
            """INSERT INTO markets (id, question, slug, yes_price, no_price, volume, liquidity, end_date, category, last_updated)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))""",
            m,
        )
    return markets


@pytest.fixture
def sample_trades(reset_db):
    """Insert sample trade data."""
    from db import engine

    trades = [
        ("m1", "BTC 100k", "YES", 50.0, 0.65, "executed", "win", 30.0, "2026-01-15T10:00:00"),
        ("m2", "US shutdown", "NO", 30.0, 0.70, "executed", "win", 12.0, "2026-01-16T11:00:00"),
        ("m1", "BTC 100k", "YES", 40.0, 0.60, "executed", "loss", -40.0, "2026-01-17T12:00:00"),
        ("m3", "SpaceX", "YES", 25.0, 0.82, "executed", "win", 5.0, "2026-01-18T13:00:00"),
        ("m2", "US shutdown", "YES", 35.0, 0.30, "executed", "loss", -35.0, "2026-01-19T14:00:00"),
    ]
    for t in trades:
        engine.execute(
            """INSERT INTO trades (market_id, market_question, side, amount_usd, price, status, result, pnl, executed_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            t,
        )
    return trades


@pytest.fixture
def sample_costs(reset_db):
    """Insert sample API cost data."""
    from db import engine

    costs = [
        ("claude-sonnet", "analysis", 500, 200, 0.0045, "chief"),
        ("haiku", "observation", 300, 100, 0.0005, "observer"),
        ("gemini-flash", "parsing", 1000, 50, 0.0001, "analyst"),
    ]
    for c in costs:
        engine.execute(
            """INSERT INTO api_costs (provider, endpoint, tokens_in, tokens_out, cost_usd, agent_id, created_at)
               VALUES (?, ?, ?, ?, ?, ?, datetime('now'))""",
            c,
        )
    return costs
