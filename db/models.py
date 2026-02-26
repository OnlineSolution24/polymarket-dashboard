"""
SQLite table definitions (9 tables).
Each table has a CREATE TABLE statement and helper CRUD functions.
"""

SCHEMA_VERSION = 1

TABLES = {
    "schema_version": """
        CREATE TABLE IF NOT EXISTS schema_version (
            version     INTEGER PRIMARY KEY
        )
    """,

    "agents": """
        CREATE TABLE IF NOT EXISTS agents (
            id              TEXT PRIMARY KEY,
            name            TEXT NOT NULL,
            role            TEXT NOT NULL DEFAULT 'custom',
            config_file     TEXT,
            persona         TEXT,
            skills          TEXT DEFAULT '[]',
            status          TEXT DEFAULT 'active',
            budget_used_today REAL DEFAULT 0.0,
            last_reset_date TEXT,
            created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """,

    "markets": """
        CREATE TABLE IF NOT EXISTS markets (
            id              TEXT PRIMARY KEY,
            question        TEXT NOT NULL,
            slug            TEXT,
            yes_price       REAL DEFAULT 0.0,
            no_price        REAL DEFAULT 0.0,
            volume          REAL DEFAULT 0.0,
            liquidity       REAL DEFAULT 0.0,
            end_date        TIMESTAMP,
            category        TEXT,
            sentiment_score REAL,
            calculated_edge REAL,
            last_updated    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """,

    "trades": """
        CREATE TABLE IF NOT EXISTS trades (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            market_id       TEXT NOT NULL,
            market_question TEXT,
            side            TEXT NOT NULL,
            amount_usd      REAL NOT NULL,
            price           REAL,
            status          TEXT DEFAULT 'pending',
            agent_id        TEXT,
            user_cmd        TEXT,
            result          TEXT,
            pnl             REAL,
            created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            executed_at     TIMESTAMP
        )
    """,

    "agent_logs": """
        CREATE TABLE IF NOT EXISTS agent_logs (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            agent_id        TEXT NOT NULL,
            level           TEXT DEFAULT 'info',
            message         TEXT NOT NULL,
            metadata        TEXT,
            created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """,

    "api_costs": """
        CREATE TABLE IF NOT EXISTS api_costs (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            provider        TEXT NOT NULL,
            endpoint        TEXT,
            tokens_in       INTEGER DEFAULT 0,
            tokens_out      INTEGER DEFAULT 0,
            cost_usd        REAL NOT NULL DEFAULT 0.0,
            agent_id        TEXT,
            created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """,

    "suggestions": """
        CREATE TABLE IF NOT EXISTS suggestions (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            agent_id        TEXT NOT NULL,
            type            TEXT NOT NULL,
            title           TEXT NOT NULL,
            description     TEXT,
            payload         TEXT,
            status          TEXT DEFAULT 'pending',
            user_response   TEXT,
            created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            resolved_at     TIMESTAMP
        )
    """,

    "ml_models": """
        CREATE TABLE IF NOT EXISTS ml_models (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            model_type      TEXT NOT NULL,
            version         INTEGER NOT NULL DEFAULT 1,
            metrics         TEXT NOT NULL DEFAULT '{}',
            feature_cols    TEXT DEFAULT '[]',
            model_path      TEXT NOT NULL,
            is_active       INTEGER DEFAULT 0,
            trained_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            training_rows   INTEGER DEFAULT 0
        )
    """,

    "circuit_breaker": """
        CREATE TABLE IF NOT EXISTS circuit_breaker (
            id                  INTEGER PRIMARY KEY DEFAULT 1,
            consecutive_losses  INTEGER DEFAULT 0,
            paused_until        TIMESTAMP,
            last_updated        TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """,

    "response_cache": """
        CREATE TABLE IF NOT EXISTS response_cache (
            cache_key       TEXT PRIMARY KEY,
            response        TEXT NOT NULL,
            provider        TEXT,
            created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            expires_at      TIMESTAMP NOT NULL
        )
    """,
}

# Indexes for performance
INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_agent_logs_agent ON agent_logs(agent_id)",
    "CREATE INDEX IF NOT EXISTS idx_agent_logs_created ON agent_logs(created_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_api_costs_created ON api_costs(created_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_api_costs_agent ON api_costs(agent_id)",
    "CREATE INDEX IF NOT EXISTS idx_trades_status ON trades(status)",
    "CREATE INDEX IF NOT EXISTS idx_trades_created ON trades(created_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_suggestions_status ON suggestions(status)",
    "CREATE INDEX IF NOT EXISTS idx_markets_updated ON markets(last_updated DESC)",
    "CREATE INDEX IF NOT EXISTS idx_cache_expires ON response_cache(expires_at)",
]
