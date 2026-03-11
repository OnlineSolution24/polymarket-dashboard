"""
SQLite table definitions (13 tables).
Each table has a CREATE TABLE statement and helper CRUD functions.
"""

SCHEMA_VERSION = 5

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
            yes_token_id    TEXT,
            no_token_id     TEXT,
            best_bid        REAL,
            best_ask        REAL,
            spread          REAL,
            volume_24h      REAL,
            volume_1w       REAL,
            volume_1m       REAL,
            last_trade_price REAL,
            accepting_orders INTEGER DEFAULT 1,
            bid_ask_spread  REAL,
            book_imbalance  REAL,
            bid_depth       REAL,
            ask_depth       REAL,
            -- Whale / Smart Money signals (Data API)
            whale_buy_count     INTEGER DEFAULT 0,
            whale_sell_count    INTEGER DEFAULT 0,
            whale_net_flow      REAL DEFAULT 0,
            top_holder_concentration REAL,
            open_interest       REAL,
            oi_change_24h       REAL,
            smart_money_score   REAL,
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

    # --- Strategy Discovery Tables ---

    "strategies": """
        CREATE TABLE IF NOT EXISTS strategies (
            id              TEXT PRIMARY KEY,
            name            TEXT NOT NULL,
            version         INTEGER DEFAULT 1,
            description     TEXT,
            definition      TEXT NOT NULL,
            status          TEXT DEFAULT 'draft',
            category        TEXT,
            discovered_by   TEXT,
            approved_by     TEXT,
            backtest_pnl        REAL,
            backtest_win_rate   REAL,
            backtest_sharpe     REAL,
            backtest_max_dd     REAL,
            backtest_trades     INTEGER,
            backtest_results    TEXT,
            live_pnl            REAL DEFAULT 0,
            live_win_rate       REAL,
            live_trades         INTEGER DEFAULT 0,
            confidence_score    REAL DEFAULT 0,
            created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            retired_at      TIMESTAMP
        )
    """,

    "strategy_trades": """
        CREATE TABLE IF NOT EXISTS strategy_trades (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            strategy_id     TEXT NOT NULL,
            trade_id        INTEGER,
            market_id       TEXT NOT NULL,
            side            TEXT NOT NULL,
            entry_price     REAL,
            exit_price      REAL,
            amount_usd      REAL,
            pnl             REAL,
            result          TEXT,
            is_backtest     INTEGER DEFAULT 0,
            created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """,

    "code_changes": """
        CREATE TABLE IF NOT EXISTS code_changes (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            agent_id        TEXT NOT NULL,
            file_path       TEXT NOT NULL,
            old_code        TEXT,
            new_code        TEXT NOT NULL,
            reason          TEXT NOT NULL,
            description     TEXT,
            diff_preview    TEXT,
            status          TEXT DEFAULT 'pending',
            user_comment    TEXT,
            backup_path     TEXT,
            created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            resolved_at     TIMESTAMP,
            applied_at      TIMESTAMP
        )
    """,

    "market_snapshots": """
        CREATE TABLE IF NOT EXISTS market_snapshots (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            market_id       TEXT NOT NULL,
            yes_price       REAL,
            no_price        REAL,
            volume          REAL,
            liquidity       REAL,
            sentiment_score REAL,
            snapshot_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """,

    "portfolio_snapshots": """
        CREATE TABLE IF NOT EXISTS portfolio_snapshots (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            snapshot_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            total_deposited REAL DEFAULT 0,
            positions_value REAL DEFAULT 0,
            positions_cost  REAL DEFAULT 0,
            unrealized_pnl  REAL DEFAULT 0,
            realized_pnl    REAL DEFAULT 0,
            position_count  INTEGER DEFAULT 0
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
    # Strategy indexes
    "CREATE INDEX IF NOT EXISTS idx_strategies_status ON strategies(status)",
    "CREATE INDEX IF NOT EXISTS idx_strategies_confidence ON strategies(confidence_score DESC)",
    "CREATE INDEX IF NOT EXISTS idx_strategy_trades_strategy ON strategy_trades(strategy_id)",
    "CREATE INDEX IF NOT EXISTS idx_strategy_trades_trade ON strategy_trades(trade_id)",
    # Market snapshot indexes
    "CREATE INDEX IF NOT EXISTS idx_snapshots_market_time ON market_snapshots(market_id, snapshot_at DESC)",
    # Code changes indexes
    "CREATE INDEX IF NOT EXISTS idx_code_changes_status ON code_changes(status)",
    "CREATE INDEX IF NOT EXISTS idx_code_changes_created ON code_changes(created_at DESC)",
    # Portfolio snapshot indexes
    "CREATE INDEX IF NOT EXISTS idx_portfolio_snapshots_time ON portfolio_snapshots(snapshot_at DESC)",
]
