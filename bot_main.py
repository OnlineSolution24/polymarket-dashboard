"""
Polymarket Trading Bot — Standalone Daemon.
Runs independently of the Dashboard. Coordinates all agents,
manages trades, and exposes a REST API for the monitoring dashboard.

Usage:
    python bot_main.py
"""

import asyncio
import logging
import signal
import sys
import threading
from pathlib import Path

# Ensure project root is on sys.path
ROOT_DIR = Path(__file__).parent
sys.path.insert(0, str(ROOT_DIR))

from config import AppConfig, load_platform_config
from db import engine
from db.models import TABLES, INDEXES

logger = logging.getLogger("bot")


# ---------------------------------------------------------------------------
# Database initialization
# ---------------------------------------------------------------------------

def init_database():
    """Create all tables and indexes if they don't exist."""
    for name, ddl in TABLES.items():
        engine.execute(ddl)
        logger.debug(f"Table ready: {name}")

    for idx_sql in INDEXES:
        engine.execute(idx_sql)

    # Seed circuit breaker row
    existing = engine.query_one("SELECT id FROM circuit_breaker WHERE id = 1")
    if not existing:
        engine.execute("INSERT INTO circuit_breaker (id, consecutive_losses) VALUES (1, 0)")

    # Seed agents from YAML configs
    from config import load_agent_configs
    import json

    for cfg in load_agent_configs():
        row = engine.query_one("SELECT id FROM agents WHERE id = ?", (cfg.id,))
        if not row:
            engine.execute(
                """INSERT INTO agents (id, name, role, config_file, persona, skills, status)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (cfg.id, cfg.name, cfg.role, f"{cfg.id}.yaml",
                 cfg.persona, json.dumps(cfg.skills),
                 "active" if cfg.enabled else "inactive"),
            )
            logger.info(f"Seeded agent: {cfg.name}")

    logger.info("Database initialized")


# ---------------------------------------------------------------------------
# Bot state (shared across modules)
# ---------------------------------------------------------------------------

class BotState:
    """Global bot runtime state."""
    paused: bool = False
    shutting_down: bool = False


bot_state = BotState()


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def main():
    # Logging
    platform_cfg = load_platform_config()
    log_level = platform_cfg.get("platform", {}).get("log_level", "info").upper()
    logging.basicConfig(
        level=getattr(logging, log_level, logging.INFO),
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    logger.info("=" * 60)
    logger.info("  Polymarket Trading Bot starting...")
    logger.info("=" * 60)

    # 1. Load config
    config = AppConfig.from_env()

    # 2. Init DB
    init_database()

    # 3. Start Telegram Bridge
    from services.telegram_bridge import get_bridge
    bridge = get_bridge(config)
    bridge_ok = bridge.start()
    if bridge_ok:
        logger.info("Telegram Bridge connected")
    else:
        logger.warning("Telegram Bridge not started (running without OpenClaw)")

    # 4. Load Agent Registry
    from agents.agent_registry import get_registry
    registry = get_registry()
    registry.load_from_configs(telegram_bridge=bridge)
    logger.info(f"Loaded {registry.count} agents")

    # 5. Start Scheduler
    from services.scheduler import start_scheduler
    start_scheduler(config)
    logger.info("Scheduler started")

    # 6. Start FastAPI REST API in background thread
    api_thread = threading.Thread(
        target=_run_api_server,
        args=(config,),
        daemon=True,
        name="api-server",
    )
    api_thread.start()
    logger.info("REST API server starting on port 8000")

    # 7. Graceful shutdown
    shutdown_event = threading.Event()

    def _signal_handler(sig, frame):
        logger.info(f"Received signal {sig}, shutting down...")
        bot_state.shutting_down = True
        shutdown_event.set()

    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    logger.info("Bot is running. Press Ctrl+C to stop.")

    # Send startup alert
    try:
        from services.telegram_alerts import get_alerts
        alerts = get_alerts(config)
        mode = load_platform_config().get("trading", {}).get("mode", "paper")
        alerts.send(
            f"🟢 <b>Bot gestartet</b>\n"
            f"Agents: {registry.count}\n"
            f"Trading-Modus: <code>{mode}</code>\n"
            f"Bridge: {'✅' if bridge_ok else '❌'}"
        )
    except Exception:
        pass

    # Block until shutdown
    shutdown_event.wait()
    logger.info("Bot stopped.")


def _run_api_server(config: AppConfig):
    """Run the FastAPI server using uvicorn."""
    try:
        import uvicorn
        from api.rest_api import create_app

        app = create_app(config)
        uvicorn.run(app, host="0.0.0.0", port=8000, log_level="warning")
    except ImportError:
        logger.error("uvicorn not installed. REST API disabled. Install with: pip install uvicorn")
    except Exception as e:
        logger.error(f"REST API server error: {e}")


if __name__ == "__main__":
    main()
