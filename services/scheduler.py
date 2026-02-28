"""
Background scheduler using APScheduler.
Loads job definitions from platform_config.yaml.
Runs market refresh, sentiment updates, agent cycles, ML retraining,
budget checks, and daily summaries.
"""

import logging
import threading
from datetime import datetime, date

from config import load_platform_config, AppConfig

logger = logging.getLogger(__name__)

_scheduler = None
_lock = threading.Lock()


def start_scheduler(config: AppConfig) -> None:
    """Start the background scheduler (once)."""
    global _scheduler

    with _lock:
        if _scheduler is not None:
            return

        try:
            from apscheduler.schedulers.background import BackgroundScheduler

            _scheduler = BackgroundScheduler()
            platform_cfg = load_platform_config()
            sched_cfg = platform_cfg.get("scheduler", {})

            # Market refresh
            if sched_cfg.get("market_refresh", {}).get("enabled", True):
                interval = sched_cfg["market_refresh"].get("interval_minutes", 30)
                _scheduler.add_job(
                    _job_refresh_markets, "interval", minutes=interval,
                    id="market_refresh", replace_existing=True, args=[config],
                )
                logger.info(f"Scheduled: market_refresh every {interval}min")

            # Sentiment update
            if sched_cfg.get("sentiment_update", {}).get("enabled", True):
                interval = sched_cfg["sentiment_update"].get("interval_minutes", 60)
                _scheduler.add_job(
                    _job_update_sentiment, "interval", minutes=interval,
                    id="sentiment_update", replace_existing=True, args=[config],
                )
                logger.info(f"Scheduled: sentiment_update every {interval}min")

            # Cost aggregation / budget check
            if sched_cfg.get("cost_aggregation", {}).get("enabled", True):
                interval = sched_cfg["cost_aggregation"].get("interval_minutes", 60)
                _scheduler.add_job(
                    _job_check_budgets, "interval", minutes=interval,
                    id="cost_aggregation", replace_existing=True, args=[config],
                )

            # Agent health check + cycle runner
            if sched_cfg.get("agent_health_check", {}).get("enabled", True):
                interval = sched_cfg["agent_health_check"].get("interval_minutes", 15)
                _scheduler.add_job(
                    _job_run_agent_cycles, "interval", minutes=interval,
                    id="agent_cycles", replace_existing=True, args=[config],
                )
                logger.info(f"Scheduled: agent_cycles every {interval}min")

            # ML retraining
            if sched_cfg.get("ml_retrain", {}).get("enabled", False):
                days = sched_cfg["ml_retrain"].get("interval_days", 7)
                _scheduler.add_job(
                    _job_ml_retrain, "interval", days=days,
                    id="ml_retrain", replace_existing=True,
                )
                logger.info(f"Scheduled: ml_retrain every {days} days")

            # Daily summary
            if sched_cfg.get("daily_summary", {}).get("enabled", True):
                hour = sched_cfg["daily_summary"].get("hour", 20)
                _scheduler.add_job(
                    _job_daily_summary, "cron", hour=hour, minute=0,
                    id="daily_summary", replace_existing=True, args=[config],
                )
                logger.info(f"Scheduled: daily_summary at {hour}:00 UTC")

            # Daily budget reset (midnight UTC)
            _scheduler.add_job(
                _job_daily_reset, "cron", hour=0, minute=0,
                id="daily_reset", replace_existing=True,
            )

            # Trader cycle (checks for approved suggestions → executes trades)
            trader_interval = sched_cfg.get("trader_cycle", {}).get("interval_minutes", 5)
            _scheduler.add_job(
                _job_run_trader_cycle, "interval", minutes=trader_interval,
                id="trader_cycle", replace_existing=True, args=[config],
            )
            logger.info(f"Scheduled: trader_cycle every {trader_interval}min")

            # Cache cleanup (every 6 hours)
            _scheduler.add_job(
                _job_cleanup_cache, "interval", hours=6,
                id="cache_cleanup", replace_existing=True,
            )

            _scheduler.start()
            logger.info("Background scheduler started with all jobs")

        except ImportError:
            logger.warning("APScheduler not installed. Background jobs disabled.")
        except Exception as e:
            logger.error(f"Failed to start scheduler: {e}")


def _job_refresh_markets(config: AppConfig):
    """Refresh Polymarket data."""
    try:
        from services.polymarket_client import PolymarketService
        from db import engine

        service = PolymarketService(config)
        markets = service.fetch_markets()

        for market in markets:
            engine.execute(
                """INSERT OR REPLACE INTO markets
                   (id, question, slug, yes_price, no_price, volume, liquidity, end_date, category, last_updated)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (market["id"], market["question"], market.get("slug", ""),
                 market.get("yes_price", 0), market.get("no_price", 0),
                 market.get("volume", 0), market.get("liquidity", 0),
                 market.get("end_date"), market.get("category", ""),
                 datetime.utcnow().isoformat()),
            )

        logger.info(f"Market refresh: {len(markets)} markets updated")
    except Exception as e:
        logger.error(f"Market refresh failed: {e}")


def _job_update_sentiment(config: AppConfig):
    """Update sentiment scores for tracked markets."""
    try:
        from services.news_sentiment import NewsSentimentService
        from db import engine

        service = NewsSentimentService(config)
        markets = engine.query("SELECT id, question FROM markets ORDER BY volume DESC LIMIT 20")

        updated = 0
        for market in markets:
            result = service.get_sentiment(market["question"][:80], days_back=3)
            if result["article_count"] > 0:
                engine.execute(
                    "UPDATE markets SET sentiment_score = ?, last_updated = ? WHERE id = ?",
                    (result["score"], datetime.utcnow().isoformat(), market["id"]),
                )
                updated += 1

        logger.info(f"Sentiment update: {updated}/{len(markets)} markets")
    except Exception as e:
        logger.error(f"Sentiment update failed: {e}")


def _job_run_agent_cycles(config: AppConfig):
    """Run enabled agent cycles based on their schedules."""
    try:
        from config import load_agent_configs
        from agents.agent_factory import create_agent
        from services.telegram_bridge import get_bridge
        from services.cost_tracker import check_budget

        bridge = get_bridge(config)
        configs = load_agent_configs()

        for agent_cfg in configs:
            if not agent_cfg.enabled:
                continue

            # Check budget before running
            budget = check_budget(agent_id=agent_cfg.id)
            if not budget["allowed"]:
                logger.debug(f"Agent {agent_cfg.id} skipped: {budget['reason']}")
                continue

            try:
                agent = create_agent(agent_cfg, bridge)
                result = agent.run_cycle()
                logger.debug(f"Agent {agent_cfg.id}: {result.get('summary', '')[:100]}")
            except Exception as e:
                logger.error(f"Agent {agent_cfg.id} cycle failed: {e}")
                # Alert on error
                try:
                    from services.telegram_alerts import get_alerts
                    alerts = get_alerts(config)
                    alerts.alert_agent_error(agent_cfg.id, str(e))
                except Exception:
                    pass

    except Exception as e:
        logger.error(f"Agent cycles failed: {e}")


def _job_run_trader_cycle(config: AppConfig):
    """Run the Trader agent cycle (process approved suggestions → execute trades)."""
    try:
        # Check if bot is paused
        try:
            from bot_main import bot_state
            if bot_state.paused:
                return
        except ImportError:
            pass

        from config import load_agent_configs
        from agents.agent_factory import create_agent
        from services.telegram_bridge import get_bridge

        bridge = get_bridge(config)

        # Find the trader config
        for cfg in load_agent_configs():
            if cfg.role == "trader" and cfg.enabled:
                agent = create_agent(cfg, bridge)
                result = agent.run_cycle()
                logger.debug(f"Trader cycle: {result.get('summary', '')[:100]}")
                break

    except Exception as e:
        logger.error(f"Trader cycle failed: {e}")


def _job_check_budgets(config: AppConfig):
    """Check budget limits and send alerts."""
    try:
        from services.cost_tracker import check_budget
        from services.telegram_alerts import get_alerts
        from config import get_budget_config

        budget = get_budget_config()
        threshold = budget.get("alert_threshold_percent", 80) / 100
        result = check_budget()
        alerts = get_alerts(config)

        daily_limit = budget.get("daily_limit_usd", 5.0)
        monthly_limit = budget.get("monthly_total_usd", 50.0)

        if result["daily_used"] >= daily_limit * threshold:
            alerts.alert_budget_warning(result["daily_used"], daily_limit, "Tag")

        if result["monthly_used"] >= monthly_limit * threshold:
            alerts.alert_budget_warning(result["monthly_used"], monthly_limit, "Monat")

    except Exception as e:
        logger.error(f"Budget check failed: {e}")


def _job_ml_retrain():
    """Automatically retrain ML models."""
    try:
        from ml.trainer import train_models
        results = train_models()
        if results.get("ok"):
            logger.info("ML auto-retrain completed successfully")
        else:
            logger.warning(f"ML auto-retrain: {results.get('error', 'unknown')}")
    except Exception as e:
        logger.error(f"ML retrain failed: {e}")


def _job_daily_summary(config: AppConfig):
    """Send daily performance summary via Telegram."""
    try:
        from db import engine
        from services.telegram_alerts import get_alerts

        today = date.today().isoformat()

        # Collect stats
        cost_row = engine.query_one(
            "SELECT COALESCE(SUM(cost_usd), 0) as total FROM api_costs WHERE date(created_at) = ?", (today,)
        )
        trade_row = engine.query_one(
            "SELECT COUNT(*) as cnt, COALESCE(SUM(pnl), 0) as pnl FROM trades WHERE date(executed_at) = ?", (today,)
        )
        agent_row = engine.query_one("SELECT COUNT(*) as cnt FROM agents WHERE status = 'active'")
        sugg_row = engine.query_one(
            "SELECT COUNT(*) as cnt FROM suggestions WHERE date(created_at) = ?", (today,)
        )

        cost = cost_row["total"] if cost_row else 0
        trades = trade_row["cnt"] if trade_row else 0
        pnl = trade_row["pnl"] if trade_row else 0
        agents = agent_row["cnt"] if agent_row else 0
        suggestions = sugg_row["cnt"] if sugg_row else 0

        summary = (
            f"Aktive Agents: {agents}\n"
            f"Trades heute: {trades} (PnL: ${pnl:+.2f})\n"
            f"AI-Kosten: ${cost:.2f}\n"
            f"Neue Vorschläge: {suggestions}"
        )

        alerts = get_alerts(config)
        alerts.send_daily_summary(summary)
        logger.info("Daily summary sent")

    except Exception as e:
        logger.error(f"Daily summary failed: {e}")


def _job_daily_reset():
    """Reset daily budget counters."""
    try:
        from services.cost_tracker import reset_daily_budgets
        reset_daily_budgets()
    except Exception as e:
        logger.error(f"Daily reset failed: {e}")


def _job_cleanup_cache():
    """Clean expired cache entries."""
    try:
        from db import engine
        engine.execute(
            "DELETE FROM response_cache WHERE expires_at < datetime('now')"
        )
        logger.debug("Cache cleanup completed")
    except Exception as e:
        logger.error(f"Cache cleanup failed: {e}")
