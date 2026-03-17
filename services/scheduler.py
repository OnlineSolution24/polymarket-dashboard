"""
Background scheduler using APScheduler.
Loads job definitions from platform_config.yaml.
Runs market refresh, sentiment updates, agent cycles, ML retraining,
budget checks, and daily summaries.
"""

import logging
import threading
import re
from datetime import datetime, date, timedelta

from config import load_platform_config, AppConfig

logger = logging.getLogger(__name__)


# --- Market Blocklist (spam prevention) ---
_MARKET_BLOCKLIST_KEYWORDS = [
    "leganes", "leganés",
    "wichita state shockers",
]

def _is_market_blocklisted(question: str) -> bool:
    """Check if a market question matches any blocklisted keyword."""
    q_lower = question.lower()
    return any(kw in q_lower for kw in _MARKET_BLOCKLIST_KEYWORDS)


# Lazy import to avoid circular deps
def _preflight(market_id, side="YES", edge=0, amount=0):
    from services.suggestion_preflight import suggestion_preflight_check
    return suggestion_preflight_check(market_id, side, edge, amount)

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
                    next_run_time=datetime.now(),
                )
                logger.info(f"Scheduled: market_refresh every {interval}min")

            # Whale/smart-money signals (top 100 markets, every 60min)
            whale_interval = sched_cfg.get("whale_signals", {}).get("interval_minutes", 60)
            _scheduler.add_job(
                _job_whale_signals, "interval", minutes=whale_interval,
                id="whale_signals", replace_existing=True, args=[config],
                next_run_time=datetime.now(),
            )
            logger.info(f"Scheduled: whale_signals every {whale_interval}min")

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
                    next_run_time=datetime.now(),
                )
                logger.info(f"Scheduled: agent_cycles every {interval}min")

            # Chief analysis cycle (1x/day at 19:00 UTC — before daily summary at 20:00)
            chief_hour = sched_cfg.get("chief_analysis_hour", 19)
            _scheduler.add_job(
                _job_run_chief_cycle, "cron", hour=chief_hour, minute=0,
                id="chief_analysis", replace_existing=True, args=[config],
            )
            logger.info(f"Scheduled: chief_analysis daily at {chief_hour}:00 UTC")

            # Analyst cycle: 3x daily at strategic times (EU pre-open, US pre-open, evening review)
            for analyst_hour in [6, 13, 21]:
                _scheduler.add_job(
                    _job_run_analyst_cycle, "cron",
                    hour=analyst_hour, minute=0,
                    id=f"analyst_cycle_{analyst_hour}", replace_existing=True, args=[config],
                )
            logger.info("Scheduled: analyst_cycle at 06:00, 13:00, 21:00 UTC")

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

            # Daily snapshot backtests (run at 04:00 UTC)
            _scheduler.add_job(
                _job_daily_backtests, "cron", hour=4, minute=0,
                id="daily_backtests", replace_existing=True,
            )
            logger.info("Scheduled: daily_backtests at 04:00 UTC")


            # Strategy evaluation (active strategies → trade suggestions)
            if sched_cfg.get("strategy_evaluation", {}).get("enabled", True):
                strat_interval = sched_cfg["strategy_evaluation"].get("interval_minutes", 15)
                _scheduler.add_job(
                    _job_evaluate_strategies, "interval", minutes=strat_interval,
                    id="strategy_evaluation", replace_existing=True, args=[config],
                )
                logger.info(f"Scheduled: strategy_evaluation every {strat_interval}min")

            # Strategy scoring (update confidence scores based on live performance)
            _scheduler.add_job(
                _job_score_strategies, "interval", hours=1,
                id="strategy_scoring", replace_existing=True,
            )
            logger.info("Scheduled: strategy_scoring every 1h")

            # Trader cycle (checks for approved suggestions → executes trades)
            trader_interval = sched_cfg.get("trader_cycle", {}).get("interval_minutes", 5)
            _scheduler.add_job(
                _job_run_trader_cycle, "interval", minutes=trader_interval,
                id="trader_cycle", replace_existing=True, args=[config],
                next_run_time=datetime.now(),
            )
            logger.info(f"Scheduled: trader_cycle every {trader_interval}min")

            # Pattern Scanner (daily at 09:00 UTC)
            pattern_hour = sched_cfg.get("pattern_scanner_hour", 9)
            _scheduler.add_job(
                _job_pattern_scanner, "cron", hour=pattern_hour, minute=0,
                id="pattern_scanner", replace_existing=True, args=[config],
            )
            logger.info(f"Scheduled: pattern_scanner daily at {pattern_hour}:00 UTC")
            # Settlement: handled by PositionManager (no scheduler job)





            # Portfolio snapshot (3x daily: 08:05, 14:00, 20:05 UTC)
            for hour in [8, 14, 20]:
                _scheduler.add_job(
                    _job_portfolio_snapshot, "cron", hour=hour, minute=5,
                    id=f"portfolio_snapshot_{hour}", replace_existing=True, args=[config],
                )
            logger.info("Scheduled: portfolio_snapshot 3x daily (08:05, 14:00, 20:05 UTC)")

            # Auto-expire old suggestions (every hour)
            _scheduler.add_job(
                _job_expire_old_suggestions, "interval", hours=1,
                id="expire_suggestions", replace_existing=True,
            )
            logger.info("Scheduled: expire_old_suggestions every 1h")

            # Weather edge analysis (every 30 min)
            # DISABLED: weather_edge (zero results, unnecessary load)

            # Resolution Sniper: Weather (every 15 min - precise hourly forecasts)
            _scheduler.add_job(
                _job_weather_sniper, "interval", minutes=15,
                id="weather_sniper", replace_existing=True, args=[config],
            )
            logger.info("Scheduled: weather_sniper every 15min")

            # Resolution Sniper: Economic data (every 5 min during US hours 8-18 UTC)
            _scheduler.add_job(
                _job_economic_sniper, "interval", minutes=5,
                id="economic_sniper", replace_existing=True, args=[config],
            )
            logger.info("Scheduled: economic_sniper every 5min")

            # Resolution Sniper: Sport scores (every 10 min)
            _scheduler.add_job(
                _job_sport_sniper, "interval", minutes=10,
                id="sport_sniper", replace_existing=True, args=[config],
            )
            logger.info("Scheduled: sport_sniper every 10min")

            # New Market Scanner (every 15 min)
            # DISABLED: new_market_scan (zero results, unnecessary load)

            # Cache cleanup (every 6 hours)
            _scheduler.add_job(
                _job_cleanup_cache, "interval", hours=6,
                id="cache_cleanup", replace_existing=True,
            )

            # Snapshot cleanup (daily, remove snapshots older than 30 days)
            _scheduler.add_job(
                _job_cleanup_snapshots, "interval", hours=24,
                id="snapshot_cleanup", replace_existing=True,
            )

            # Position sync with Polymarket wallet (2x daily)
            # Position sync job removed - handled by PositionManager
            logger.info("Scheduled: position_sync every 12h")

            # Arbitrage scanner (every 30 min)
            _scheduler.add_job(
                _job_arbitrage_scan, "interval", minutes=30,
                id="arbitrage_scan", replace_existing=True, args=[config],
            )
            logger.info("Scheduled: arbitrage_scan every 30min")
            # Edge Sources (every 20 min) — crypto, cross-platform, weather ensemble
            _scheduler.add_job(
                _job_edge_sources, "interval", minutes=20,
                id="edge_sources", replace_existing=True, args=[config],
            )
            logger.info("Scheduled: edge_sources every 20min")

            # Health Monitor (every 2 hours)
            _scheduler.add_job(
                _job_health_monitor, "interval", hours=2,
                id="health_monitor", replace_existing=True, args=[config],
            )
            logger.info("Scheduled: health_monitor every 2h")

            # Live Sports Edge (every 5 min during games)
            # DISABLED: live_sports_edge (zero results, unnecessary load)

            _scheduler.start()
            logger.info("Background scheduler started with all jobs")

        except ImportError:
            logger.warning("APScheduler not installed. Background jobs disabled.")
        except Exception as e:
            logger.error(f"Failed to start scheduler: {e}")


def _job_refresh_markets(config: AppConfig):
    """Refresh Polymarket data via Gamma API + compute order book signals."""
    try:
        from services.polymarket_client import PolymarketService
        from db import engine

        service = PolymarketService(config)
        markets = service.fetch_markets()

        # Filter out closed/non-accepting markets
        active_markets = [m for m in markets if m.get("accepting_orders", 1) != 0]
        closed_count = len(markets) - len(active_markets)
        if closed_count:
            logger.info(f"Market refresh: filtered out {closed_count} closed/non-accepting markets")
        markets = active_markets

        now = datetime.utcnow().isoformat()

        for market in markets:
            engine.execute(
                """INSERT OR REPLACE INTO markets
                   (id, question, description, slug, yes_price, no_price, volume, liquidity,
                    end_date, category, yes_token_id, no_token_id,
                    best_bid, best_ask, spread, volume_24h, volume_1w, volume_1m,
                    last_trade_price, accepting_orders, last_updated)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (market["id"], market["question"], market.get("description", ""),
                 market.get("slug", ""),
                 market.get("yes_price", 0), market.get("no_price", 0),
                 market.get("volume", 0), market.get("liquidity", 0),
                 market.get("end_date"), market.get("category", ""),
                 market.get("yes_token_id", ""), market.get("no_token_id", ""),
                 market.get("best_bid", 0), market.get("best_ask", 0),
                 market.get("spread", 0), market.get("volume_24h", 0),
                 market.get("volume_1w", 0), market.get("volume_1m", 0),
                 market.get("last_trade_price", 0),
                 market.get("accepting_orders", 1), now),
            )

        # Save snapshots for historical analysis
        for market in markets:
            engine.execute(
                """INSERT INTO market_snapshots
                   (market_id, yes_price, no_price, volume, liquidity, sentiment_score, snapshot_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (market["id"], market.get("yes_price", 0), market.get("no_price", 0),
                 market.get("volume", 0), market.get("liquidity", 0),
                 None, now),
            )

        # Compute order book signals for top 20 markets
        ob_count = 0
        try:
            top_markets = engine.query(
                "SELECT id, yes_token_id FROM markets "
                "WHERE yes_token_id IS NOT NULL AND yes_token_id != '' "
                "ORDER BY volume DESC LIMIT 20"
            )
            for m in top_markets:
                analysis = service.get_order_book_analysis(m["yes_token_id"])
                if analysis.get("bid_ask_spread") is not None:
                    engine.execute(
                        """UPDATE markets SET bid_ask_spread = ?, book_imbalance = ?,
                           bid_depth = ?, ask_depth = ? WHERE id = ?""",
                        (analysis["bid_ask_spread"], analysis["book_imbalance"],
                         analysis["bid_depth"], analysis["ask_depth"], m["id"]),
                    )
                    ob_count += 1
        except Exception as e:
            logger.error(f"Order book signal computation failed: {e}")

        logger.info(
            f"Market refresh: {len(markets)} markets updated + snapshots saved + "
            f"{ob_count} order books analyzed"
        )
    except Exception as e:
        logger.error(f"Market refresh failed: {e}")


def _job_whale_signals(config: AppConfig):
    """Enhanced whale/smart-money signal collection for top 100 markets.

    Runs every 60 minutes. Fetches whale trades, holder concentration,
    open interest, smart money score, and volume spike detection.
    """
    import time

    try:
        from services.data_api_client import DataAPIClient
        from db import engine

        data_client = DataAPIClient(timeout=20)
        whale_count = 0
        volume_spike_count = 0

        # Get top 100 active markets by volume
        top_markets = engine.query(
            "SELECT id, volume_24h, volume_1w FROM markets "
            "WHERE accepting_orders = 1 "
            "ORDER BY volume DESC LIMIT 100"
        )

        for m in top_markets:
            mid = m["id"]
            try:
                # Whale trades (single API call, reuse for smart_money_score)
                whale = data_client.compute_whale_signals(mid)

                # Holder concentration
                conc = data_client.compute_holder_concentration(mid)

                # Open interest
                oi = data_client.get_open_interest(mid)

                # Smart money score (pass pre-computed data to avoid duplicate calls)
                sm_score = data_client.compute_smart_money_score(
                    mid, whale_data=whale, concentration=conc
                )

                # Compute OI change vs last stored value
                oi_change = None
                if oi is not None:
                    prev = engine.query_one(
                        "SELECT open_interest FROM markets WHERE id = ?", (mid,)
                    )
                    if prev and prev.get("open_interest"):
                        old_oi = float(prev["open_interest"])
                        if old_oi > 0:
                            oi_change = round((oi - old_oi) / old_oi, 4)

                # Volume spike detection:
                # Compare volume_24h with volume_1w / 7 (daily average over last week)
                volume_change = None
                vol_24h = m.get("volume_24h") or 0
                vol_1w = m.get("volume_1w") or 0
                if vol_1w > 0:
                    avg_daily = vol_1w / 7.0
                    if avg_daily > 0:
                        volume_change = round(vol_24h / avg_daily, 4)
                        if volume_change > 2.0:
                            volume_spike_count += 1

                engine.execute(
                    """UPDATE markets SET
                       whale_buy_count = ?, whale_sell_count = ?, whale_net_flow = ?,
                       top_holder_concentration = ?, open_interest = ?,
                       oi_change_24h = COALESCE(?, oi_change_24h),
                       smart_money_score = ?,
                       volume_change_24h = ?
                       WHERE id = ?""",
                    (whale["whale_buy_count"], whale["whale_sell_count"],
                     whale["whale_net_flow"], conc, oi, oi_change, sm_score,
                     volume_change, mid),
                )
                whale_count += 1

                # Rate limit: 0.5s between markets (3 API calls per market)
                time.sleep(0.5)

            except Exception as e:
                logger.warning(f"Whale signals failed for market {mid[:20]}: {e}")
                continue

        data_client.close()

        logger.info(
            f"Whale signals: {whale_count}/{len(top_markets)} markets updated, "
            f"{volume_spike_count} volume spikes detected"
        )
    except Exception as e:
        logger.error(f"Whale signal job failed: {e}")


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
    """Run enabled agent cycles (skip analyst — it has its own daily schedule)."""
    try:
        from config import load_agent_configs
        from agents.agent_factory import create_agent
        from services.telegram_bridge import get_bridge
        from services.cost_tracker import check_budget

        bridge = get_bridge(config)
        configs = load_agent_configs()

        # Agents that have their own dedicated schedule
        _SEPARATE_SCHEDULE = {"chief", "analyst", "trader"}

        for agent_cfg in configs:
            if not agent_cfg.enabled:
                continue
            if agent_cfg.role in _SEPARATE_SCHEDULE:
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
                try:
                    from services.telegram_alerts import get_alerts
                    alerts = get_alerts(config)
                    alerts.alert_agent_error(agent_cfg.id, str(e))
                except Exception:
                    pass

    except Exception as e:
        logger.error(f"Agent cycles failed: {e}")


def _job_run_chief_cycle(config: AppConfig):
    """Run only the Chief agent cycle at a separate interval."""
    try:
        from config import load_agent_configs
        from agents.agent_factory import create_agent
        from services.telegram_bridge import get_bridge
        from services.cost_tracker import check_budget

        bridge = get_bridge(config)

        for cfg in load_agent_configs():
            if cfg.role == "chief" and cfg.enabled:
                budget = check_budget(agent_id=cfg.id)
                if not budget["allowed"]:
                    logger.debug(f"Chief {cfg.id} skipped: {budget['reason']}")
                    return

                agent = create_agent(cfg, bridge)
                result = agent.run_cycle()
                logger.debug(f"Chief cycle: {result.get('summary', '')[:100]}")
                return

    except Exception as e:
        logger.error(f"Chief cycle failed: {e}")


def _job_run_analyst_cycle(config: AppConfig):
    """Run the Analyst agent cycle (1x/day for cost efficiency)."""
    try:
        from config import load_agent_configs
        from agents.agent_factory import create_agent
        from services.telegram_bridge import get_bridge
        from services.cost_tracker import check_budget

        bridge = get_bridge(config)

        for cfg in load_agent_configs():
            if cfg.role == "analyst" and cfg.enabled:
                budget = check_budget(agent_id=cfg.id)
                if not budget["allowed"]:
                    logger.debug(f"Analyst {cfg.id} skipped: {budget['reason']}")
                    return

                agent = create_agent(cfg, bridge)
                result = agent.run_cycle()
                logger.info(f"Analyst daily cycle: {result.get('summary', '')[:100]}")
                return

    except Exception as e:
        logger.error(f"Analyst cycle failed: {e}")


def _job_evaluate_strategies(config: AppConfig):
    """Evaluate active strategies → create trade suggestions for matching markets."""
    try:
        from db import engine
        from services.strategy_evaluator import find_matching_markets, compute_trade_params

        platform_cfg = load_platform_config()
        trading_cfg = platform_cfg.get("trading", {})
        strategy_cfg = platform_cfg.get("strategy", {})
        mode = trading_cfg.get("mode", "paper")
        capital = trading_cfg.get("capital_usd", 100.0)

        strategies = engine.query(
            "SELECT * FROM strategies WHERE status = 'active' ORDER BY confidence_score DESC"
        )
        if not strategies:
            return

        max_active = strategy_cfg.get("max_active_strategies", 5)
        strategies = strategies[:max_active]

        total_suggestions = 0
        for strategy in strategies:
            try:
                import json as _json
                definition = _json.loads(strategy["definition"]) if isinstance(strategy["definition"], str) else strategy["definition"]
                trade_params = definition.get("trade_params", {})

                matched = find_matching_markets(definition)
                if not matched:
                    continue

                for market in matched:
                    # Skip if we already have a pending/auto_approved/approved suggestion for this market+strategy in last 24h
                    existing = engine.query_one(
                        "SELECT id FROM suggestions WHERE type = 'trade' "
                        "AND status IN ('pending', 'auto_approved', 'approved') "
                        "AND payload LIKE ? AND payload LIKE ? "
                        "AND created_at > datetime('now', '-24 hours')",
                        (f'%"market_id": "{market["id"]}"%', f'%"strategy_id": "{strategy["id"]}"%'),
                    )
                    if existing:
                        logger.debug(f"Skipping duplicate suggestion for market {market['id']} + strategy {strategy['id']}")
                        continue

                    params = compute_trade_params(market, trade_params, capital)
                    if not params:
                        continue

                    params["strategy_id"] = strategy["id"]
                    params["strategy_name"] = strategy["name"]

                    # Pre-flight: open position, rebuy cooldown, min edge, circuit breaker
                    pf_ok, pf_reason = _preflight(market["id"], params.get("side", "YES"), params.get("edge", 0), params.get("amount_usd", 0))
                    if not pf_ok:
                        logger.debug(f"Preflight reject ({strategy['name']}): {pf_reason} for {market['id'][:30]}")
                        continue

                    status = "auto_approved" if mode == "full-auto" else "pending"
                    now = datetime.utcnow().isoformat()

                    # Dedup: skip if suggestion for this market exists in last 24h (any status)
                    _dup = engine.query_one(
                        "SELECT id FROM suggestions WHERE json_extract(payload, '$.market_id') = ? "
                        "AND created_at > datetime('now', '-24 hours')",
                        (market["id"],),
                    )
                    if _dup:
                        logger.debug(f"Strategy dedup: skip {market['id'][:30]} - already suggested")
                        continue

                    engine.execute(
                        """INSERT INTO suggestions (agent_id, type, title, description, payload, status, created_at)
                           VALUES (?, ?, ?, ?, ?, ?, ?)""",
                        (
                            strategy.get("discovered_by", "strategy-evaluator"),
                            "trade",
                            f"Strategy '{strategy['name']}': {params['side']} auf '{params['market_question'][:50]}...'",
                            f"Edge: {params['edge']:.1%} | ${params['amount_usd']:.2f} auf {params['side']} | Strategie: {strategy['name']}",
                            _json.dumps(params),
                            status,
                            now,
                        ),
                    )

                    # Track in strategy_trades
                    engine.execute(
                        """INSERT INTO strategy_trades (strategy_id, market_id, side, entry_price, amount_usd, is_backtest, created_at)
                           VALUES (?, ?, ?, ?, ?, 0, ?)""",
                        (strategy["id"], market["id"], params["side"], params["price"], params["amount_usd"], now),
                    )

                    total_suggestions += 1

            except Exception as e:
                logger.error(f"Strategy {strategy['id']} evaluation failed: {e}")

        if total_suggestions > 0:
            logger.info(f"Strategy evaluation: {total_suggestions} new suggestions from {len(strategies)} active strategies")

    except Exception as e:
        logger.error(f"Strategy evaluation failed: {e}")


def _job_score_strategies():
    """Update strategy confidence scores based on live trade performance."""
    try:
        from db import engine
        import json as _json

        platform_cfg = load_platform_config()
        strategy_cfg = platform_cfg.get("strategy", {})
        retire_trades = strategy_cfg.get("retire_after_trades", 50)
        retire_dd = strategy_cfg.get("retire_on_drawdown_pct", 20)

        strategies = engine.query("SELECT * FROM strategies WHERE status = 'active'")
        for strategy in strategies:
            # Count ALL strategy_trades (not just settled) for live_trades
            all_trades = engine.query(
                "SELECT COUNT(*) as cnt FROM strategy_trades "
                "WHERE strategy_id = ? AND is_backtest = 0",
                (strategy["id"],),
            )
            live_count = all_trades[0]["cnt"] if all_trades else 0

            # Get settled trades from strategy_trades directly (has own pnl/result columns)
            settled = engine.query(
                "SELECT pnl, result FROM strategy_trades "
                "WHERE strategy_id = ? AND is_backtest = 0 AND result IS NOT NULL",
                (strategy["id"],),
            )

            # Also estimate unrealized PnL for open strategy_trades via trades table
            unrealized = engine.query(
                "SELECT st.entry_price, st.amount_usd, t.price as current_price "
                "FROM strategy_trades st "
                "JOIN trades t ON st.market_id = t.market_id "
                "  AND t.status = 'executed' AND t.result IS NULL "
                "WHERE st.strategy_id = ? AND st.is_backtest = 0 AND st.result IS NULL "
                "GROUP BY st.id",
                (strategy["id"],),
            )

            # Calculate realized PnL from settled trades
            realized_pnl = sum(t.get("pnl") or 0 for t in settled) if settled else 0
            settled_count = len(settled) if settled else 0
            wins = sum(1 for t in settled if (t.get("pnl") or 0) > 0) if settled else 0

            # Calculate unrealized PnL estimate (current_price - entry_price) * shares
            unrealized_pnl = 0.0
            if unrealized:
                for t in unrealized:
                    entry = t.get("entry_price") or 0
                    amount = t.get("amount_usd") or 0
                    if entry > 0 and amount > 0:
                        shares = amount / entry
                        current = t.get("current_price") or entry
                        unrealized_pnl += (current - entry) * shares

            total_pnl = realized_pnl + unrealized_pnl
            win_rate = wins / settled_count if settled_count > 0 else 0

            # Confidence = weighted combination of win rate and profitability
            pnl_score = min(max(total_pnl / 10, -1), 1)
            confidence = round(0.6 * win_rate + 0.4 * (pnl_score + 1) / 2, 3) if settled_count > 0 else 0.5

            engine.execute(
                "UPDATE strategies SET live_trades = ?, live_pnl = ?, live_win_rate = ?, "
                "confidence_score = ?, updated_at = ? WHERE id = ?",
                (live_count, round(total_pnl, 2), round(win_rate, 3), confidence,
                 datetime.utcnow().isoformat(), strategy["id"]),
            )

            # Auto-retire on drawdown or trade limit
            if settled:
                max_dd = 0
                running_pnl = 0
                peak = 0
                for t in settled:
                    running_pnl += t.get("pnl") or 0
                    peak = max(peak, running_pnl)
                    dd = peak - running_pnl
                    max_dd = max(max_dd, dd)

                capital = platform_cfg.get("trading", {}).get("capital_usd", 100)
                dd_pct = (max_dd / capital * 100) if capital > 0 else 0

                if live_count >= retire_trades or dd_pct >= retire_dd:
                    engine.execute(
                        "UPDATE strategies SET status = 'retired', retired_at = ? WHERE id = ?",
                        (datetime.utcnow().isoformat(), strategy["id"]),
                    )
                    logger.info(f"Strategy {strategy['id']} retired (trades={live_count}, dd={dd_pct:.1f}%)")

        logger.debug(f"Strategy scoring: updated {len(strategies)} strategies")

    except Exception as e:
        logger.error(f"Strategy scoring failed: {e}")


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
    """Automatically retrain ML models (requires min 20 completed trades)."""
    try:
        from db import engine

        # Check minimum data threshold
        row = engine.query_one(
            "SELECT COUNT(*) as cnt FROM trades WHERE result IS NOT NULL AND pnl IS NOT NULL"
        )
        count = row["cnt"] if row else 0
        if count < 20:
            logger.debug(f"ML retrain skipped: only {count}/20 completed trades")
            return

        from ml.trainer import train_models
        results = train_models()
        if results.get("ok"):
            logger.info(f"ML auto-retrain completed ({count} trades)")
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

        # AI costs today
        cost_row = engine.query_one(
            "SELECT COALESCE(SUM(cost_usd), 0) as total FROM api_costs WHERE date(created_at) = ?", (today,)
        )
        cost = float(cost_row["total"]) if cost_row else 0

        # BUY trades today (exclude cashout/sell records)
        buy_row = engine.query_one(
            "SELECT COUNT(*) as cnt FROM trades WHERE date(executed_at) = ? "
            "AND status = 'executed' AND amount_usd > 0", (today,)
        )
        buys_today = int(buy_row["cnt"]) if buy_row else 0

        # Realized PnL today (only from closed positions — cashout, win, loss)
        pnl_row = engine.query_one(
            "SELECT COALESCE(SUM(pnl), 0) as total_pnl FROM trades WHERE date(executed_at) = ? "
            "AND result IN ('cashout', 'win', 'loss') AND amount_usd > 0", (today,)
        )
        realized_pnl = float(pnl_row["total_pnl"]) if pnl_row else 0

        # Overall portfolio PnL (from latest portfolio snapshot)
        snap = engine.query_one(
            "SELECT unrealized_pnl, realized_pnl, positions_value, position_count "
            "FROM portfolio_snapshots ORDER BY snapshot_at DESC LIMIT 1"
        )
        total_unrealized = float(snap["unrealized_pnl"]) if snap else 0
        total_realized = float(snap["realized_pnl"]) if snap else 0
        portfolio_value = float(snap["positions_value"]) if snap else 0
        open_positions = int(snap["position_count"]) if snap else 0

        # Executed suggestions today (actual trades, not expired/failed)
        sugg_row = engine.query_one(
            "SELECT COUNT(*) as cnt FROM suggestions WHERE date(created_at) = ? "
            "AND status IN ('executed', 'auto_approved', 'approved')", (today,)
        )
        suggestions = int(sugg_row["cnt"]) if sugg_row else 0

        summary = (
            f"Offene Positionen: {open_positions} (Wert: ${portfolio_value:,.2f})\n"
            f"Neue Käufe heute: {buys_today}\n"
            f"Realisiert heute: ${realized_pnl:+.2f}\n"
            f"Gesamt unrealisiert: ${total_unrealized:+.2f}\n"
            f"Gesamt realisiert: ${total_realized:+.2f}\n"
            f"AI-Kosten: ${cost:.2f}\n"
            f"Vorschläge heute: {suggestions}"
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


def _job_weather_edge_analysis(config: AppConfig):
    """Analyze weather/temperature markets using forecast data to calculate edge.

    Fetches real weather forecasts from Open-Meteo API, compares with Polymarket
    prices, and creates trade suggestions for markets with significant edge.
    """
    try:
        from db import engine
        from services.weather_forecast import parse_weather_market, analyze_weather_markets
        import json as _json

        platform_cfg = load_platform_config()
        trading_cfg = platform_cfg.get("trading", {})
        mode = trading_cfg.get("mode", "paper")
        min_edge = trading_cfg.get("weather_min_edge", 0.15)  # 15% default edge threshold

        # Find weather/temperature markets in DB
        weather_markets = engine.query(
            "SELECT id, question, yes_price, no_price, volume, liquidity "
            "FROM markets WHERE (question LIKE '%temperature%' OR question LIKE '%°F%' "
            "OR question LIKE '%°C%') AND accepting_orders = 1 "
            "AND yes_price > 0 AND yes_price < 1 "
            "ORDER BY volume DESC LIMIT 200"
        )

        if not weather_markets:
            logger.debug("Weather edge: no temperature markets found")
            return

        # Filter to only parseable markets
        parseable = []
        for m in weather_markets:
            parsed = parse_weather_market(m["question"])
            if parsed:
                parseable.append({
                    "id": m["id"],
                    "question": m["question"],
                    "yes_price": m["yes_price"],
                    "no_price": m["no_price"],
                })

        if not parseable:
            logger.debug(f"Weather edge: {len(weather_markets)} temperature markets found but none parseable")
            return

        # Analyze all parseable markets (batched with forecast caching)
        results = analyze_weather_markets(parseable)

        # Update calculated_edge in DB
        updated = 0
        for r in results:
            engine.execute(
                "UPDATE markets SET calculated_edge = ?, last_updated = datetime('now') WHERE id = ?",
                (r["edge"], r["market_id"]),
            )
            updated += 1

        # Create trade suggestions for markets with sufficient edge
        suggestions_created = 0
        for r in results:
            abs_edge = r["abs_edge"]
            if abs_edge < min_edge:
                continue

            market_id = r["market_id"]
            side = r["side"]

            # Skip if we already have an open position for this market
            open_pos = engine.query_one(
                "SELECT id FROM trades WHERE market_id = ? AND status IN ('executed', 'executing') "
                "AND (result IS NULL OR result = 'open')",
                (market_id,),
            )
            if open_pos:
                continue

            # Skip if recently closed (rebuy cooldown) - catches ALL result types
            rebuy_cooldown_days = trading_cfg.get("rebuy_cooldown_days", 7)
            last_closed = engine.query_one(
                "SELECT MAX(executed_at) as last_close FROM trades WHERE market_id = ? "
                "AND status = 'closed' AND result IS NOT NULL",
                (market_id,),
            )
            if last_closed and last_closed.get("last_close"):
                try:
                    closed_at = datetime.fromisoformat(last_closed["last_close"])
                    if (datetime.utcnow() - closed_at).days < rebuy_cooldown_days:
                        continue
                except (ValueError, TypeError):
                    pass

            # Skip if we already have a pending/approved/executed suggestion for this market
            existing = engine.query_one(
                "SELECT id FROM suggestions WHERE type = 'trade' "
                "AND status IN ('pending', 'auto_approved', 'executed') "
                "AND payload LIKE ?",
                (f'%"market_id": "{market_id}"%',),
            )
            if existing:
                continue

            # Calculate amount (Kelly-lite: edge * fraction of max position)
            capital = trading_cfg.get("capital_usd", 100.0)
            limits = trading_cfg.get("limits", {})
            max_pct = limits.get("max_position_pct", 5) / 100
            max_amount = capital * max_pct

            # Scale amount by edge strength (higher edge = larger bet, capped)
            amount = min(round(abs_edge * capital * 0.5, 2), max_amount)
            amount = max(amount, 1.0)  # minimum $1

            price = r["yes_price"] if side == "YES" else (1 - r["yes_price"])

            # Max entry price check - skip if price too high (no upside)
            max_entry = ws_config.get("max_entry_price", 0.93)
            if price > max_entry:
                logger.debug(f"Weather: skip {market_id[:30]} - price {price:.3f} > max {max_entry}")
                continue

            status = "auto_approved" if mode == "full-auto" else "pending"

            payload = {
                "market_id": market_id,
                "market_question": r["question"],
                "side": side,
                "amount_usd": amount,
                "price": price,
                "edge": r["edge"],
                "fair_probability": r["fair_probability"],
                "forecast_temp_c": r["forecast_temp_c"],
                "days_ahead": r["days_ahead"],
                "strategy_name": "Weather Forecast Edge",
            }

            # Dedup: skip if suggestion for this market exists in last 24h (any status)
            _dup = engine.query_one(
                "SELECT id FROM suggestions WHERE json_extract(payload, '$.market_id') = ? "
                "AND created_at > datetime('now', '-24 hours')",
                (market_id,),
            )
            if _dup:
                logger.debug(f"Weather dedup: skip {market_id[:30]} - already suggested")
                continue

            engine.execute(
                """INSERT INTO suggestions (agent_id, type, title, description, payload, status, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, datetime('now'))""",
                (
                    "weather-forecast",
                    "trade",
                    f"Weather: {side} '{r['question'][:50]}...'",
                    f"Edge: {r['edge']:+.1%} | Fair: {r['fair_probability']:.0%} vs Market: {r['yes_price']:.0%} | "
                    f"Forecast: {r['forecast_temp_c']}°C | {r['days_ahead']}d ahead | {r['city'].title()}",
                    _json.dumps(payload),
                    status,
                ),
            )
            suggestions_created += 1

        logger.info(
            f"Weather edge: {len(parseable)} parseable / {len(weather_markets)} total weather markets, "
            f"{updated} edges updated, {suggestions_created} suggestions created (min_edge={min_edge:.0%})"
        )

    except Exception as e:
        logger.error(f"Weather edge analysis failed: {e}")


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


def _job_cleanup_snapshots():
    """Remove market snapshots older than 30 days."""
    try:
        from db import engine
        engine.execute(
            "DELETE FROM market_snapshots WHERE snapshot_at < datetime('now', '-30 days')"
        )
        logger.debug("Snapshot cleanup completed")
    except Exception as e:
        logger.error(f"Snapshot cleanup failed: {e}")


def _job_pattern_scanner(config: AppConfig):
    """
    Pattern Scanner: Find statistically significant win patterns in settled trades.
    Groups trades by category, price bucket, volume bucket, side, etc.
    If a pattern has significantly higher win rate than average, propose it as strategy.
    Runs daily — needs Settlement data to work.
    """
    try:
        from db import engine
        from services.telegram_alerts import get_alerts

        # Need at least 15 settled trades to find meaningful patterns
        count_row = engine.query_one(
            "SELECT COUNT(*) as cnt FROM trades WHERE result IS NOT NULL"
        )
        total_trades = count_row["cnt"] if count_row else 0
        if total_trades < 15:
            logger.debug(f"Pattern scanner: only {total_trades}/15 settled trades, skipping")
            return

        # Overall baseline win rate
        stats = engine.query_one(
            "SELECT COUNT(*) as total, "
            "SUM(CASE WHEN result='win' THEN 1 ELSE 0 END) as wins, "
            "COALESCE(SUM(pnl), 0) as total_pnl "
            "FROM trades WHERE result IS NOT NULL"
        )
        baseline_wr = stats["wins"] / stats["total"] if stats["total"] > 0 else 0

        patterns_found = []

        # --- Pattern dimensions to scan ---

        # 1. By category
        by_category = engine.query(
            "SELECT m.category, COUNT(*) as cnt, "
            "SUM(CASE WHEN t.result='win' THEN 1 ELSE 0 END) as wins, "
            "COALESCE(SUM(t.pnl), 0) as pnl "
            "FROM trades t JOIN markets m ON t.market_id = m.id "
            "WHERE t.result IS NOT NULL AND m.category != '' "
            "GROUP BY m.category HAVING cnt >= 5"
        )
        for row in by_category:
            wr = row["wins"] / row["cnt"]
            if wr > baseline_wr + 0.10 and row["cnt"] >= 5:
                patterns_found.append({
                    "name": f"Category: {row['category']}",
                    "rule": f"category = '{row['category']}'",
                    "trades": row["cnt"], "win_rate": wr, "pnl": row["pnl"],
                })

        # 2. By price bucket (entry price)
        price_buckets = [
            ("Under 20¢", 0, 0.20),
            ("20-40¢", 0.20, 0.40),
            ("40-60¢", 0.40, 0.60),
            ("60-80¢", 0.60, 0.80),
            ("Over 80¢", 0.80, 1.01),
        ]
        for label, low, high in price_buckets:
            row = engine.query_one(
                "SELECT COUNT(*) as cnt, "
                "SUM(CASE WHEN result='win' THEN 1 ELSE 0 END) as wins, "
                "COALESCE(SUM(pnl), 0) as pnl "
                "FROM trades WHERE result IS NOT NULL AND price >= ? AND price < ?",
                (low, high),
            )
            if row and row["cnt"] >= 5:
                wr = row["wins"] / row["cnt"]
                if wr > baseline_wr + 0.10:
                    patterns_found.append({
                        "name": f"Price: {label}",
                        "rule": f"price >= {low} AND price < {high}",
                        "trades": row["cnt"], "win_rate": wr, "pnl": row["pnl"],
                    })

        # 3. By side (YES vs NO)
        by_side = engine.query(
            "SELECT side, COUNT(*) as cnt, "
            "SUM(CASE WHEN result='win' THEN 1 ELSE 0 END) as wins, "
            "COALESCE(SUM(pnl), 0) as pnl "
            "FROM trades WHERE result IS NOT NULL "
            "GROUP BY side HAVING cnt >= 5"
        )
        for row in by_side:
            wr = row["wins"] / row["cnt"]
            if wr > baseline_wr + 0.10:
                patterns_found.append({
                    "name": f"Side: {row['side']}",
                    "rule": f"side = '{row['side']}'",
                    "trades": row["cnt"], "win_rate": wr, "pnl": row["pnl"],
                })

        # 4. By volume bucket
        vol_buckets = [
            ("Low Volume (<$50k)", 0, 50000),
            ("Medium ($50k-$500k)", 50000, 500000),
            ("High Volume (>$500k)", 500000, 999999999),
        ]
        for label, low, high in vol_buckets:
            row = engine.query_one(
                "SELECT COUNT(*) as cnt, "
                "SUM(CASE WHEN t.result='win' THEN 1 ELSE 0 END) as wins, "
                "COALESCE(SUM(t.pnl), 0) as pnl "
                "FROM trades t JOIN markets m ON t.market_id = m.id "
                "WHERE t.result IS NOT NULL AND m.volume >= ? AND m.volume < ?",
                (low, high),
            )
            if row and row["cnt"] >= 5:
                wr = row["wins"] / row["cnt"]
                if wr > baseline_wr + 0.10:
                    patterns_found.append({
                        "name": f"Volume: {label}",
                        "rule": f"volume >= {low} AND volume < {high}",
                        "trades": row["cnt"], "win_rate": wr, "pnl": row["pnl"],
                    })

        # --- Report findings ---
        if patterns_found:
            # Sort by win rate
            patterns_found.sort(key=lambda p: p["win_rate"], reverse=True)

            msg_lines = ["🔍 <b>Pattern Scanner — Ergebnisse</b>\n"]
            msg_lines.append(f"Basis Win-Rate: {baseline_wr:.0%} ({stats['total']} Trades)\n")

            for p in patterns_found[:5]:
                msg_lines.append(
                    f"✅ <b>{p['name']}</b>\n"
                    f"   Win-Rate: {p['win_rate']:.0%} ({p['trades']} Trades)\n"
                    f"   PnL: ${p['pnl']:+.2f}\n"
                )

            alerts = get_alerts(config)
            alerts.send("\n".join(msg_lines))

            # Save patterns to DB as draft strategies (if not already existing)
            import json as json_mod
            for p in patterns_found[:3]:
                existing = engine.query_one(
                    "SELECT id FROM strategies WHERE name = ?",
                    (f"Pattern: {p['name']}",),
                )
                if not existing:
                    import uuid
                    strat_id = f"pattern_{uuid.uuid4().hex[:8]}"
                    engine.execute(
                        "INSERT INTO strategies (id, name, description, status, definition, created_at) "
                        "VALUES (?, ?, ?, 'draft', ?, datetime('now'))",
                        (
                            strat_id,
                            f"Pattern: {p['name']}",
                            f"Auto-discovered: {p['win_rate']:.0%} win rate over {p['trades']} trades",
                            json_mod.dumps({
                                "source": "pattern_scanner",
                                "rule": p["rule"],
                                "win_rate": p["win_rate"],
                                "sample_size": p["trades"],
                                "pnl": p["pnl"],
                            }),
                        ),
                    )

            logger.info(f"Pattern scanner: found {len(patterns_found)} patterns, top: {patterns_found[0]['name']}")
        else:
            logger.info(f"Pattern scanner: no significant patterns in {total_trades} trades (baseline WR: {baseline_wr:.0%})")

    except Exception as e:
        logger.error(f"Pattern scanner failed: {e}")


# Settlement and position sync removed — handled by PositionManager


def _job_portfolio_snapshot(config: AppConfig):
    """Fetch real portfolio value from Polymarket Data API and store snapshot.

    Uses the /positions endpoint with the funder (proxy wallet) address.
    Each position includes: currentValue, cashPnl, realizedPnl from Polymarket.
    """
    try:
        import httpx
        from db import engine

        funder = config.polymarket_funder
        if not funder:
            logger.warning("Portfolio snapshot: no POLYMARKET_FUNDER configured")
            return

        resp = httpx.get(
            "https://data-api.polymarket.com/positions",
            params={"user": funder},
            timeout=30,
        )
        resp.raise_for_status()
        positions = resp.json()

        total_value = 0.0
        total_cost = 0.0
        total_pnl = 0.0
        total_realized = 0.0
        position_count = len(positions)

        for p in positions:
            total_value += float(p.get("currentValue", 0) or 0)
            total_cost += float(p.get("initialValue", 0) or 0)
            total_pnl += float(p.get("cashPnl", 0) or 0)
            total_realized += float(p.get("realizedPnl", 0) or 0)

        # Get total_deposited from config
        platform_cfg = load_platform_config()
        total_deposited = platform_cfg.get("trading", {}).get("total_deposited", 0)

        # Store snapshot
        engine.execute(
            """INSERT INTO portfolio_snapshots
               (snapshot_at, total_deposited, positions_value, positions_cost,
                unrealized_pnl, realized_pnl, position_count)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (datetime.utcnow().isoformat(),
             total_deposited,
             round(total_value, 2),
             round(total_cost, 2),
             round(total_pnl, 2),
             round(total_realized, 2),
             position_count),
        )

        logger.info(
            f"Portfolio snapshot: {position_count} positions, "
            f"value=${total_value:.2f}, cost=${total_cost:.2f}, "
            f"pnl=${total_pnl:.2f}, realized=${total_realized:.2f}"
        )

    except Exception as e:
        logger.error(f"Portfolio snapshot failed: {e}")


def _job_expire_old_suggestions():
    """Expire auto_approved and pending suggestions older than 24 hours.

    Stuck suggestions block the deduplication check and prevent new trades.
    """
    try:
        from db import engine

        result = engine.execute(
            "UPDATE suggestions SET status = 'expired' "
            "WHERE status IN ('auto_approved', 'pending') "
            "AND created_at < datetime('now', '-24 hours')"
        )
        expired = result.rowcount if hasattr(result, 'rowcount') else 0
        if expired:
            logger.info(f"Expired {expired} old stuck suggestions")
    except Exception as e:
        logger.error(f"Expire suggestions failed: {e}")


def _job_daily_backtests():
    """Run snapshot-based backtests for all active strategies."""
    try:
        from services.backtester import run_all_backtests
        logger.info("Starting daily snapshot backtests...")
        results = run_all_backtests(days=7)
        profitable = sum(1 for r in results if r.get("total_pnl", 0) > 0)
        logger.info(
            f"Daily backtests complete: {len(results)} strategies, "
            f"{profitable} profitable"
        )

        # Send summary via telegram if available
        try:
            from services.telegram_alerts import TelegramAlerts
            alerts = TelegramAlerts()
            lines = ["Daily Backtest Results (7d):"]
            for r in sorted(results, key=lambda x: x.get("total_pnl", 0), reverse=True):
                pnl = r.get("total_pnl", 0)
                wr = r.get("win_rate", 0)
                trades = r.get("total_trades", 0)
                emoji = "+" if pnl > 0 else ""
                lines.append(
                    f"  {r['strategy_name']}: {emoji}${pnl:.2f} "
                    f"(WR: {wr:.0%}, {trades} trades)"
                )
            alerts.send_message("\n".join(lines))
        except Exception as e:
            logger.debug(f"Telegram alert for backtests skipped: {e}")

    except Exception as e:
        logger.error(f"Daily backtests failed: {e}", exc_info=True)


def _job_new_market_scan(config: AppConfig):
    """Scan for newly created markets and detect mispricing opportunities."""
    try:
        from services.new_market_scanner import run_new_market_scan
        opportunities = run_new_market_scan(config)
        if opportunities:
            logger.info(f"New market scan: {len(opportunities)} opportunities found")
        else:
            logger.debug("New market scan: no opportunities")
    except Exception as e:
        logger.error(f"New market scan failed: {e}")


# -----------------------------------------------
# Resolution Sniper Jobs
# -----------------------------------------------

def _job_weather_sniper(config: AppConfig):
    """Resolution Sniper: Weather - hourly forecasts for today/tomorrow markets."""
    try:
        from services.resolution_sniper import run_weather_sniper
        count = run_weather_sniper(config)
        if count > 0:
            logger.info(f"Weather sniper: {count} snipe suggestions created")
    except Exception as e:
        logger.error(f"Weather sniper job failed: {e}")


def _job_economic_sniper(config: AppConfig):
    """Resolution Sniper: Economic data - check for fresh CPI/Fed/jobs releases."""
    try:
        from services.resolution_sniper import run_economic_sniper
        count = run_economic_sniper(config)
        if count > 0:
            logger.info(f"Economic sniper: {count} snipe suggestions created")
    except Exception as e:
        logger.error(f"Economic sniper job failed: {e}")


def _job_sport_sniper(config: AppConfig):
    """Resolution Sniper: Sport scores - check live scores for finished games."""
    try:
        from services.resolution_sniper import run_sport_sniper
        count = run_sport_sniper(config)
        if count > 0:
            logger.info(f"Sport sniper: {count} snipe suggestions created")
    except Exception as e:
        logger.error(f"Sport sniper job failed: {e}")


def _job_edge_sources(config: AppConfig):
    """Run all edge sources: crypto (Binance), cross-platform (Manifold), weather ensemble."""
    try:
        from services.edge_sources import run_all_edge_sources
        results = run_all_edge_sources(config)
        total = sum(results.values())
        if total > 0:
            logger.info(f"Edge sources: {total} markets updated | {results}")
    except Exception as e:
        logger.error(f"Edge sources job failed: {e}")


def _job_arbitrage_scan(config: AppConfig):
    """Scan for arbitrage opportunities across all three types."""
    try:
        import json as _json
        from services.arbitrage_scanner import ArbitrageScanner
        from services.telegram_alerts import TelegramAlerts
        from db import engine
        from config import load_platform_config

        platform_cfg = load_platform_config()
        trading_cfg = platform_cfg.get("trading", {})
        mode = trading_cfg.get("mode", "paper")

        arb_cfg = platform_cfg.get("arbitrage", {})
        min_abs_profit = arb_cfg.get("min_absolute_profit", 1.00)

        scanner = ArbitrageScanner()
        try:
            opportunities = scanner.scan_all(min_profit_usd=min_abs_profit, arb_config=arb_cfg)
        finally:
            scanner.close()

        if not opportunities:
            logger.info("Arbitrage scan: no opportunities found")
            return

        alerts = TelegramAlerts(config)
        actionable = 0

        for opp in opportunities:
            opp_dict = opp.to_dict()

            # Blocklist check: skip if any market is blocklisted
            if any(_is_market_blocklisted(m.get("question", "")) for m in opp.markets):
                logger.info(f"Arb blocklist: skipping {opp.description[:60]}")
                continue

            # Log all opportunities
            logger.info(
                f"Arbitrage [{opp.arb_type}]: {opp.description} "
                f"(profit=${opp.profit_usd:.2f}, confidence={opp.confidence:.0%})"
            )

            # Only act on opportunities with guaranteed profit > $0.50
            if opp.profit_usd >= 0.50 and opp.confidence >= 0.85:
                actionable += 1

                # Create trade suggestion
                status = "auto_approved" if mode == "full-auto" else "pending"
                now = datetime.utcnow().isoformat()

                # Build payload for the trade
                payload = {
                    "arb_type": opp.arb_type,
                    "strategy_id": "strat_arbitrage",
                    "strategy_name": "Arbitrage Scanner",
                    "markets": opp.markets,
                    "profit_usd": opp.profit_usd,
                    "confidence": opp.confidence,
                    "description": opp.description,
                }

                # For YES/NO arb with 2 sides, create one suggestion per side
                if opp.arb_type == "yes_no" and len(opp.markets) == 2:
                    for mkt in opp.markets:
                        trade_payload = {
                            "market_id": mkt["market_id"],
                            "market_question": mkt["question"][:100],
                            "side": mkt["side"],
                            "price": mkt["price"],
                            "amount_usd": min(12.50, opp.profit_usd * 10),
                            "edge": opp.profit_usd / 25.0,
                            "token_id": mkt.get("token_id", ""),
                            "strategy_id": "strat_arbitrage",
                            "strategy_name": "Arbitrage Scanner",
                            "arb_type": opp.arb_type,
                            "arb_profit": opp.profit_usd,
                        }

                        # Dedup check (any status in last 24h)
                        existing = engine.query_one(
                            "SELECT id, status FROM suggestions WHERE json_extract(payload, '$.market_id') = ? "
                            "AND created_at > datetime('now', '-24 hours')",
                            (mkt["market_id"],),
                        )
                        if existing:
                            logger.info(f"Arb dedup: skip {mkt['market_id']} - suggestion exists (status={existing.get('status', '?')})")
                            continue

                        # Pre-flight check
                        pf_ok, pf_reason = _preflight(mkt["market_id"], mkt["side"], trade_payload.get("edge", 0), trade_payload.get("amount_usd", 0))
                        if not pf_ok:
                            logger.debug(f"Arb preflight reject: {pf_reason}")
                            continue

                        engine.execute(
                            """INSERT INTO suggestions (agent_id, type, title, description, payload, status, created_at)
                               VALUES (?, ?, ?, ?, ?, ?, ?)""",
                            (
                                "arbitrage-scanner",
                                "trade",
                                f"Arbitrage: {mkt['side']} auf '{mkt['question'][:50]}...'",
                                opp.description,
                                _json.dumps(trade_payload),
                                status,
                                now,
                            ),
                        )
                else:
                    # For multi-outcome or other types, create info suggestion
                    # Dedup: check if any market in this arb was suggested in last 24h
                    _skip_multi = False
                    for _m in opp.markets:
                        _mid = _m.get("market_id", "")
                        if _mid:
                            _ex = engine.query_one(
                                "SELECT id FROM suggestions WHERE json_extract(payload, '$.market_id') = ? "
                                "AND created_at > datetime('now', '-24 hours')",
                                (_mid,),
                            )
                            if _ex:
                                logger.info(f"Arb dedup (multi): skip - market {_mid} already suggested")
                                _skip_multi = True
                                break
                    if _skip_multi:
                        continue
                    engine.execute(
                        """INSERT INTO suggestions (agent_id, type, title, description, payload, status, created_at)
                           VALUES (?, ?, ?, ?, ?, ?, ?)""",
                        (
                            "arbitrage-scanner",
                            "trade",
                            f"Arbitrage ({opp.arb_type}): ${opp.profit_usd:.2f} Profit",
                            opp.description,
                            _json.dumps(payload),
                            status,
                            datetime.utcnow().isoformat(),
                        ),
                    )

                # Telegram alert removed — alerts only at trade execution in trader.py
                logger.info(f"Arbitrage suggestion created: {opp.arb_type} ${opp.profit_usd:.2f}")

        logger.info(
            f"Arbitrage scan: {len(opportunities)} opportunities found, "
            f"{actionable} actionable (>$0.50 profit)"
        )

    except Exception as e:
        logger.error(f"Arbitrage scan failed: {e}")


def _job_edge_sources(config):
    """Run all external edge data sources."""
    try:
        from services.edge_sources import run_all_edge_sources
        from config import load_platform_config

        platform_cfg = load_platform_config()
        summary = run_all_edge_sources(platform_cfg)
        logger.info(f"Edge sources completed: {summary}")
    except Exception as e:
        logger.error(f"Edge sources scan failed: {e}")


def _job_health_monitor(config):
    """Check all strategy heartbeats and alert on issues."""
    try:
        from services.health_monitor import run_health_monitor
        from config import load_platform_config

        platform_cfg = load_platform_config()
        report = run_health_monitor(platform_cfg)

        ok = len(report.get("ok", []))
        warn = len(report.get("warning", []))
        crit = len(report.get("critical", []))
        logger.info(f"Health Monitor: {ok} OK, {warn} warnings, {crit} critical")
    except Exception as e:
        logger.error(f"Health monitor failed: {e}")


def _job_live_sports_edge(config):
    """Scan live games for sports trading edges."""
    try:
        from services.live_sports_edge import run_live_sports_edge
        from config import load_platform_config

        platform_cfg = load_platform_config()
        results = run_live_sports_edge(platform_cfg)
        if results:
            logger.info(f"Live Sports: {len(results)} edges found")
    except Exception as e:
        logger.error(f"Live sports edge scan failed: {e}")
