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

            # Chief analysis cycle (1x/day at 19:00 UTC — before daily summary at 20:00)
            chief_hour = sched_cfg.get("chief_analysis_hour", 19)
            _scheduler.add_job(
                _job_run_chief_cycle, "cron", hour=chief_hour, minute=0,
                id="chief_analysis", replace_existing=True, args=[config],
            )
            logger.info(f"Scheduled: chief_analysis daily at {chief_hour}:00 UTC")

            # Analyst cycle (1x/day at 08:00 UTC)
            analyst_hour = sched_cfg.get("analyst_hour", 8)
            _scheduler.add_job(
                _job_run_analyst_cycle, "cron", hour=analyst_hour, minute=0,
                id="analyst_cycle", replace_existing=True, args=[config],
            )
            logger.info(f"Scheduled: analyst_cycle daily at {analyst_hour}:00 UTC")

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
            )
            logger.info(f"Scheduled: trader_cycle every {trader_interval}min")

            # Pattern Scanner (daily at 09:00 UTC — after analyst at 08:00)
            pattern_hour = sched_cfg.get("pattern_scanner_hour", 9)
            _scheduler.add_job(
                _job_pattern_scanner, "cron", hour=pattern_hour, minute=0,
                id="pattern_scanner", replace_existing=True, args=[config],
            )
            logger.info(f"Scheduled: pattern_scanner daily at {pattern_hour}:00 UTC")

            # Settlement: check if markets resolved, update trade results
            settlement_interval = sched_cfg.get("settlement", {}).get("interval_minutes", 30)
            _scheduler.add_job(
                _job_settle_trades, "interval", minutes=settlement_interval,
                id="settlement", replace_existing=True, args=[config],
            )
            logger.info(f"Scheduled: settlement every {settlement_interval}min")

            # Position sync (every 30min — sync real on-chain positions into DB)
            _scheduler.add_job(
                _job_sync_positions, "interval", minutes=30,
                id="position_sync", replace_existing=True, args=[config],
            )
            logger.info("Scheduled: position_sync every 30min")

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
            _scheduler.add_job(
                _job_weather_edge_analysis, "interval", minutes=30,
                id="weather_edge", replace_existing=True, args=[config],
            )
            logger.info("Scheduled: weather_edge_analysis every 30min")

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

        now = datetime.utcnow().isoformat()

        for market in markets:
            engine.execute(
                """INSERT OR REPLACE INTO markets
                   (id, question, slug, yes_price, no_price, volume, liquidity,
                    end_date, category, yes_token_id, no_token_id,
                    best_bid, best_ask, spread, volume_24h, volume_1w, volume_1m,
                    last_trade_price, accepting_orders, last_updated)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (market["id"], market["question"], market.get("slug", ""),
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

        # Compute whale/smart-money signals for top 20 markets
        whale_count = 0
        try:
            from services.data_api_client import DataAPIClient

            data_client = DataAPIClient()
            top_for_whale = engine.query(
                "SELECT id FROM markets "
                "WHERE accepting_orders = 1 "
                "ORDER BY volume DESC LIMIT 20"
            )
            for m in top_for_whale:
                mid = m["id"]
                whale = data_client.compute_whale_signals(mid)
                conc = data_client.compute_holder_concentration(mid)
                oi = data_client.get_open_interest(mid)
                sm_score = data_client.compute_smart_money_score(mid)

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

                engine.execute(
                    """UPDATE markets SET
                       whale_buy_count = ?, whale_sell_count = ?, whale_net_flow = ?,
                       top_holder_concentration = ?, open_interest = ?,
                       oi_change_24h = COALESCE(?, oi_change_24h),
                       smart_money_score = ?
                       WHERE id = ?""",
                    (whale["whale_buy_count"], whale["whale_sell_count"],
                     whale["whale_net_flow"], conc, oi, oi_change, sm_score, mid),
                )
                whale_count += 1

            data_client.close()
        except Exception as e:
            logger.error(f"Whale signal computation failed: {e}")

        logger.info(
            f"Market refresh: {len(markets)} markets updated + snapshots saved + "
            f"{ob_count} order books + {whale_count} whale signals analyzed"
        )
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
    """Run enabled agent cycles (skip analyst — it has its own daily schedule)."""
    try:
        from config import load_agent_configs
        from agents.agent_factory import create_agent
        from services.telegram_bridge import get_bridge
        from services.cost_tracker import check_budget

        bridge = get_bridge(config)
        configs = load_agent_configs()

        # Agents that have their own dedicated schedule
        _SEPARATE_SCHEDULE = {"chief", "analyst"}

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
                    # Skip if we already have a pending/auto_approved suggestion for this market+strategy
                    existing = engine.query_one(
                        "SELECT id FROM suggestions WHERE type = 'trade' AND status IN ('pending', 'auto_approved') "
                        "AND payload LIKE ? AND payload LIKE ?",
                        (f'%"market_id": "{market["id"]}"%', f'%"strategy_id": "{strategy["id"]}"%'),
                    )
                    if existing:
                        continue

                    params = compute_trade_params(market, trade_params, capital)
                    if not params:
                        continue

                    params["strategy_id"] = strategy["id"]
                    params["strategy_name"] = strategy["name"]

                    status = "auto_approved" if mode == "full-auto" else "pending"
                    now = datetime.utcnow().isoformat()

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
            trades = engine.query(
                "SELECT pnl, result FROM strategy_trades st "
                "JOIN trades t ON st.trade_id = t.id "
                "WHERE st.strategy_id = ? AND t.result IS NOT NULL",
                (strategy["id"],),
            )
            if not trades:
                continue

            total = len(trades)
            wins = sum(1 for t in trades if (t.get("pnl") or 0) > 0)
            total_pnl = sum(t.get("pnl") or 0 for t in trades)
            win_rate = wins / total if total > 0 else 0

            # Confidence = weighted combination of win rate and profitability
            pnl_score = min(max(total_pnl / 10, -1), 1)  # Normalize PnL to [-1, 1]
            confidence = round(0.6 * win_rate + 0.4 * (pnl_score + 1) / 2, 3)

            engine.execute(
                "UPDATE strategies SET live_trades = ?, live_pnl = ?, live_win_rate = ?, "
                "confidence_score = ?, updated_at = ? WHERE id = ?",
                (total, round(total_pnl, 2), round(win_rate, 3), confidence,
                 datetime.utcnow().isoformat(), strategy["id"]),
            )

            # Auto-retire on drawdown or trade limit
            max_dd = 0
            running_pnl = 0
            peak = 0
            for t in trades:
                running_pnl += t.get("pnl") or 0
                peak = max(peak, running_pnl)
                dd = peak - running_pnl
                max_dd = max(max_dd, dd)

            capital = platform_cfg.get("trading", {}).get("capital_usd", 100)
            dd_pct = (max_dd / capital * 100) if capital > 0 else 0

            if total >= retire_trades or dd_pct >= retire_dd:
                engine.execute(
                    "UPDATE strategies SET status = 'retired', retired_at = ? WHERE id = ?",
                    (datetime.utcnow().isoformat(), strategy["id"]),
                )
                logger.info(f"Strategy {strategy['id']} retired (trades={total}, dd={dd_pct:.1f}%)")

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
                "SELECT id FROM trades WHERE market_id = ? AND status = 'executed' "
                "AND (result IS NULL OR result = 'open')",
                (market_id,),
            )
            if open_pos:
                continue

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


def _job_settle_trades(config: AppConfig):
    """
    Settlement: Check if any executed trades have been resolved.
    For each open trade, query Gamma API to see if the market resolved.
    If resolved, calculate PnL, update DB, notify via Telegram, update circuit breaker.
    """
    try:
        from db import engine
        from services.polymarket_client import PolymarketService
        from services.telegram_alerts import get_alerts

        # Find all executed trades that haven't been settled yet
        open_trades = engine.query(
            "SELECT id, market_id, market_question, side, amount_usd, price "
            "FROM trades WHERE status = 'executed' AND result IS NULL"
        )

        if not open_trades:
            return

        service = PolymarketService(config)
        alerts = get_alerts(config)
        settled_count = 0
        wins = 0
        losses = 0
        total_pnl = 0.0

        # Cache resolution results per market (multiple trades may share a market)
        resolution_cache = {}

        for trade in open_trades:
            market_id = trade["market_id"]

            # Check cache first
            if market_id not in resolution_cache:
                resolution_cache[market_id] = service.get_market_resolution(market_id)

            resolution = resolution_cache[market_id]
            if not resolution or not resolution.get("resolved"):
                continue

            # Calculate PnL
            winning_side = resolution["winning_side"]
            trade_side = trade["side"]
            entry_price = trade["price"] or 0
            amount = trade["amount_usd"] or 0

            if entry_price <= 0 or amount <= 0:
                logger.warning(f"Trade {trade['id']}: invalid price/amount, skipping")
                continue

            # How many shares did we buy?
            shares = amount / entry_price

            if trade_side == winning_side:
                # Won: each share pays out $1
                payout = shares * 1.0
                pnl = payout - amount
                result = "win"
                wins += 1
            else:
                # Lost: shares are worth $0
                pnl = -amount
                result = "loss"
                losses += 1

            pnl = round(pnl, 4)
            total_pnl += pnl

            # Update trade record
            engine.execute(
                "UPDATE trades SET result = ?, pnl = ? WHERE id = ?",
                (result, pnl, trade["id"]),
            )

            # Also update strategy_trades if linked
            engine.execute(
                "UPDATE strategy_trades SET result = ?, pnl = ?, exit_price = ? "
                "WHERE market_id = ? AND result IS NULL",
                (result, pnl, 1.0 if result == "win" else 0.0, market_id),
            )

            settled_count += 1

            # Send Telegram notification
            market_name = (trade.get("market_question") or market_id)[:80]
            alerts.alert_trade_settled(market_name, trade_side, result, pnl, amount)

        if settled_count > 0:
            # Update circuit breaker with consecutive losses
            _update_circuit_breaker_from_settlements(config)

            logger.info(
                f"Settlement: {settled_count} trades settled "
                f"({wins}W/{losses}L, PnL: ${total_pnl:+.2f})"
            )

    except Exception as e:
        logger.error(f"Settlement job failed: {e}")


def _update_circuit_breaker_from_settlements(config: AppConfig):
    """Recalculate circuit breaker state based on recent trade results."""
    try:
        from db import engine

        # Get last N trades with results, ordered by execution time
        recent = engine.query(
            "SELECT result FROM trades WHERE result IS NOT NULL "
            "ORDER BY executed_at DESC LIMIT 10"
        )

        if not recent:
            return

        # Count consecutive losses from the most recent trade
        consecutive_losses = 0
        for trade in recent:
            if trade["result"] == "loss":
                consecutive_losses += 1
            else:
                break

        platform_cfg = load_platform_config()
        cb_cfg = platform_cfg.get("circuit_breaker", {})
        max_losses = cb_cfg.get("max_consecutive_losses", 3)
        pause_hours = cb_cfg.get("pause_hours", 24)

        if consecutive_losses >= max_losses:
            from datetime import timedelta
            paused_until = (datetime.utcnow() + timedelta(hours=pause_hours)).isoformat()
            engine.execute(
                "INSERT OR REPLACE INTO circuit_breaker (id, consecutive_losses, paused_until) "
                "VALUES (1, ?, ?)",
                (consecutive_losses, paused_until),
            )

            from services.telegram_alerts import get_alerts
            alerts = get_alerts(config)
            alerts.alert_circuit_breaker(consecutive_losses, paused_until)
            logger.warning(f"Circuit breaker ACTIVATED: {consecutive_losses} consecutive losses")
        else:
            # Reset circuit breaker if not triggered
            engine.execute(
                "INSERT OR REPLACE INTO circuit_breaker (id, consecutive_losses, paused_until) "
                "VALUES (1, ?, NULL)",
                (consecutive_losses,),
            )

    except Exception as e:
        logger.error(f"Circuit breaker update failed: {e}")


def _job_sync_positions(config: AppConfig):
    """
    Sync real on-chain positions from Polymarket Data API into the DB.
    Creates trade records for positions that aren't tracked yet,
    and updates entry prices from actual trade data.
    """
    try:
        from db import engine
        from services.polymarket_client import PolymarketService

        funder = config.polymarket_funder
        if not funder:
            return

        service = PolymarketService(config)
        positions = service.get_user_positions(funder)

        if not positions:
            logger.debug("Position sync: no on-chain positions found")
            return

        synced = 0
        for pos in positions:
            # Data API returns: market, asset (token_id), size (shares),
            # avgPrice, curPrice, value, pnl, etc.
            market_slug = pos.get("market", {}).get("slug", "") if isinstance(pos.get("market"), dict) else ""
            condition_id = pos.get("conditionId", pos.get("market", {}).get("conditionId", "")) if isinstance(pos.get("market"), dict) else pos.get("conditionId", "")
            token_id = pos.get("asset", pos.get("tokenId", ""))
            shares = float(pos.get("size", 0) or 0)
            avg_price = float(pos.get("avgPrice", 0) or 0)
            cur_price = float(pos.get("curPrice", 0) or 0)

            if shares <= 0 or not condition_id:
                continue

            amount_usd = round(shares * avg_price, 4)

            # Determine side (YES/NO) by matching token_id to market
            market_row = engine.query_one(
                "SELECT id, yes_token_id, no_token_id, question FROM markets WHERE id = ?",
                (condition_id,),
            )
            if not market_row:
                # Try to find by token ID
                market_row = engine.query_one(
                    "SELECT id, yes_token_id, no_token_id, question FROM markets "
                    "WHERE yes_token_id = ? OR no_token_id = ?",
                    (token_id, token_id),
                )
            if not market_row:
                continue

            side = "YES" if token_id == market_row.get("yes_token_id") else "NO"
            market_id = market_row["id"]
            question = market_row.get("question", "")

            # Check if we already have ANY trade for this market+side (active or closed)
            existing = engine.query_one(
                "SELECT id, price, result FROM trades WHERE market_id = ? AND side = ? "
                "AND status = 'executed' AND (result IS NULL OR result = 'open')",
                (market_id, side),
            )

            # Also check if market was recently traded (avoid re-creating after cashout)
            any_trade = engine.query_one(
                "SELECT id FROM trades WHERE market_id = ? AND status = 'executed'",
                (market_id,),
            )

            if existing:
                # Update price if missing
                if not existing.get("price") or existing["price"] <= 0:
                    engine.execute(
                        "UPDATE trades SET price = ?, amount_usd = ? WHERE id = ?",
                        (avg_price, amount_usd, existing["id"]),
                    )
                    synced += 1
                    logger.info(f"Position sync: updated price for trade {existing['id']} "
                                f"({side} {question[:40]}): ${avg_price:.4f}")
            elif any_trade:
                # Market was already traded — don't re-import (avoids cashout loop)
                logger.debug(f"Position sync: skipping {question[:40]} — already traded")
            else:
                # Truly new position — import it
                engine.execute(
                    """INSERT INTO trades
                       (market_id, market_question, side, amount_usd, price, status,
                        agent_id, user_cmd, created_at, executed_at)
                       VALUES (?, ?, ?, ?, ?, 'executed', 'sync', 'position_sync', ?, ?)""",
                    (market_id, question, side, amount_usd, avg_price,
                     datetime.utcnow().isoformat(), datetime.utcnow().isoformat()),
                )
                synced += 1
                logger.info(f"Position sync: imported {side} position in "
                            f"'{question[:40]}' (${amount_usd:.2f} @ {avg_price:.4f})")

        if synced > 0:
            logger.info(f"Position sync: {synced} positions synced from Data API")

    except Exception as e:
        logger.error(f"Position sync failed: {e}")


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
