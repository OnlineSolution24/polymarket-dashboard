"""
FastAPI REST API for the Polymarket Trading Bot.
Exposes read endpoints for the monitoring dashboard and
write endpoints for emergency controls (pause/resume, suggestion responses).
"""

import json
import logging
from datetime import datetime, date
from typing import Optional

from fastapi import FastAPI, Depends, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from api.auth import verify_api_key
from config import AppConfig, load_platform_config
from db import engine

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Request / Response models
# ---------------------------------------------------------------------------

class SuggestionResponse(BaseModel):
    action: str  # "approve" or "reject"
    note: Optional[str] = None


class BotActionResponse(BaseModel):
    ok: bool
    message: str


class StrategyCreate(BaseModel):
    name: str
    description: str = ""
    hypothesis: str = ""
    entry_rules: list[dict] = []
    exit_rules: list[dict] = []
    trade_params: dict = {}
    category_filter: list[str] = []
    min_liquidity: float = 500
    category: str = ""
    discovered_by: str = "manual"


class StrategyUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    hypothesis: Optional[str] = None
    entry_rules: Optional[list[dict]] = None
    exit_rules: Optional[list[dict]] = None
    trade_params: Optional[dict] = None
    category_filter: Optional[list[str]] = None
    min_liquidity: Optional[float] = None
    category: Optional[str] = None


class StrategyStatusUpdate(BaseModel):
    status: str  # active, retired, rejected
    approved_by: str = "user"


class TradeRequest(BaseModel):
    market_id: str
    side: str  # YES or NO
    amount: float


class LogEventRequest(BaseModel):
    agent_id: str
    level: str = "info"
    message: str


# ---------------------------------------------------------------------------
# App factory
# ---------------------------------------------------------------------------

def create_app(config: AppConfig) -> FastAPI:
    app = FastAPI(
        title="Polymarket Trading Bot API",
        version="1.0.0",
        docs_url="/api/docs",
        redoc_url=None,
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Store config in app state
    app.state.config = config

    # ------------------------------------------------------------------
    # Health / Status
    # ------------------------------------------------------------------

    @app.get("/api/status", dependencies=[Depends(verify_api_key)])
    def get_status():
        from bot_main import bot_state
        platform_cfg = load_platform_config()
        trading_cfg = platform_cfg.get("trading", {})

        agent_count = engine.query_one(
            "SELECT COUNT(*) as cnt FROM agents WHERE status = 'active'"
        )
        today = date.today().isoformat()
        cost_row = engine.query_one(
            "SELECT COALESCE(SUM(cost_usd), 0) as total FROM api_costs WHERE date(created_at) = ?",
            (today,),
        )
        trade_row = engine.query_one(
            "SELECT COUNT(*) as cnt FROM trades WHERE date(created_at) = ?",
            (today,),
        )
        open_row = engine.query_one(
            "SELECT COUNT(*) as cnt FROM trades WHERE status = 'executed' AND (result = 'open' OR result IS NULL)"
        )
        pnl_row = engine.query_one(
            "SELECT COALESCE(SUM(pnl), 0) as total_pnl FROM trades WHERE date(executed_at) = ?",
            (today,),
        )
        pending_row = engine.query_one(
            "SELECT COUNT(*) as cnt FROM suggestions WHERE status = 'pending'"
        )
        monthly_cost_row = engine.query_one(
            "SELECT COALESCE(SUM(cost_usd), 0) as total FROM api_costs "
            "WHERE strftime('%Y-%m', created_at) = strftime('%Y-%m', 'now')"
        )
        cb = engine.query_one("SELECT * FROM circuit_breaker WHERE id = 1")

        return {
            "bot_paused": bot_state.paused,
            "trading_mode": trading_cfg.get("mode", "paper"),
            "active_agents": agent_count["cnt"] if agent_count else 0,
            "cost_today_usd": round(cost_row["total"], 4) if cost_row else 0,
            "cost_month_usd": round(monthly_cost_row["total"], 4) if monthly_cost_row else 0,
            "trades_today": trade_row["cnt"] if trade_row else 0,
            "open_positions": open_row["cnt"] if open_row else 0,
            "pnl_today": round(pnl_row["total_pnl"], 4) if pnl_row else 0,
            "pending_suggestions": pending_row["cnt"] if pending_row else 0,
            "circuit_breaker": {
                "consecutive_losses": cb["consecutive_losses"] if cb else 0,
                "paused_until": cb["paused_until"] if cb else None,
            },
            "timestamp": datetime.utcnow().isoformat(),
        }

    # ------------------------------------------------------------------
    # Markets
    # ------------------------------------------------------------------

    @app.get("/api/markets", dependencies=[Depends(verify_api_key)])
    def get_markets(
        limit: int = Query(50, ge=1, le=200),
        category: Optional[str] = None,
        order_by: str = Query("volume", pattern="^(volume|yes_price|last_updated)$"),
    ):
        sql = "SELECT * FROM markets"
        params = []
        if category:
            sql += " WHERE category = ?"
            params.append(category)
        sql += f" ORDER BY {order_by} DESC LIMIT ?"
        params.append(limit)
        return engine.query(sql, tuple(params))

    # ------------------------------------------------------------------
    # Trades
    # ------------------------------------------------------------------

    @app.get("/api/trades", dependencies=[Depends(verify_api_key)])
    def get_trades(
        limit: int = Query(50, ge=1, le=500),
        status: Optional[str] = None,
    ):
        sql = "SELECT * FROM trades"
        params = []
        if status:
            sql += " WHERE status = ?"
            params.append(status)
        sql += " ORDER BY created_at DESC LIMIT ?"
        params.append(limit)
        return engine.query(sql, tuple(params))

    @app.get("/api/trades/stats", dependencies=[Depends(verify_api_key)])
    def get_trade_stats():
        stats = engine.query_one("""
            SELECT COUNT(*) as total,
                   SUM(CASE WHEN result='win' THEN 1 ELSE 0 END) as wins,
                   SUM(CASE WHEN result='loss' THEN 1 ELSE 0 END) as losses,
                   COALESCE(SUM(pnl), 0) as total_pnl
            FROM trades WHERE result IS NOT NULL
        """)
        return stats or {"total": 0, "wins": 0, "losses": 0, "total_pnl": 0}

    # ------------------------------------------------------------------
    # Agents
    # ------------------------------------------------------------------

    @app.get("/api/agents", dependencies=[Depends(verify_api_key)])
    def get_agents():
        return engine.query("SELECT * FROM agents ORDER BY name")

    # ------------------------------------------------------------------
    # Logs
    # ------------------------------------------------------------------

    @app.get("/api/logs", dependencies=[Depends(verify_api_key)])
    def get_logs(
        agent_id: Optional[str] = None,
        level: Optional[str] = None,
        limit: int = Query(100, ge=1, le=1000),
    ):
        sql = "SELECT * FROM agent_logs"
        conditions = []
        params = []
        if agent_id:
            conditions.append("agent_id = ?")
            params.append(agent_id)
        if level:
            conditions.append("level = ?")
            params.append(level)
        if conditions:
            sql += " WHERE " + " AND ".join(conditions)
        sql += " ORDER BY created_at DESC LIMIT ?"
        params.append(limit)
        return engine.query(sql, tuple(params))

    # ------------------------------------------------------------------
    # Costs
    # ------------------------------------------------------------------

    @app.get("/api/costs", dependencies=[Depends(verify_api_key)])
    def get_costs(
        days: int = Query(7, ge=1, le=90),
    ):
        rows = engine.query(
            "SELECT * FROM api_costs WHERE created_at >= date('now', ?) ORDER BY created_at DESC",
            (f"-{days} days",),
        )
        # Summary
        today = date.today().isoformat()
        daily_row = engine.query_one(
            "SELECT COALESCE(SUM(cost_usd), 0) as total FROM api_costs WHERE date(created_at) = ?",
            (today,),
        )
        monthly_row = engine.query_one(
            "SELECT COALESCE(SUM(cost_usd), 0) as total FROM api_costs "
            "WHERE strftime('%Y-%m', created_at) = strftime('%Y-%m', 'now')"
        )
        # Provider breakdowns
        today_by_provider = engine.query(
            "SELECT provider, SUM(cost_usd) as total, SUM(tokens_in) as tokens_in, SUM(tokens_out) as tokens_out "
            "FROM api_costs WHERE date(created_at) = ? GROUP BY provider",
            (today,),
        )
        month_by_provider = engine.query(
            "SELECT provider, SUM(cost_usd) as total "
            "FROM api_costs WHERE strftime('%Y-%m', created_at) = strftime('%Y-%m', 'now') GROUP BY provider"
        )
        # Agent breakdown (today)
        today_by_agent = engine.query(
            "SELECT agent_id, SUM(cost_usd) as total FROM api_costs "
            "WHERE date(created_at) = ? AND agent_id IS NOT NULL GROUP BY agent_id ORDER BY total DESC",
            (today,),
        )
        return {
            "daily_total": round(daily_row["total"], 4) if daily_row else 0,
            "monthly_total": round(monthly_row["total"], 4) if monthly_row else 0,
            "today_by_provider": today_by_provider or [],
            "month_by_provider": month_by_provider or [],
            "today_by_agent": today_by_agent or [],
            "entries": rows,
        }

    # ------------------------------------------------------------------
    # Suggestions
    # ------------------------------------------------------------------

    @app.get("/api/suggestions", dependencies=[Depends(verify_api_key)])
    def get_suggestions(
        status: Optional[str] = Query(None, pattern="^(pending|approved|rejected|auto_approved)$"),
        limit: int = Query(50, ge=1, le=200),
    ):
        sql = "SELECT * FROM suggestions"
        params = []
        if status:
            sql += " WHERE status = ?"
            params.append(status)
        sql += " ORDER BY created_at DESC LIMIT ?"
        params.append(limit)
        return engine.query(sql, tuple(params))

    @app.post("/api/suggestions/{suggestion_id}/respond", dependencies=[Depends(verify_api_key)])
    def respond_to_suggestion(suggestion_id: int, body: SuggestionResponse):
        row = engine.query_one("SELECT * FROM suggestions WHERE id = ?", (suggestion_id,))
        if not row:
            raise HTTPException(status_code=404, detail="Suggestion not found")
        if row["status"] != "pending":
            raise HTTPException(status_code=400, detail=f"Suggestion already {row['status']}")

        new_status = "approved" if body.action == "approve" else "rejected"
        engine.execute(
            "UPDATE suggestions SET status = ?, user_response = ?, resolved_at = ? WHERE id = ?",
            (new_status, body.note, datetime.utcnow().isoformat(), suggestion_id),
        )
        logger.info(f"Suggestion {suggestion_id} {new_status}")
        return BotActionResponse(ok=True, message=f"Suggestion {new_status}")

    @app.post("/api/suggestions/cleanup-pending", dependencies=[Depends(verify_api_key)])
    def cleanup_pending_suggestions():
        """Bulk-close all pending suggestions."""
        before = engine.query_one(
            "SELECT COUNT(*) as cnt FROM suggestions WHERE status = 'pending'"
        )
        pending_before = before["cnt"] if before else 0

        engine.execute(
            "UPDATE suggestions SET status = 'rejected', user_response = ?, resolved_at = ? "
            "WHERE status = 'pending'",
            ("Bulk cleanup via API", datetime.utcnow().isoformat()),
        )

        after = engine.query_one(
            "SELECT COUNT(*) as cnt FROM suggestions WHERE status = 'pending'"
        )
        pending_after = after["cnt"] if after else 0
        closed = pending_before - pending_after
        logger.info(f"Pending suggestions cleanup: closed={closed}")
        return {"ok": True, "closed": closed, "pending_after": pending_after}

    # ------------------------------------------------------------------
    # Circuit Breaker
    # ------------------------------------------------------------------

    @app.get("/api/circuit-breaker", dependencies=[Depends(verify_api_key)])
    def get_circuit_breaker():
        cb = engine.query_one("SELECT * FROM circuit_breaker WHERE id = 1")
        return cb or {"id": 1, "consecutive_losses": 0, "paused_until": None}

    @app.post("/api/circuit-breaker/reset", dependencies=[Depends(verify_api_key)])
    def reset_circuit_breaker():
        engine.execute(
            "UPDATE circuit_breaker SET consecutive_losses = 0, paused_until = NULL, last_updated = ? WHERE id = 1",
            (datetime.utcnow().isoformat(),),
        )
        # Send alert
        try:
            from services.telegram_alerts import get_alerts
            alerts = get_alerts(config)
            alerts.send("🔄 <b>Circuit Breaker zurückgesetzt</b> (via Dashboard)")
        except Exception:
            pass
        return BotActionResponse(ok=True, message="Circuit breaker reset")

    # ------------------------------------------------------------------
    # Bot Control
    # ------------------------------------------------------------------

    @app.post("/api/bot/pause", dependencies=[Depends(verify_api_key)])
    def pause_bot():
        from bot_main import bot_state
        bot_state.paused = True
        logger.warning("Bot PAUSED via API")
        try:
            from services.telegram_alerts import get_alerts
            alerts = get_alerts(config)
            alerts.send("⏸️ <b>Bot pausiert</b> (via Dashboard)")
        except Exception:
            pass
        return BotActionResponse(ok=True, message="Bot paused")

    @app.post("/api/bot/resume", dependencies=[Depends(verify_api_key)])
    def resume_bot():
        from bot_main import bot_state
        bot_state.paused = False
        logger.info("Bot RESUMED via API")
        try:
            from services.telegram_alerts import get_alerts
            alerts = get_alerts(config)
            alerts.send("▶️ <b>Bot fortgesetzt</b> (via Dashboard)")
        except Exception:
            pass
        return BotActionResponse(ok=True, message="Bot resumed")

    # ------------------------------------------------------------------
    # Config
    # ------------------------------------------------------------------

    @app.get("/api/config", dependencies=[Depends(verify_api_key)])
    def get_config():
        """Return platform config (without secrets)."""
        cfg = load_platform_config()
        return cfg

    @app.post("/api/config", dependencies=[Depends(verify_api_key)])
    def save_config(body: dict):
        """Save platform config. Scheduler picks up changes on next cycle."""
        from config import save_platform_config
        save_platform_config(body)
        logger.info("Platform config updated via API")
        return BotActionResponse(ok=True, message="Config saved")

    @app.post("/api/scheduler/reload", dependencies=[Depends(verify_api_key)])
    def reload_scheduler():
        """Stop and restart the scheduler with current config."""
        try:
            from services.scheduler import _scheduler, start_scheduler, _lock
            import services.scheduler as sched_module

            with _lock:
                if _scheduler is not None:
                    _scheduler.shutdown(wait=False)
                    sched_module._scheduler = None
                    logger.info("Scheduler stopped for reload")

            start_scheduler(config)
            logger.info("Scheduler reloaded with new config")

            try:
                from services.telegram_alerts import get_alerts
                alerts = get_alerts(config)
                alerts.send("🔄 <b>Scheduler neu geladen</b> (via Dashboard)")
            except Exception:
                pass

            return BotActionResponse(ok=True, message="Scheduler reloaded")
        except Exception as e:
            logger.error(f"Scheduler reload failed: {e}")
            raise HTTPException(status_code=500, detail=str(e))

    # ------------------------------------------------------------------
    # Strategies
    # ------------------------------------------------------------------

    @app.get("/api/strategies", dependencies=[Depends(verify_api_key)])
    def get_strategies(
        status: Optional[str] = None,
        limit: int = Query(50, ge=1, le=200),
    ):
        sql = "SELECT * FROM strategies"
        params = []
        if status:
            sql += " WHERE status = ?"
            params.append(status)
        sql += " ORDER BY confidence_score DESC, created_at DESC LIMIT ?"
        params.append(limit)
        rows = engine.query(sql, tuple(params))
        # Parse definition JSON for each row
        for row in (rows or []):
            try:
                row["definition_parsed"] = json.loads(row.get("definition") or "{}")
            except (json.JSONDecodeError, TypeError):
                row["definition_parsed"] = {}
        return rows or []

    @app.get("/api/strategies/{strategy_id}", dependencies=[Depends(verify_api_key)])
    def get_strategy_detail(strategy_id: str):
        row = engine.query_one("SELECT * FROM strategies WHERE id = ?", (strategy_id,))
        if not row:
            raise HTTPException(status_code=404, detail="Strategy not found")
        try:
            row["definition_parsed"] = json.loads(row.get("definition") or "{}")
            row["backtest_results_parsed"] = json.loads(row.get("backtest_results") or "{}")
        except (json.JSONDecodeError, TypeError):
            pass
        # Include linked trades
        row["strategy_trades"] = engine.query(
            "SELECT * FROM strategy_trades WHERE strategy_id = ? ORDER BY created_at DESC LIMIT 50",
            (strategy_id,),
        ) or []
        return row

    @app.post("/api/strategies", dependencies=[Depends(verify_api_key)])
    def create_strategy(body: StrategyCreate):
        import uuid
        strategy_id = f"strat_{uuid.uuid4().hex[:8]}"
        definition = {
            "name": body.name,
            "description": body.description,
            "hypothesis": body.hypothesis,
            "entry_rules": body.entry_rules,
            "exit_rules": body.exit_rules,
            "trade_params": body.trade_params,
            "category_filter": body.category_filter,
            "min_liquidity": body.min_liquidity,
        }
        engine.execute(
            """INSERT INTO strategies (id, name, description, definition, status, category, discovered_by, created_at, updated_at)
               VALUES (?, ?, ?, ?, 'pending_backtest', ?, ?, ?, ?)""",
            (strategy_id, body.name, body.description, json.dumps(definition),
             body.category, body.discovered_by,
             datetime.utcnow().isoformat(), datetime.utcnow().isoformat()),
        )
        logger.info(f"Strategy created: {strategy_id} ({body.name})")
        return {"ok": True, "strategy_id": strategy_id}

    @app.put("/api/strategies/{strategy_id}", dependencies=[Depends(verify_api_key)])
    def update_strategy(strategy_id: str, body: StrategyUpdate):
        """Update strategy definition, rules, and metadata."""
        row = engine.query_one("SELECT id, definition FROM strategies WHERE id = ?", (strategy_id,))
        if not row:
            raise HTTPException(status_code=404, detail="Strategy not found")

        # Merge with existing definition
        try:
            definition = json.loads(row.get("definition") or "{}")
        except (json.JSONDecodeError, TypeError):
            definition = {}

        if body.entry_rules is not None:
            definition["entry_rules"] = body.entry_rules
        if body.exit_rules is not None:
            definition["exit_rules"] = body.exit_rules
        if body.trade_params is not None:
            definition["trade_params"] = body.trade_params
        if body.category_filter is not None:
            definition["category_filter"] = body.category_filter
        if body.min_liquidity is not None:
            definition["min_liquidity"] = body.min_liquidity
        if body.hypothesis is not None:
            definition["hypothesis"] = body.hypothesis
        if body.name is not None:
            definition["name"] = body.name
        if body.description is not None:
            definition["description"] = body.description

        # Update DB
        updates = ["definition = ?", "updated_at = ?"]
        params = [json.dumps(definition), datetime.utcnow().isoformat()]

        if body.name is not None:
            updates.append("name = ?")
            params.append(body.name)
        if body.description is not None:
            updates.append("description = ?")
            params.append(body.description)
        if body.category is not None:
            updates.append("category = ?")
            params.append(body.category)

        params.append(strategy_id)
        engine.execute(f"UPDATE strategies SET {', '.join(updates)} WHERE id = ?", tuple(params))
        logger.info(f"Strategy {strategy_id} updated")
        return BotActionResponse(ok=True, message=f"Strategy {strategy_id} updated")

    @app.put("/api/strategies/{strategy_id}/status", dependencies=[Depends(verify_api_key)])
    def update_strategy_status(strategy_id: str, body: StrategyStatusUpdate):
        row = engine.query_one("SELECT id FROM strategies WHERE id = ?", (strategy_id,))
        if not row:
            raise HTTPException(status_code=404, detail="Strategy not found")
        valid = {"active", "retired", "rejected", "validated", "pending_backtest"}
        if body.status not in valid:
            raise HTTPException(status_code=400, detail=f"Invalid status. Must be one of: {valid}")
        updates = "status = ?, approved_by = ?, updated_at = ?"
        params = [body.status, body.approved_by, datetime.utcnow().isoformat()]
        if body.status == "retired":
            updates += ", retired_at = ?"
            params.append(datetime.utcnow().isoformat())
        params.append(strategy_id)
        engine.execute(f"UPDATE strategies SET {updates} WHERE id = ?", tuple(params))
        logger.info(f"Strategy {strategy_id} → {body.status} (by {body.approved_by})")
        return BotActionResponse(ok=True, message=f"Strategy status updated to {body.status}")

    @app.delete("/api/strategies/{strategy_id}", dependencies=[Depends(verify_api_key)])
    def delete_strategy(strategy_id: str):
        engine.execute("DELETE FROM strategy_trades WHERE strategy_id = ?", (strategy_id,))
        engine.execute("DELETE FROM strategies WHERE id = ?", (strategy_id,))
        return BotActionResponse(ok=True, message="Strategy deleted")

    # ------------------------------------------------------------------
    # Backtesting
    # ------------------------------------------------------------------

    @app.post("/api/backtest/{strategy_id}", dependencies=[Depends(verify_api_key)])
    def run_backtest_endpoint(strategy_id: str):
        from services.backtest_service import run_strategy_backtest
        result = run_strategy_backtest(strategy_id)
        if not result.get("ok"):
            raise HTTPException(status_code=400, detail=result.get("error", "Backtest failed"))
        return result

    @app.get("/api/backtest/{strategy_id}/results", dependencies=[Depends(verify_api_key)])
    def get_backtest_results(strategy_id: str):
        row = engine.query_one(
            "SELECT backtest_results, backtest_pnl, backtest_win_rate, backtest_sharpe, "
            "backtest_max_dd, backtest_trades, confidence_score FROM strategies WHERE id = ?",
            (strategy_id,),
        )
        if not row:
            raise HTTPException(status_code=404, detail="Strategy not found")
        try:
            row["backtest_results_parsed"] = json.loads(row.get("backtest_results") or "{}")
        except (json.JSONDecodeError, TypeError):
            row["backtest_results_parsed"] = {}
        return row

    # ------------------------------------------------------------------
    # Analytics
    # ------------------------------------------------------------------

    @app.get("/api/analytics/patterns", dependencies=[Depends(verify_api_key)])
    def get_pattern_analysis():
        from services.backtest_service import get_pattern_analysis
        return get_pattern_analysis()

    @app.get("/api/analytics/strategy-signals/{strategy_id}", dependencies=[Depends(verify_api_key)])
    def get_strategy_signals(strategy_id: str):
        row = engine.query_one("SELECT definition FROM strategies WHERE id = ?", (strategy_id,))
        if not row:
            raise HTTPException(status_code=404, detail="Strategy not found")
        try:
            definition = json.loads(row["definition"])
        except (json.JSONDecodeError, TypeError):
            return []
        from services.strategy_evaluator import find_matching_markets, compute_trade_params
        platform_cfg = load_platform_config()
        capital = platform_cfg.get("trading", {}).get("capital_usd", 100)
        trade_params = definition.get("trade_params", {})
        matches = find_matching_markets(definition)
        signals = []
        for market in matches:
            params = compute_trade_params(market, trade_params, capital)
            if params:
                params["strategy_id"] = strategy_id
                params["strategy_name"] = definition.get("name", "")
                signals.append(params)
        return signals

    # ------------------------------------------------------------------
    # Market Snapshots
    # ------------------------------------------------------------------

    @app.get("/api/snapshots/{market_id}", dependencies=[Depends(verify_api_key)])
    def get_market_snapshots(
        market_id: str,
        hours: int = Query(48, ge=1, le=720),
    ):
        rows = engine.query(
            "SELECT * FROM market_snapshots WHERE market_id = ? AND snapshot_at >= datetime('now', ?) "
            "ORDER BY snapshot_at DESC",
            (market_id, f"-{hours} hours"),
        )
        return rows or []

    # ------------------------------------------------------------------
    # Trade Execution (for MCP / OpenClaw agents)
    # ------------------------------------------------------------------

    @app.post("/api/trades/execute", dependencies=[Depends(verify_api_key)])
    def execute_trade(body: TradeRequest):
        """Execute a real trade (goes through all safety checks)."""
        platform_cfg = load_platform_config()
        trading_cfg = platform_cfg.get("trading", {})
        mode = trading_cfg.get("mode", "paper")

        if mode == "paper":
            return _simulate_trade_internal(body, trading_cfg)

        # Real trading requires authenticated credentials.
        if not config.polymarket_private_key:
            return {
                "ok": False,
                "status": "failed",
                "error": "POLYMARKET_PRIVATE_KEY is missing; real trade execution disabled.",
            }

        # Validate via risk checks
        risk_result = _check_risk_internal(body, trading_cfg)
        if not risk_result["ok"]:
            return risk_result

        # Resolve requested market_id + side to concrete CLOB token id.
        # Backward-compatible: if market row not found, treat market_id as token_id.
        market_row = engine.query_one(
            "SELECT id, question, yes_token_id, no_token_id FROM markets WHERE id = ?",
            (body.market_id,),
        )
        token_id = body.market_id
        market_id = body.market_id
        market_question = None
        if market_row:
            market_id = market_row["id"]
            market_question = market_row.get("question")
            side_upper = (body.side or "").upper()
            token_id = market_row.get("yes_token_id") if side_upper == "YES" else market_row.get("no_token_id")
            if not token_id:
                return {
                    "ok": False,
                    "status": "failed",
                    "error": f"No token_id for side={side_upper} on market={market_id}",
                }

        # Execute
        try:
            from services.polymarket_client import PolymarketService
            service = PolymarketService(config)
            result = service.place_market_order(
                token_id=token_id, amount=body.amount, side=body.side,
            )
            status = "executed" if result.get("ok") else "failed"
        except Exception as e:
            logger.error(f"Trade execution failed: {e}")
            status = "failed"

        # Log trade
        engine.execute(
            """INSERT INTO trades (market_id, market_question, side, amount_usd, status, agent_id, created_at, executed_at)
               VALUES (?, ?, ?, ?, ?, 'openclaw_trader', ?, ?)""",
            (market_id, market_question, body.side, body.amount, status,
             datetime.utcnow().isoformat(), datetime.utcnow().isoformat()),
        )
        return {"ok": status == "executed", "status": status}

    @app.post("/api/trades/simulate", dependencies=[Depends(verify_api_key)])
    def simulate_trade(body: TradeRequest):
        """Paper trade simulation."""
        platform_cfg = load_platform_config()
        trading_cfg = platform_cfg.get("trading", {})
        return _simulate_trade_internal(body, trading_cfg)

    @app.post("/api/trades/check-risk", dependencies=[Depends(verify_api_key)])
    def check_risk(body: TradeRequest):
        """Run risk validation without executing."""
        platform_cfg = load_platform_config()
        trading_cfg = platform_cfg.get("trading", {})
        return _check_risk_internal(body, trading_cfg)

    def _simulate_trade_internal(body: TradeRequest, trading_cfg: dict) -> dict:
        risk = _check_risk_internal(body, trading_cfg)
        engine.execute(
            """INSERT INTO trades (market_id, side, amount_usd, status, agent_id, created_at, executed_at)
               VALUES (?, ?, ?, 'paper', 'openclaw_trader', ?, ?)""",
            (body.market_id, body.side, body.amount,
             datetime.utcnow().isoformat(), datetime.utcnow().isoformat()),
        )
        return {"ok": True, "status": "paper", "risk_check": risk}

    def _check_risk_internal(body: TradeRequest, trading_cfg: dict) -> dict:
        limits = trading_cfg.get("limits", {})
        issues = []

        # Circuit breaker
        cb = engine.query_one("SELECT * FROM circuit_breaker WHERE id = 1")
        if cb and cb.get("paused_until"):
            try:
                if datetime.utcnow() < datetime.fromisoformat(cb["paused_until"]):
                    issues.append(f"Circuit breaker active until {cb['paused_until']}")
            except (ValueError, TypeError):
                pass

        # Daily loss
        max_daily = limits.get("max_daily_loss_usd", 50)
        loss_row = engine.query_one(
            "SELECT COALESCE(SUM(pnl), 0) as total FROM trades "
            "WHERE date(executed_at) = date('now') AND pnl < 0"
        )
        if loss_row and abs(loss_row["total"]) >= max_daily:
            issues.append(f"Daily loss limit reached (${abs(loss_row['total']):.2f} / ${max_daily:.2f})")

        # Position size
        capital = trading_cfg.get("capital_usd", 100)
        max_pct = limits.get("max_position_pct", 5) / 100
        if body.amount > capital * max_pct:
            issues.append(f"Position too large (${body.amount:.2f} > max ${capital * max_pct:.2f})")

        # Category blacklist
        blacklist = trading_cfg.get("category_blacklist", [])
        if blacklist:
            market = engine.query_one("SELECT category FROM markets WHERE id = ?", (body.market_id,))
            if market and market.get("category") in blacklist:
                issues.append(f"Category '{market['category']}' is blacklisted")

        return {"ok": len(issues) == 0, "issues": issues}

    # ------------------------------------------------------------------
    # Monitor / Health
    # ------------------------------------------------------------------

    @app.get("/api/monitor/health", dependencies=[Depends(verify_api_key)])
    def monitor_health():
        error_count = engine.query_one(
            "SELECT COUNT(*) as cnt FROM agent_logs WHERE level = 'error' "
            "AND created_at > datetime('now', '-24 hours')"
        )
        last_log = engine.query_one(
            "SELECT MAX(created_at) as ts FROM agent_logs"
        )
        tables = engine.query(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        )
        return {
            "status": "healthy",
            "timestamp": datetime.utcnow().isoformat(),
            "errors_24h": error_count["cnt"] if error_count else 0,
            "last_activity": last_log["ts"] if last_log else None,
            "tables": [t["name"] for t in (tables or [])],
            "db_accessible": True,
        }

    @app.get("/api/monitor/errors", dependencies=[Depends(verify_api_key)])
    def monitor_errors(
        hours: int = Query(24, ge=1, le=168),
        limit: int = Query(50, ge=1, le=500),
    ):
        return engine.query(
            "SELECT * FROM agent_logs WHERE level = 'error' "
            "AND created_at > datetime('now', ?) ORDER BY created_at DESC LIMIT ?",
            (f"-{hours} hours", limit),
        ) or []

    # ------------------------------------------------------------------
    # Logging (for OpenClaw agents via MCP)
    # ------------------------------------------------------------------

    @app.post("/api/logs", dependencies=[Depends(verify_api_key)])
    def create_log_event(body: LogEventRequest):
        engine.execute(
            "INSERT INTO agent_logs (agent_id, level, message, created_at) VALUES (?, ?, ?, ?)",
            (body.agent_id, body.level, body.message, datetime.utcnow().isoformat()),
        )
        return BotActionResponse(ok=True, message="Log event created")

    # ------------------------------------------------------------------
    # Self-Modification (code change proposals by AI agents)
    # ------------------------------------------------------------------

    from api.self_modify import register_self_modify_endpoints
    register_self_modify_endpoints(app)

    return app
