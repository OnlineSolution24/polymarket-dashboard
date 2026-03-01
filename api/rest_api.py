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
        order_by: str = Query("volume", regex="^(volume|yes_price|last_updated)$"),
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
        status: Optional[str] = Query(None, regex="^(pending|approved|rejected|auto_approved)$"),
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
    # Config (read-only)
    # ------------------------------------------------------------------

    @app.get("/api/config", dependencies=[Depends(verify_api_key)])
    def get_config():
        """Return platform config (without secrets)."""
        cfg = load_platform_config()
        return cfg

    return app
