"""
FastAPI REST API for the Polymarket Trading Bot.
Exposes read endpoints for the monitoring dashboard and
write endpoints for emergency controls (pause/resume, suggestion responses).
"""

import json
import logging
from datetime import datetime, date, timedelta
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


class CashoutRequest(BaseModel):
    trade_id: int


class ImportPositionRequest(BaseModel):
    market_id: str
    title: str
    outcome: str = "YES"
    avg_price: float = 0
    cost: float = 0
    shares: float = 0


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

    @app.get("/api/trades/positions", dependencies=[Depends(verify_api_key)])
    def get_open_positions():
        """Open positions with live market prices and unrealized PnL."""
        positions = engine.query("""
            SELECT t.id, t.market_id, t.market_question, t.side, t.amount_usd,
                   t.price as entry_price, t.executed_at,
                   m.yes_price, m.no_price
            FROM trades t
            LEFT JOIN markets m ON t.market_id = m.id
            WHERE t.status = 'executed'
              AND (t.result IS NULL OR t.result = 'open')
              AND t.amount_usd > 0
            ORDER BY t.executed_at DESC
        """)
        result = []
        for p in positions:
            entry = p.get("entry_price") or 0
            current = p.get("yes_price") if p.get("side") == "YES" else p.get("no_price")
            current = current or 0
            shares = (p["amount_usd"] / entry) if entry > 0 else 0
            current_value = shares * current
            cost_basis = p["amount_usd"]
            unrealized_pnl = current_value - cost_basis
            pnl_pct = ((current - entry) / entry * 100) if entry > 0 else 0
            result.append({
                "id": p["id"],
                "market_question": p.get("market_question", ""),
                "side": p["side"],
                "entry_price": round(entry, 4),
                "current_price": round(current, 4),
                "shares": round(shares, 1),
                "cost_basis": round(cost_basis, 2),
                "current_value": round(current_value, 2),
                "unrealized_pnl": round(unrealized_pnl, 2),
                "pnl_pct": round(pnl_pct, 1),
                "executed_at": p.get("executed_at"),
            })
        return result

    @app.get("/api/trades/closed", dependencies=[Depends(verify_api_key)])
    def get_closed_trades():
        """Closed trades with realized PnL (profit only, no cost basis)."""
        return engine.query("""
            SELECT id, market_question, side, amount_usd, price as entry_price,
                   result, pnl as realized_pnl, user_cmd, executed_at
            FROM trades
            WHERE result IN ('win', 'loss', 'cashout', 'hedge', 'settled')
              AND amount_usd > 0
            ORDER BY executed_at DESC
            LIMIT 100
        """)

    @app.get("/api/trades/performance", dependencies=[Depends(verify_api_key)])
    def get_performance():
        """Portfolio performance using real Polymarket Data API values.

        Fetches live positions from Polymarket (cashPnl, realizedPnl per position).
        Falls back to latest portfolio_snapshot if live fetch fails.
        Win/Loss counted per market: only fully closed markets count.
        """
        import httpx

        platform_cfg = load_platform_config()
        trading_cfg = platform_cfg.get("trading", {})

        # Read total_deposited from DB (persistent), fallback to config
        dep_row = engine.query_one("SELECT value FROM settings WHERE key = 'total_deposited'")
        total_deposited = float(dep_row["value"]) if dep_row else trading_cfg.get("total_deposited", 0)

        # ---- Real USDC balance from Polygon blockchain ----
        funder = config.polymarket_funder if hasattr(config, 'polymarket_funder') else ""
        real_cash_balance = None
        if funder:
            try:
                usdc_contract = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
                padded_addr = funder[2:].lower().zfill(64)
                calldata = "0x70a08231" + padded_addr
                rpc_resp = httpx.post("https://polygon.drpc.org", json={
                    "jsonrpc": "2.0", "id": 1, "method": "eth_call",
                    "params": [{"to": usdc_contract, "data": calldata}, "latest"],
                }, timeout=10)
                rpc_data = rpc_resp.json()
                if "result" in rpc_data and not rpc_data.get("error"):
                    real_cash_balance = int(rpc_data["result"], 16) / 1e6
            except Exception:
                pass

        # ---- Live data from Polymarket Data API ----
        positions_data = []
        if funder:
            try:
                resp = httpx.get(
                    "https://data-api.polymarket.com/positions",
                    params={"user": funder},
                    timeout=15,
                )
                resp.raise_for_status()
                positions_data = resp.json()
            except Exception:
                pass

        if positions_data:
            total_value = sum(float(p.get("currentValue", 0) or 0) for p in positions_data)
            total_cost = sum(float(p.get("initialValue", 0) or 0) for p in positions_data)
            unrealized_pnl = sum(float(p.get("cashPnl", 0) or 0) for p in positions_data)
            total_realized = sum(float(p.get("realizedPnl", 0) or 0) for p in positions_data)

            # Per-position details for dashboard
            live_positions = []
            for p in positions_data:
                condition_id = p.get("conditionId", p.get("condition_id", ""))
                # Find matching DB trade for manual cashout
                db_trade = engine.query_one(
                    "SELECT id FROM trades WHERE market_id = ? AND status = 'executed' "
                    "AND (result IS NULL OR result = 'open') ORDER BY created_at LIMIT 1",
                    (condition_id,),
                ) if condition_id else None
                shares = float(p.get("size", 0) or 0)
                redeemable = bool(p.get("redeemable", False))
                # Skip dust positions
                if shares < 0.01:
                    continue
                live_positions.append({
                    "title": p.get("title", "?")[:60],
                    "outcome": p.get("outcome", "?"),
                    "shares": shares,
                    "avg_price": float(p.get("avgPrice", 0) or 0),
                    "cur_price": float(p.get("curPrice", 0) or 0),
                    "cost": round(float(p.get("initialValue", 0) or 0), 2),
                    "value": round(float(p.get("currentValue", 0) or 0), 2),
                    "pnl": round(float(p.get("cashPnl", 0) or 0), 2),
                    "pnl_pct": round(float(p.get("percentPnl", 0) or 0), 1),
                    "realized_pnl": round(float(p.get("realizedPnl", 0) or 0), 2),
                    "trade_id": db_trade["id"] if db_trade else None,
                    "market_id": condition_id,
                    "redeemable": redeemable,
                })
        else:
            # Fallback: latest portfolio snapshot
            snap = engine.query_one(
                "SELECT * FROM portfolio_snapshots ORDER BY snapshot_at DESC LIMIT 1"
            )
            if snap:
                total_value = snap.get("positions_value", 0)
                total_cost = snap.get("positions_cost", 0)
                unrealized_pnl = snap.get("unrealized_pnl", 0)
                total_realized = snap.get("realized_pnl", 0)
            else:
                total_value = total_cost = unrealized_pnl = total_realized = 0
            live_positions = []

        # ---- Equity curve from snapshots ----
        equity_curve = engine.query("""
            SELECT snapshot_at, positions_value, positions_cost,
                   unrealized_pnl, realized_pnl
            FROM portfolio_snapshots
            ORDER BY snapshot_at
        """) or []

        # ---- Per-market Win/Loss (DB-based, only fully closed) ----
        market_stats = engine.query("""
            SELECT market_id,
                   MIN(CASE WHEN amount_usd > 0 THEN market_question END) as name,
                   SUM(CASE WHEN status = 'executed' AND (result IS NULL OR result = 'open') THEN 1 ELSE 0 END) as open_count,
                   COUNT(*) as trade_count
            FROM trades
            WHERE status IN ('executed', 'closed')
            GROUP BY market_id
        """)

        wins = 0
        losses = 0
        closed_markets = []
        open_market_count = 0
        for m in (market_stats or []):
            market_name = m.get("name") or m.get("market_id", "?")
            has_open = (m.get("open_count") or 0) > 0

            # Check for real settlement (market resolved on-chain)
            settled_row = engine.query_one(
                "SELECT COUNT(*) as cnt FROM trades WHERE market_id = ? "
                "AND result IN ('win', 'loss', 'settled', 'settlement_win', 'stop_loss', 'take_profit', 'penny_cleanup')",
                (m["market_id"],),
            )
            has_settlement = (settled_row.get("cnt", 0) if settled_row else 0) > 0

            if has_open:
                # Still has open positions
                open_market_count += 1
            elif has_settlement:
                # Market resolved on-chain
                win_row = engine.query_one(
                    "SELECT COUNT(*) as cnt FROM trades WHERE market_id = ? "
                    "AND result IN ('win', 'settled', 'settlement_win', 'take_profit')",
                    (m["market_id"],),
                )
                is_win = (win_row.get("cnt", 0) if win_row else 0) > 0
                if is_win:
                    wins += 1
                else:
                    losses += 1
                settle_pnl_row = engine.query_one(
                    "SELECT COALESCE(SUM(pnl), 0) as total_pnl FROM trades WHERE market_id = ? "
                    "AND result IS NOT NULL",
                    (m["market_id"],),
                )
                settle_pnl = float(settle_pnl_row["total_pnl"]) if settle_pnl_row else 0
                # Get detailed trade data for display
                detail = engine.query_one(
                    "SELECT AVG(price) as avg_entry, SUM(amount_usd) as total_cost, "
                    "MAX(executed_at) as closed_at "
                    "FROM trades WHERE market_id = ? AND status IN ('executed', 'closed')",
                    (m["market_id"],),
                )
                avg_entry = float(detail["avg_entry"]) if detail and detail.get("avg_entry") else 0
                total_cost_d = float(detail["total_cost"]) if detail and detail.get("total_cost") else 0
                closed_at = detail.get("closed_at", "") if detail else ""
                pnl_pct = (settle_pnl / total_cost_d * 100) if total_cost_d > 0 else 0
                closed_markets.append({
                    "market_id": m["market_id"],
                    "name": market_name[:60],
                    "result": "win" if is_win else "loss",
                    "pnl": round(settle_pnl, 2),
                    "pnl_pct": round(pnl_pct, 1),
                    "avg_entry": round(avg_entry, 4),
                    "total_cost": round(total_cost_d, 2),
                    "trade_count": m.get("trade_count", 0),
                    "closed_at": str(closed_at)[:16] if closed_at else "",
                })
            else:
                # All trades closed (cashout/loss/phantom) — count by total PnL
                pnl_row = engine.query_one(
                    "SELECT COALESCE(SUM(pnl), 0) as total_pnl FROM trades WHERE market_id = ? "
                    "AND result IN ('cashout', 'loss', 'phantom', 'stop_loss', 'settlement_win', 'take_profit', 'penny_cleanup')",
                    (m["market_id"],),
                )
                total_pnl = float(pnl_row["total_pnl"]) if pnl_row else 0
                is_win = total_pnl > 0
                if is_win:
                    wins += 1
                else:
                    losses += 1
                detail2 = engine.query_one(
                    "SELECT AVG(price) as avg_entry, SUM(amount_usd) as total_cost, "
                    "MAX(executed_at) as closed_at "
                    "FROM trades WHERE market_id = ?",
                    (m["market_id"],),
                )
                avg_entry2 = float(detail2["avg_entry"]) if detail2 and detail2.get("avg_entry") else 0
                total_cost2 = float(detail2["total_cost"]) if detail2 and detail2.get("total_cost") else 0
                closed_at2 = detail2.get("closed_at", "") if detail2 else ""
                pnl_pct2 = (total_pnl / total_cost2 * 100) if total_cost2 > 0 else 0
                closed_markets.append({
                    "market_id": m["market_id"],
                    "name": market_name[:60],
                    "result": "win" if is_win else "loss",
                    "pnl": round(total_pnl, 2),
                    "pnl_pct": round(pnl_pct2, 1),
                    "avg_entry": round(avg_entry2, 4),
                    "total_cost": round(total_cost2, 2),
                    "trade_count": m.get("trade_count", 0),
                    "closed_at": str(closed_at2)[:16] if closed_at2 else "",
                })

        return {
            "total_deposited": total_deposited,
            "cash_balance": round(real_cash_balance, 2) if real_cash_balance is not None else None,
            "positions_value": round(total_value, 2),
            "positions_cost": round(total_cost, 2),
            "unrealized_pnl": round(unrealized_pnl, 2),
            "realized_pnl": round(total_realized, 2),
            "live_positions": live_positions,
            "equity_curve": equity_curve,
            "closed_markets": closed_markets,
            "open_market_count": open_market_count,
            "total_markets": len(closed_markets) + open_market_count,
            "wins": wins,
            "losses": losses,
        }

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
        # Hourly breakdown (last 24h)
        hourly = engine.query(
            "SELECT strftime('%Y-%m-%d %H:00', created_at) as hour, "
            "provider, COUNT(*) as calls, SUM(cost_usd) as total, "
            "SUM(tokens_in) as tokens_in, SUM(tokens_out) as tokens_out "
            "FROM api_costs WHERE created_at >= datetime('now', '-24 hours') "
            "GROUP BY hour, provider ORDER BY hour DESC"
        )
        return {
            "daily_total": round(daily_row["total"], 4) if daily_row else 0,
            "monthly_total": round(monthly_row["total"], 4) if monthly_row else 0,
            "today_by_provider": today_by_provider or [],
            "month_by_provider": month_by_provider or [],
            "today_by_agent": today_by_agent or [],
            "hourly": hourly or [],
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

    @app.post("/api/settings/{key}", dependencies=[Depends(verify_api_key)])
    def save_setting(key: str, body: dict):
        """Save a persistent setting to DB (survives restarts)."""
        value = str(body.get("value", ""))
        engine.execute(
            "INSERT OR REPLACE INTO settings (key, value, updated_at) VALUES (?, ?, datetime('now'))",
            (key, value),
        )
        return BotActionResponse(ok=True, message=f"Setting '{key}' saved")

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

    @app.get("/api/strategies/{strategy_id}/live-stats", dependencies=[Depends(verify_api_key)])
    def get_strategy_live_stats(strategy_id: str):
        """Compute live trading stats for a strategy from actual trades."""
        row = engine.query_one("SELECT definition, category FROM strategies WHERE id = ?", (strategy_id,))
        if not row:
            raise HTTPException(status_code=404, detail="Strategy not found")

        try:
            defn = json.loads(row.get("definition") or "{}")
        except (json.JSONDecodeError, TypeError):
            defn = {}

        category_filter = defn.get("category_filter", [])
        trade_side = defn.get("trade_params", {}).get("side")

        # Build category matching conditions from market_question text
        _CAT_KEYWORDS = {
            "Weather": ['temperature', '°F', '°C', 'weather', 'high of', 'low of', 'hottest', 'coldest', 'rainfall', 'snow', 'hurricane'],
            "Sports": ['win the', 'vs.', 'defeat', 'nba', 'nfl', 'nhl', 'mlb', 'premier league', 'champions league', 'super bowl', 'ufc', 'boxing'],
            "Politics": ['trump', 'biden', 'election', 'president', 'senate', 'congress', 'democrat', 'republican', 'vote'],
            "Economics": ['fed ', 'interest rate', 'gdp', 'inflation', 'unemployment', 'cpi', 'fomc', 'treasury'],
            "Crypto": ['bitcoin', 'btc', 'ethereum', 'eth', 'crypto', 'solana', 'dogecoin', 'xrp', 'token'],
        }

        conditions = []
        params = []
        for cat in category_filter:
            keywords = _CAT_KEYWORDS.get(cat, [])
            for kw in keywords:
                conditions.append("market_question LIKE ?")
                params.append(f"%{kw}%")
            # Also match category column
            conditions.append("category = ?")
            params.append(cat)

        if not conditions:
            # Fallback: match by strategy category
            cat = row.get("category", "")
            if cat:
                keywords = _CAT_KEYWORDS.get(cat, [])
                for kw in keywords:
                    conditions.append("market_question LIKE ?")
                    params.append(f"%{kw}%")
                conditions.append("category = ?")
                params.append(cat)

        if not conditions:
            return {"trades": 0, "wins": 0, "losses": 0, "open": 0, "total_pnl": 0,
                    "total_invested": 0, "win_rate": 0, "avg_pnl": 0, "roi_pct": 0,
                    "best_trade": 0, "worst_trade": 0, "recent_trades": []}

        where = "(" + " OR ".join(conditions) + ")"

        # Side filter
        side_filter = ""
        side_params = []
        if trade_side:
            side_filter = " AND side = ?"
            side_params = [trade_side]

        # Summary stats
        stats = engine.query_one(
            f"""SELECT
                COUNT(*) as total_trades,
                SUM(CASE WHEN result = 'win' OR result = 'take_profit' THEN 1 ELSE 0 END) as wins,
                SUM(CASE WHEN result = 'loss' THEN 1 ELSE 0 END) as losses,
                SUM(CASE WHEN result IS NULL OR result = 'open' THEN 1 ELSE 0 END) as open_trades,
                COALESCE(SUM(pnl), 0) as total_pnl,
                COALESCE(SUM(amount_usd), 0) as total_invested,
                COALESCE(AVG(CASE WHEN pnl IS NOT NULL THEN pnl END), 0) as avg_pnl,
                COALESCE(MAX(pnl), 0) as best_trade,
                COALESCE(MIN(CASE WHEN pnl IS NOT NULL THEN pnl END), 0) as worst_trade,
                MIN(created_at) as first_trade,
                MAX(created_at) as last_trade
            FROM trades
            WHERE status IN ('executed', 'closed') AND {where}{side_filter}""",
            tuple(params + side_params),
        ) or {}

        total = stats.get("total_trades", 0) or 0
        wins = stats.get("wins", 0) or 0
        losses = stats.get("losses", 0) or 0
        settled = wins + losses
        total_pnl = stats.get("total_pnl", 0) or 0
        total_invested = stats.get("total_invested", 0) or 0

        # Recent trades
        recent = engine.query(
            f"""SELECT id, side, amount_usd, pnl, result, status,
                       substr(market_question, 1, 80) as question, created_at
            FROM trades
            WHERE status IN ('executed', 'closed') AND {where}{side_filter}
            ORDER BY created_at DESC LIMIT 10""",
            tuple(params + side_params),
        ) or []

        return {
            "trades": total,
            "wins": wins,
            "losses": losses,
            "open": stats.get("open_trades", 0) or 0,
            "total_pnl": round(total_pnl, 2),
            "total_invested": round(total_invested, 2),
            "win_rate": round(wins / settled * 100, 1) if settled > 0 else 0,
            "avg_pnl": round(stats.get("avg_pnl", 0) or 0, 2),
            "roi_pct": round(total_pnl / total_invested * 100, 1) if total_invested > 0 else 0,
            "best_trade": round(stats.get("best_trade", 0) or 0, 2),
            "worst_trade": round(stats.get("worst_trade", 0) or 0, 2),
            "first_trade": stats.get("first_trade"),
            "last_trade": stats.get("last_trade"),
            "recent_trades": recent,
        }

    # ------------------------------------------------------------------
    # Backtesting
    # ------------------------------------------------------------------
    # ------------------------------------------------------------------
    # Snapshot-based backtesting (uses 30k+ market snapshots)
    # ------------------------------------------------------------------

    class SnapshotBacktestRequest(BaseModel):
        strategy: str = ""  # strategy name or id
        days: int = 7
        initial_capital: float = 1000.0

    @app.post("/api/backtest/snapshot", dependencies=[Depends(verify_api_key)])
    def run_snapshot_backtest_endpoint(req: SnapshotBacktestRequest):
        from services.backtester import run_snapshot_backtest
        # Resolve strategy by name or id
        strategy_id = req.strategy
        if not strategy_id.startswith("strat_"):
            row = engine.query_one(
                "SELECT id FROM strategies WHERE name = ?", (req.strategy,)
            )
            if row:
                strategy_id = row["id"]
            else:
                raise HTTPException(status_code=404, detail=f"Strategy not found: {req.strategy}")
        result = run_snapshot_backtest(strategy_id, days=req.days, initial_capital=req.initial_capital)
        if not result.get("ok"):
            raise HTTPException(status_code=400, detail=result.get("error", "Backtest failed"))
        return result

    @app.post("/api/backtest/snapshot/all", dependencies=[Depends(verify_api_key)])
    def run_all_snapshot_backtests(days: int = Query(default=7)):
        from services.backtester import run_all_backtests
        return run_all_backtests(days=days)

    @app.get("/api/backtest/snapshot/history/{strategy_id}", dependencies=[Depends(verify_api_key)])
    def get_snapshot_backtest_history(strategy_id: str, limit: int = Query(default=10)):
        from services.backtester import get_backtest_history
        return get_backtest_history(strategy_id, limit=limit)

    @app.get("/api/backtest/snapshot/latest", dependencies=[Depends(verify_api_key)])
    def get_latest_snapshot_backtests():
        from services.backtester import get_latest_all_results
        return get_latest_all_results()


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

    @app.post("/api/trades/cashout", dependencies=[Depends(verify_api_key)])
    def manual_cashout(body: CashoutRequest):
        """Manually cashout (sell) an open position by trade ID."""
        trade = engine.query_one(
            "SELECT id, market_id, market_question, side, amount_usd, price "
            "FROM trades WHERE id = ? AND status = 'executed' AND (result IS NULL OR result = 'open')",
            (body.trade_id,),
        )
        if not trade:
            raise HTTPException(404, "Trade not found or already closed")

        market = engine.query_one(
            "SELECT yes_price, no_price, yes_token_id, no_token_id FROM markets WHERE id = ?",
            (trade["market_id"],),
        )
        if not market:
            raise HTTPException(404, "Market not found")

        token_id = market.get("yes_token_id") if trade["side"] == "YES" else market.get("no_token_id")
        if not token_id:
            raise HTTPException(400, "No token ID for this position")

        if not config.polymarket_private_key:
            raise HTTPException(400, "No private key configured")

        entry_price = trade["price"] or 0
        current_price = market.get("yes_price") if trade["side"] == "YES" else market.get("no_price")
        db_shares = (trade["amount_usd"] / entry_price) if entry_price > 0 else 0
        profit_usd = (current_price - entry_price) * db_shares if entry_price > 0 else 0
        profit_pct = ((current_price - entry_price) / entry_price * 100) if entry_price > 0 else 0

        # Get actual on-chain balance for full sell
        actual_shares = None
        try:
            from py_clob_client.clob_types import BalanceAllowanceParams, AssetType
            from services.polymarket_client import PolymarketService
            service = PolymarketService(config)
            _bal_params = BalanceAllowanceParams(asset_type=AssetType.CONDITIONAL, token_id=token_id)
            _bal_resp = service._auth_client.get_balance_allowance(_bal_params)
            if _bal_resp and isinstance(_bal_resp, dict):
                raw_bal = int(_bal_resp.get("balance", "0"))
                actual_shares = raw_bal / 1e6
        except Exception:
            pass

        # Use on-chain balance (full amount), fall back to DB
        shares = round(actual_shares, 2) if actual_shares and actual_shares > 0 else round(db_shares, 2)

        # If balance is dust, just close the DB record
        if shares < 0.5:
            engine.execute(
                "UPDATE trades SET result = 'cashout', pnl = ? WHERE id = ?",
                (round(profit_usd, 4), trade["id"]),
            )
            return {"ok": True, "message": f"Dust position closed (shares={shares:.4f})", "profit_usd": round(profit_usd, 4)}

        try:
            result = service.place_sell_order(token_id=token_id, amount=shares)

            sell_value_usd = round(shares * current_price, 2) if current_price else trade["amount_usd"]

            if result.get("ok"):
                # Record cashout
                engine.execute(
                    """INSERT INTO trades
                       (market_id, market_question, side, amount_usd, price, status, agent_id, user_cmd, created_at, executed_at, result, pnl)
                       VALUES (?, ?, ?, ?, ?, 'executed', 'user', ?, ?, ?, 'cashout', ?)""",
                    (trade["market_id"], f"CASHOUT: {trade.get('market_question', '')[:50]}",
                     trade["side"], -sell_value_usd, current_price,
                     f"manual_cashout:{trade['id']}",
                     datetime.utcnow().isoformat(), datetime.utcnow().isoformat(),
                     round(profit_usd, 4)),
                )
                # Mark original as cashed out
                engine.execute(
                    "UPDATE trades SET result = 'cashout', pnl = ? WHERE id = ?",
                    (round(profit_usd, 4), trade["id"]),
                )
                # Telegram notification
                try:
                    from services.telegram_alerts import get_alerts
                    alerts = get_alerts(config)
                    alerts.send(
                        f"🔧 <b>Manueller Cashout</b>\n"
                        f"Markt: {trade.get('market_question', '')[:60]}\n"
                        f"Seite: {trade['side']} | Entry: {entry_price:.4f} → Sell: {current_price:.4f}\n"
                        f"Verkauft: {shares:.1f} Anteile\n"
                        f"Netto-Profit: ${profit_usd:+.2f} ({profit_pct:+.1f}%)"
                    )
                except Exception:
                    pass
                return {"ok": True, "profit_usd": round(profit_usd, 2), "profit_pct": round(profit_pct, 1)}
            else:
                return {"ok": False, "error": result.get("error", "Sell order failed")}
        except Exception as e:
            logger.error(f"Manual cashout failed: {e}")
            raise HTTPException(500, str(e))

    @app.post("/api/trades/import", dependencies=[Depends(verify_api_key)])
    def import_position(body: ImportPositionRequest):
        """Import an on-chain position into the DB so the bot can manage it."""
        # Check if already tracked
        existing = engine.query_one(
            "SELECT id, status, result FROM trades WHERE market_id = ? "
            "AND status IN ('executed', 'closed') ORDER BY created_at DESC LIMIT 1",
            (body.market_id,),
        )
        if existing:
            return {"ok": False, "error": "Position already tracked or previously closed", "trade_id": existing["id"]}

        side = body.outcome.upper() if body.outcome else "YES"
        engine.execute(
            """INSERT INTO trades
               (market_id, market_question, side, amount_usd, price, status, agent_id, user_cmd, created_at, executed_at)
               VALUES (?, ?, ?, ?, ?, 'executed', 'user', 'imported', ?, ?)""",
            (body.market_id, body.title, side, round(body.cost, 2),
             round(body.avg_price, 4),
             datetime.utcnow().isoformat(), datetime.utcnow().isoformat()),
        )
        trade_id = engine.query_one("SELECT last_insert_rowid() as id")["id"]
        return {"ok": True, "trade_id": trade_id}

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
    # Health Monitor
    # ------------------------------------------------------------------

    @app.get("/api/health/monitor")
    async def health_monitor_report():
        """Get health status of all strategy sources."""
        try:
            from services.health_monitor import get_health_report
            from config import load_platform_config
            config = load_platform_config()
            return get_health_report(config)
        except Exception as e:
            return {"error": str(e)}

    # ------------------------------------------------------------------
# ------------------------------------------------------------------
    # Edge Tracking
    # ------------------------------------------------------------------

    @app.get("/api/edge/statistics")
    async def edge_statistics(days: int = 30):
        """Get edge tracking statistics."""
        try:
            from services.edge_tracker import get_edge_statistics
            from db import engine
            return get_edge_statistics(engine, days)
        except Exception as e:
            return {"error": str(e)}

    @app.get("/api/edge/market/{market_id}")
    async def edge_market_history(market_id: str):
        """Get edge history for a specific market."""
        try:
            from services.edge_tracker import get_market_edge_history
            from db import engine
            return get_market_edge_history(engine, market_id)
        except Exception as e:
            return {"error": str(e)}

    # Self-Modification (code change proposals by AI agents)
    # ------------------------------------------------------------------

    from api.self_modify import register_self_modify_endpoints
    register_self_modify_endpoints(app)

    return app
