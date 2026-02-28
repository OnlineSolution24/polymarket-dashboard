"""
Autonomous Trader Agent.
Reads approved suggestions/recommendations, validates against risk rules,
and executes trades on Polymarket.

Supports 3 modes (configured in platform_config.yaml → trading.mode):
  - paper:     Simulate trades, log everything, never place real orders.
  - semi-auto: Create suggestions for user to approve in Dashboard.
  - full-auto: Execute trades autonomously (with all safety checks).
"""

import json
import logging
from datetime import datetime

from agents.base_agent import BaseAgent
from config import AppConfig, load_platform_config
from db import engine

logger = logging.getLogger(__name__)


class TraderAgent(BaseAgent):
    """
    Autonomous Trader with safety checks.
    Processes approved trade suggestions and executes on Polymarket.
    """

    def run_cycle(self) -> dict:
        """One trading cycle: check suggestions → validate → execute."""
        # Check if bot is paused
        try:
            from bot_main import bot_state
            if bot_state.paused:
                self.log("debug", "Bot pausiert — skip")
                return {"ok": True, "summary": "Bot paused, skipping"}
        except ImportError:
            pass

        trading_cfg = self._get_trading_config()
        mode = trading_cfg.get("mode", "paper")
        self.log("debug", f"Trader cycle (mode={mode})")

        try:
            executed = 0

            if mode == "full-auto":
                executed = self._process_auto_approved()
                executed += self._process_user_approved()
            elif mode == "semi-auto":
                executed = self._process_user_approved()
                self._create_trade_suggestions()
            else:
                # paper mode — simulate only
                executed = self._process_paper_trades()

            return {"ok": True, "summary": f"Trader cycle done. Mode={mode}, executed={executed}"}

        except Exception as e:
            self.log("error", f"Trader cycle fehlgeschlagen: {e}")
            return {"ok": False, "summary": str(e)}

    # ------------------------------------------------------------------
    # Trade processing by mode
    # ------------------------------------------------------------------

    def _process_auto_approved(self) -> int:
        """Process suggestions that were auto-approved by the Chief (full-auto mode)."""
        suggestions = engine.query(
            "SELECT * FROM suggestions WHERE status = 'auto_approved' AND type = 'trade' ORDER BY created_at"
        )
        count = 0
        for s in suggestions:
            payload = json.loads(s.get("payload") or "{}")
            if self._execute_trade(payload, source=f"suggestion:{s['id']}"):
                engine.execute(
                    "UPDATE suggestions SET status = 'executed', resolved_at = ? WHERE id = ?",
                    (datetime.utcnow().isoformat(), s["id"]),
                )
                count += 1
        return count

    def _process_user_approved(self) -> int:
        """Process suggestions that the user approved via Dashboard."""
        suggestions = engine.query(
            "SELECT * FROM suggestions WHERE status = 'approved' AND type = 'trade' ORDER BY created_at"
        )
        count = 0
        for s in suggestions:
            payload = json.loads(s.get("payload") or "{}")
            if self._execute_trade(payload, source=f"suggestion:{s['id']}"):
                engine.execute(
                    "UPDATE suggestions SET status = 'executed', resolved_at = ? WHERE id = ?",
                    (datetime.utcnow().isoformat(), s["id"]),
                )
                count += 1
        return count

    def _process_paper_trades(self) -> int:
        """Paper mode: process pending trades as simulated."""
        suggestions = engine.query(
            "SELECT * FROM suggestions WHERE status IN ('auto_approved', 'approved') AND type = 'trade' ORDER BY created_at"
        )
        count = 0
        for s in suggestions:
            payload = json.loads(s.get("payload") or "{}")
            self._simulate_trade(payload, source=f"suggestion:{s['id']}")
            engine.execute(
                "UPDATE suggestions SET status = 'executed', resolved_at = ? WHERE id = ?",
                (datetime.utcnow().isoformat(), s["id"]),
            )
            count += 1
        return count

    def _create_trade_suggestions(self) -> None:
        """Semi-auto: look at markets with good edge and create suggestions for the user."""
        markets = engine.query(
            "SELECT * FROM markets WHERE calculated_edge IS NOT NULL AND calculated_edge > 0.05 "
            "ORDER BY calculated_edge DESC LIMIT 5"
        )
        for m in markets:
            # Check if we already have a pending suggestion for this market
            existing = engine.query_one(
                "SELECT id FROM suggestions WHERE type = 'trade' AND status = 'pending' "
                "AND payload LIKE ?",
                (f'%"market_id": "{m["id"]}"%',),
            )
            if existing:
                continue

            side = "YES" if m["yes_price"] < 0.5 else "NO"
            amount = self._calculate_position_size(m)
            if amount <= 0:
                continue

            self.create_suggestion(
                type="trade",
                title=f"Trade: {side} auf '{m['question'][:50]}...'",
                description=(
                    f"Edge: {m['calculated_edge']:.1%} | "
                    f"Preis: YES={m['yes_price']:.2f} NO={m['no_price']:.2f} | "
                    f"Empfohlen: ${amount:.2f} auf {side}"
                ),
                payload={
                    "market_id": m["id"],
                    "market_question": m["question"],
                    "side": side,
                    "amount_usd": amount,
                    "edge": m["calculated_edge"],
                    "yes_price": m["yes_price"],
                    "no_price": m["no_price"],
                },
            )

    # ------------------------------------------------------------------
    # Safety checks
    # ------------------------------------------------------------------

    def _validate_trade(self, payload: dict) -> tuple[bool, str]:
        """Run all safety checks before executing a trade. Returns (ok, reason)."""
        trading_cfg = self._get_trading_config()
        limits = trading_cfg.get("limits", {})

        # 1. Circuit breaker
        cb = engine.query_one("SELECT * FROM circuit_breaker WHERE id = 1")
        if cb and cb.get("paused_until"):
            paused_until = datetime.fromisoformat(cb["paused_until"])
            if datetime.utcnow() < paused_until:
                return False, f"Circuit Breaker aktiv bis {cb['paused_until']}"

        # 2. Daily loss limit
        max_daily_loss = limits.get("max_daily_loss_usd", 50)
        today_pnl = engine.query_one(
            "SELECT COALESCE(SUM(pnl), 0) as total FROM trades "
            "WHERE date(executed_at) = date('now') AND pnl < 0"
        )
        if today_pnl and abs(today_pnl["total"]) >= max_daily_loss:
            return False, f"Tägliches Verlustlimit erreicht (${abs(today_pnl['total']):.2f} / ${max_daily_loss:.2f})"

        # 3. Position size limit
        amount = payload.get("amount_usd", 0)
        max_position_pct = limits.get("max_position_pct", 5) / 100
        capital = trading_cfg.get("capital_usd", 100)
        max_position = capital * max_position_pct
        if amount > max_position:
            return False, f"Position zu groß (${amount:.2f} > max ${max_position:.2f})"

        # 4. Minimum edge
        min_edge = limits.get("min_edge", 0.03)
        edge = payload.get("edge", 0)
        if edge < min_edge:
            return False, f"Edge zu klein ({edge:.1%} < {min_edge:.1%})"

        # 5. Category blacklist
        blacklist = trading_cfg.get("category_blacklist", [])
        market = engine.query_one(
            "SELECT category FROM markets WHERE id = ?",
            (payload.get("market_id"),),
        )
        if market and market.get("category") in blacklist:
            return False, f"Kategorie '{market['category']}' ist gesperrt"

        # 6. Budget check
        from services.cost_tracker import check_budget
        budget = check_budget(agent_id=self.id)
        if not budget["allowed"]:
            return False, f"Budget: {budget['reason']}"

        return True, "OK"

    # ------------------------------------------------------------------
    # Trade execution
    # ------------------------------------------------------------------

    def _execute_trade(self, payload: dict, source: str = "") -> bool:
        """Execute a real trade on Polymarket after validation."""
        market_id = payload.get("market_id", "")
        side = payload.get("side", "YES")
        amount = payload.get("amount_usd", 0)
        question = payload.get("market_question", "")

        # Validate
        ok, reason = self._validate_trade(payload)
        if not ok:
            self.log("warn", f"Trade abgelehnt: {reason} | {question[:50]}")
            return False

        # Insert trade record
        engine.execute(
            """INSERT INTO trades (market_id, market_question, side, amount_usd, price, status, agent_id, user_cmd, created_at)
               VALUES (?, ?, ?, ?, ?, 'executing', ?, ?, ?)""",
            (market_id, question, side, amount,
             payload.get("yes_price") if side == "YES" else payload.get("no_price"),
             self.id, source, datetime.utcnow().isoformat()),
        )
        trade_id = engine.query_one("SELECT last_insert_rowid() as id")["id"]

        # Execute on Polymarket
        try:
            config = AppConfig.from_env()
            if not config.polymarket_private_key:
                # No key configured — fall back to paper trade
                self.log("warn", "Kein Polymarket-Key. Führe Paper Trade aus.")
                self._finalize_trade(trade_id, "paper", payload)
                return True

            from services.polymarket_client import PolymarketService
            service = PolymarketService(config)

            token_id = market_id  # In production, resolve from market tokens

            result = service.place_market_order(
                token_id=token_id,
                amount=amount,
                side=side,
            )

            if result.get("ok"):
                self._finalize_trade(trade_id, "executed", payload)
                self.log("info", f"Trade ausgeführt: {side} ${amount:.2f} auf '{question[:50]}'")

                # Alert
                try:
                    from services.telegram_alerts import get_alerts
                    alerts = get_alerts(config)
                    alerts.alert_trade_executed(question[:60], side, amount)
                except Exception:
                    pass

                return True
            else:
                error = result.get("error", "Unknown error")
                self._finalize_trade(trade_id, "failed", payload, error=error)
                self.log("error", f"Trade fehlgeschlagen: {error}")
                return False

        except Exception as e:
            self._finalize_trade(trade_id, "failed", payload, error=str(e))
            self.log("error", f"Trade Exception: {e}")
            return False

    def _simulate_trade(self, payload: dict, source: str = "") -> None:
        """Paper trade: log everything but don't place a real order."""
        market_id = payload.get("market_id", "")
        side = payload.get("side", "YES")
        amount = payload.get("amount_usd", 0)
        question = payload.get("market_question", "")

        # Still validate
        ok, reason = self._validate_trade(payload)
        if not ok:
            self.log("info", f"[PAPER] Trade abgelehnt: {reason}")
            return

        engine.execute(
            """INSERT INTO trades (market_id, market_question, side, amount_usd, price, status, agent_id, user_cmd, created_at, executed_at)
               VALUES (?, ?, ?, ?, ?, 'paper', ?, ?, ?, ?)""",
            (market_id, question, side, amount,
             payload.get("yes_price") if side == "YES" else payload.get("no_price"),
             self.id, f"paper|{source}", datetime.utcnow().isoformat(), datetime.utcnow().isoformat()),
        )
        self.log("info", f"[PAPER] Trade: {side} ${amount:.2f} auf '{question[:50]}'")

    def _finalize_trade(self, trade_id: int, status: str, payload: dict, error: str = None) -> None:
        """Update trade record with final status."""
        engine.execute(
            "UPDATE trades SET status = ?, executed_at = ? WHERE id = ?",
            (status, datetime.utcnow().isoformat(), trade_id),
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _calculate_position_size(self, market: dict) -> float:
        """Calculate appropriate position size based on edge and capital."""
        trading_cfg = self._get_trading_config()
        limits = trading_cfg.get("limits", {})
        capital = trading_cfg.get("capital_usd", 100)
        max_pct = limits.get("max_position_pct", 5) / 100
        edge = market.get("calculated_edge", 0) or 0

        if edge <= 0:
            return 0

        # Simple Kelly fraction (capped)
        kelly_fraction = min(edge, max_pct)
        amount = capital * kelly_fraction

        # Minimum trade size
        min_trade = limits.get("min_trade_usd", 1.0)
        if amount < min_trade:
            return 0

        return round(amount, 2)

    def _get_trading_config(self) -> dict:
        """Get trading section from platform config."""
        platform_cfg = load_platform_config()
        return platform_cfg.get("trading", {
            "mode": "paper",
            "capital_usd": 100,
            "limits": {
                "max_position_pct": 5,
                "max_daily_loss_usd": 50,
                "min_edge": 0.03,
                "min_trade_usd": 1.0,
            },
            "category_blacklist": [],
        })
