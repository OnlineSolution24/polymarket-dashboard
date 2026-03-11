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
from datetime import datetime, timedelta

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
                self._check_and_execute_cashouts()
                self._check_and_execute_hedges()
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
            # Global deduplication check (covers pending suggestions, open positions, cooldown)
            allowed, reason = self._check_deduplication(m["id"])
            if not allowed:
                self.log("debug", f"Suggestion skip: {reason}")
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
    # Deduplication
    # ------------------------------------------------------------------

    def _check_deduplication(self, market_id: str, side: str = None) -> tuple[bool, str]:
        """
        Global deduplication check for a market.
        Returns (allowed, reason). If allowed=False, skip the trade.
        """
        trading_cfg = self._get_trading_config()
        dedup_cfg = trading_cfg.get("deduplication", {})
        if not dedup_cfg.get("enabled", True):
            return True, "OK"

        cooldown_hours = dedup_cfg.get("cooldown_hours", 24)
        max_attempts = dedup_cfg.get("max_attempts_per_market", 3)
        check_pending = dedup_cfg.get("check_pending_suggestions", True)

        # 1. Existing open position (any side)
        open_position = engine.query_one(
            "SELECT id, side FROM trades WHERE market_id = ? AND status = 'executed'",
            (market_id,),
        )
        if open_position:
            return False, f"Skipping market {market_id[:30]} - position exists ({open_position['side']})"

        # 2. Recent attempts within cooldown window (any status except 'paper')
        cutoff = (datetime.utcnow() - timedelta(hours=cooldown_hours)).isoformat()
        recent = engine.query_one(
            "SELECT COUNT(*) as cnt FROM trades "
            "WHERE market_id = ? AND status != 'paper' AND created_at > ?",
            (market_id, cutoff),
        )
        if recent and recent["cnt"] > 0:
            return False, f"Skipping market {market_id[:30]} - already attempted within {cooldown_hours}h"

        # 3. Max attempts per market (all time)
        total = engine.query_one(
            "SELECT COUNT(*) as cnt FROM trades "
            "WHERE market_id = ? AND status != 'paper'",
            (market_id,),
        )
        if total and total["cnt"] >= max_attempts:
            return False, f"Skipping market {market_id[:30]} - max attempts reached ({total['cnt']}/{max_attempts})"

        # 4. Pending or auto_approved suggestion for same market
        if check_pending:
            pending = engine.query_one(
                "SELECT id FROM suggestions WHERE type = 'trade' "
                "AND status IN ('pending', 'auto_approved') AND payload LIKE ?",
                (f'%"market_id": "{market_id}"%',),
            )
            if pending:
                return False, f"Skipping market {market_id[:30]} - pending suggestion exists"

        return True, "OK"

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

        # 5b. Re-buy cooldown: wait 7 days after closing a position in same market
        rebuy_cooldown_days = trading_cfg.get("rebuy_cooldown_days", 7)
        last_closed = engine.query_one(
            "SELECT MAX(executed_at) as last_close FROM trades WHERE market_id = ? "
            "AND result IN ('cashout', 'win', 'loss', 'settled')",
            (payload.get("market_id"),),
        )
        if last_closed and last_closed.get("last_close"):
            try:
                closed_at = datetime.fromisoformat(last_closed["last_close"])
                if (datetime.utcnow() - closed_at).days < rebuy_cooldown_days:
                    days_left = rebuy_cooldown_days - (datetime.utcnow() - closed_at).days
                    return False, f"Re-Buy Cooldown: noch {days_left} Tage warten"
            except (ValueError, TypeError):
                pass

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

        # Global deduplication check
        allowed, reason = self._check_deduplication(market_id, side)
        if not allowed:
            self.log("info", reason)
            return False

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

            # Resolve correct CLOB token ID from market data
            market_row = engine.query_one(
                "SELECT yes_token_id, no_token_id FROM markets WHERE id = ?",
                (market_id,),
            )
            if not market_row:
                self._finalize_trade(trade_id, "failed", payload, error="Market not found in DB")
                self.log("error", f"Market {market_id[:30]} nicht in DB gefunden")
                return False

            token_id = market_row.get("yes_token_id") if side == "YES" else market_row.get("no_token_id")
            if not token_id:
                self._finalize_trade(trade_id, "failed", payload, error="No CLOB token ID available")
                self.log("error", f"Kein CLOB Token-ID für {market_id[:30]} (side={side})")
                return False

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
    # Profit Hedging
    # ------------------------------------------------------------------

    def _check_and_execute_hedges(self) -> int:
        """Check open positions for profit-taking opportunities."""
        trading_cfg = self._get_trading_config()
        hedge_cfg = trading_cfg.get("hedging", {})
        if not hedge_cfg.get("enabled", False):
            return 0

        threshold_pct = hedge_cfg.get("profit_threshold_pct", 15)
        hedge_amount_pct = hedge_cfg.get("hedge_amount_pct", 50) / 100
        min_profit_usd = hedge_cfg.get("min_profit_usd", 1.0)
        cooldown_min = hedge_cfg.get("cooldown_minutes", 60)

        # Get all open positions (executed trades that haven't been settled/hedged yet)
        positions = engine.query(
            "SELECT id, market_id, side, amount_usd, price, executed_at "
            "FROM trades WHERE status = 'executed' "
            "AND (result IS NULL OR result = 'open') "
            "AND amount_usd > 0 "
            "ORDER BY executed_at"
        )
        if not positions:
            return 0

        hedged = 0
        for pos in positions:
            market_id = pos["market_id"]
            entry_price = pos.get("price") or 0
            if entry_price <= 0:
                continue

            # Skip if this market was already cashed out or settled
            already_closed = engine.query_one(
                "SELECT id FROM trades WHERE market_id = ? "
                "AND (result IN ('cashout', 'win', 'loss', 'settled') "
                "     OR user_cmd LIKE 'cashout:%')",
                (market_id,),
            )
            if already_closed:
                continue

            # Max 1 hedge per original position
            existing_hedge = engine.query_one(
                "SELECT id FROM trades WHERE user_cmd = ?",
                (f"hedge:{pos['id']}",),
            )
            if existing_hedge:
                continue

            # Cooldown: check if we recently hedged this market
            recent_hedge = engine.query_one(
                "SELECT id FROM trades WHERE market_id = ? AND user_cmd LIKE 'hedge:%' "
                "AND executed_at > ?",
                (market_id, (datetime.utcnow() - timedelta(minutes=cooldown_min)).isoformat()),
            )
            if recent_hedge:
                continue

            # Get current market price
            market = engine.query_one(
                "SELECT yes_price, no_price, yes_token_id, no_token_id "
                "FROM markets WHERE id = ?",
                (market_id,),
            )
            if not market:
                continue

            current_price = market.get("yes_price") if pos["side"] == "YES" else market.get("no_price")
            if not current_price or current_price <= 0:
                continue

            # Calculate profit
            profit_pct = ((current_price - entry_price) / entry_price) * 100
            profit_usd = (current_price - entry_price) * (pos["amount_usd"] / entry_price)

            if profit_pct < threshold_pct or profit_usd < min_profit_usd:
                continue

            # Execute hedge: sell portion of position
            sell_amount = round(pos["amount_usd"] * hedge_amount_pct, 2)
            token_id = market.get("yes_token_id") if pos["side"] == "YES" else market.get("no_token_id")
            if not token_id:
                continue

            self.log("info",
                f"Hedging: {pos['side']} Position in {market_id[:30]}... "
                f"Profit: {profit_pct:.1f}% (${profit_usd:.2f}), selling ${sell_amount:.2f}")

            try:
                config = AppConfig.from_env()
                if not config.polymarket_private_key:
                    self.log("info", f"[PAPER-HEDGE] Wuerde ${sell_amount:.2f} verkaufen")
                    continue

                from services.polymarket_client import PolymarketService
                service = PolymarketService(config)
                result = service.place_sell_order(token_id=token_id, amount=sell_amount)

                if result.get("ok"):
                    # Record hedge trade (result='hedge' so it's not treated as open position)
                    engine.execute(
                        """INSERT INTO trades
                           (market_id, market_question, side, amount_usd, price, status, result, agent_id, user_cmd, created_at, executed_at)
                           VALUES (?, ?, ?, ?, ?, 'executed', 'hedge', ?, ?, ?, ?)""",
                        (market_id, f"HEDGE: {pos.get('market_question', '')[:50]}",
                         pos["side"], -sell_amount, current_price,
                         self.id, f"hedge:{pos['id']}",
                         datetime.utcnow().isoformat(), datetime.utcnow().isoformat()),
                    )
                    self.log("info", f"Hedge ausgefuehrt: SELL ${sell_amount:.2f} (Profit: {profit_pct:.1f}%)")

                    try:
                        from services.telegram_alerts import get_alerts
                        alerts = get_alerts(config)
                        alerts.send(
                            f"🛡 <b>Profit-Hedge ausgefuehrt</b>\n"
                            f"Markt: {market_id[:40]}...\n"
                            f"Profit: {profit_pct:.1f}% (${profit_usd:.2f})\n"
                            f"Verkauft: ${sell_amount:.2f} ({pos['side']})"
                        )
                    except Exception:
                        pass

                    hedged += 1
                else:
                    self.log("warn", f"Hedge fehlgeschlagen: {result.get('error', '?')}")

            except Exception as e:
                self.log("error", f"Hedge Exception: {e}")

        return hedged

    # ------------------------------------------------------------------
    # Auto-Cashout (sell 100% when in profit)
    # ------------------------------------------------------------------

    def _check_and_execute_cashouts(self) -> int:
        """Auto-sell positions when profit target is reached.

        Two modes:
        - Normal: sell at min_profit_pct (default 10%) and min_profit_usd
        - Aged positions: after max_hold_hours (default 7 days), sell at force_sell_profit_pct (default 3%)
        """
        trading_cfg = self._get_trading_config()
        cashout_cfg = trading_cfg.get("cashout", {})
        if not cashout_cfg.get("enabled", False):
            return 0

        sell_pct = cashout_cfg.get("sell_pct", 100) / 100
        min_profit_pct = cashout_cfg.get("min_profit_pct", 10)
        min_profit_usd = cashout_cfg.get("min_profit_usd", 0.50)
        max_hold_hours = cashout_cfg.get("max_hold_hours", 168)
        force_sell_profit_pct = cashout_cfg.get("force_sell_profit_pct", 3)
        cooldown_min = cashout_cfg.get("cooldown_minutes", 30)

        # Get open positions with a recorded entry price
        positions = engine.query(
            "SELECT id, market_id, market_question, side, amount_usd, price, executed_at "
            "FROM trades WHERE status = 'executed' AND (result = 'open' OR result IS NULL) "
            "AND price IS NOT NULL AND price > 0 ORDER BY executed_at"
        )
        if not positions:
            return 0

        cashed_out = 0
        for pos in positions:
            market_id = pos["market_id"]
            entry_price = pos["price"]

            # Cooldown: skip if we recently tried to cashout this market
            recent = engine.query_one(
                "SELECT id FROM trades WHERE market_id = ? AND user_cmd LIKE 'cashout:%' "
                "AND executed_at > ?",
                (market_id, (datetime.utcnow() - timedelta(minutes=cooldown_min)).isoformat()),
            )
            if recent:
                continue

            # Get current market price
            market = engine.query_one(
                "SELECT yes_price, no_price, yes_token_id, no_token_id "
                "FROM markets WHERE id = ?",
                (market_id,),
            )
            if not market:
                continue

            current_price = market.get("yes_price") if pos["side"] == "YES" else market.get("no_price")
            if not current_price or current_price <= 0:
                continue

            # Calculate profit
            profit_pct = ((current_price - entry_price) / entry_price) * 100
            shares = pos["amount_usd"] / entry_price
            profit_usd = (current_price - entry_price) * shares

            # Determine threshold: lower for aged positions
            threshold_pct = min_profit_pct
            if pos.get("executed_at"):
                try:
                    age_hours = (datetime.utcnow() - datetime.fromisoformat(pos["executed_at"])).total_seconds() / 3600
                    if age_hours > max_hold_hours:
                        threshold_pct = force_sell_profit_pct
                except (ValueError, TypeError):
                    pass

            if profit_pct < threshold_pct or profit_usd < min_profit_usd:
                continue

            # Execute cashout: sell position
            sell_amount = round(pos["amount_usd"] * sell_pct, 2)
            token_id = market.get("yes_token_id") if pos["side"] == "YES" else market.get("no_token_id")
            if not token_id:
                continue

            self.log("info",
                f"CASHOUT: {pos['side']} Position in {market_id[:30]}... "
                f"Profit: {profit_pct:.1f}% (${profit_usd:.2f}), selling ${sell_amount:.2f}")

            try:
                config = AppConfig.from_env()
                if not config.polymarket_private_key:
                    self.log("info", f"[PAPER-CASHOUT] Would sell ${sell_amount:.2f}")
                    continue

                from services.polymarket_client import PolymarketService
                service = PolymarketService(config)
                result = service.place_sell_order(token_id=token_id, amount=sell_amount)

                if result.get("ok"):
                    # Record cashout trade
                    engine.execute(
                        """INSERT INTO trades
                           (market_id, market_question, side, amount_usd, price, status, agent_id, user_cmd, created_at, executed_at, result, pnl)
                           VALUES (?, ?, ?, ?, ?, 'executed', ?, ?, ?, ?, 'cashout', ?)""",
                        (market_id, f"CASHOUT: {pos.get('market_question', '')[:50]}",
                         pos["side"], -sell_amount, current_price,
                         self.id, f"cashout:{pos['id']}",
                         datetime.utcnow().isoformat(), datetime.utcnow().isoformat(),
                         round(profit_usd, 4)),
                    )

                    # Mark original trade as closed with profit
                    if sell_pct >= 1.0:
                        engine.execute(
                            "UPDATE trades SET result = 'win', pnl = ? WHERE id = ?",
                            (round(profit_usd, 4), pos["id"]),
                        )

                    self.log("info", f"Cashout done: SELL ${sell_amount:.2f} (Profit: +${profit_usd:.2f})")

                    try:
                        from services.telegram_alerts import get_alerts
                        alerts = get_alerts(config)
                        alerts.send(
                            f"💰 <b>Auto-Cashout!</b>\n"
                            f"Markt: {pos.get('market_question', market_id)[:60]}\n"
                            f"Seite: {pos['side']} | Entry: {entry_price:.2f} → Now: {current_price:.2f}\n"
                            f"Profit: +${profit_usd:.2f} ({profit_pct:.1f}%)\n"
                            f"Verkauft: ${sell_amount:.2f}"
                        )
                    except Exception:
                        pass

                    cashed_out += 1
                else:
                    self.log("warn", f"Cashout failed: {result.get('error', '?')}")

            except Exception as e:
                self.log("error", f"Cashout Exception: {e}")

        return cashed_out

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
