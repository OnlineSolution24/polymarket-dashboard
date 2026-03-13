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
                self._check_stop_losses()
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
        seen_markets = set()  # prevent same-batch duplicates
        for s in suggestions:
            payload = json.loads(s.get("payload") or "{}")
            market_id = payload.get("market_id", "")

            # Skip if we already processed this market in this batch
            if market_id in seen_markets:
                engine.execute(
                    "UPDATE suggestions SET status = 'expired', resolved_at = ? WHERE id = ?",
                    (datetime.utcnow().isoformat(), s["id"]),
                )
                continue
            seen_markets.add(market_id)

            if self._execute_trade(payload, source=f"suggestion:{s['id']}"):
                engine.execute(
                    "UPDATE suggestions SET status = 'executed', resolved_at = ? WHERE id = ?",
                    (datetime.utcnow().isoformat(), s["id"]),
                )
                count += 1
            else:
                # Mark as failed so it doesn't retry forever
                engine.execute(
                    "UPDATE suggestions SET status = 'failed', resolved_at = ? WHERE id = ?",
                    (datetime.utcnow().isoformat(), s["id"]),
                )
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
            # Global deduplication check (covers open positions, re-buy cooldown)
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

    def _check_deduplication(self, market_id: str, side: str = None, suggestion_id: int = None) -> tuple[bool, str]:
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

        # 1. Existing OPEN position (include 'executing' to prevent race conditions)
        open_position = engine.query_one(
            "SELECT id, side FROM trades WHERE market_id = ? AND status IN ('executed', 'executing') "
            "AND (result IS NULL OR result = 'open')",
            (market_id,),
        )
        if open_position:
            return False, f"Skipping market {market_id[:30]} - open position exists ({open_position['side']})"

        # 2. Re-buy cooldown (from _validate_trade, checked here too for early exit)
        rebuy_cooldown_days = trading_cfg.get("rebuy_cooldown_days", 7)
        last_closed = engine.query_one(
            "SELECT MAX(executed_at) as last_close FROM trades WHERE market_id = ? "
            "AND result IN ('cashout', 'win', 'loss', 'settled')",
            (market_id,),
        )
        if last_closed and last_closed.get("last_close"):
            try:
                closed_at = datetime.fromisoformat(last_closed["last_close"])
                if (datetime.utcnow() - closed_at).days < rebuy_cooldown_days:
                    days_left = rebuy_cooldown_days - (datetime.utcnow() - closed_at).days
                    return False, f"Skipping market {market_id[:30]} - re-buy cooldown ({days_left}d left)"
            except (ValueError, TypeError):
                pass

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

        # 7. Diversification check (final gate)
        try:
            from services.diversification import classify_category, check_diversification
            market_data = engine.query_one(
                "SELECT category, slug, question FROM markets WHERE id = ?",
                (payload.get("market_id"),),
            )
            if market_data:
                cat = market_data.get("category") or ""
                known_cats = {"Sports", "Politics", "Economics", "Crypto", "Weather",
                              "Science & Tech", "Entertainment", "Other"}
                if cat not in known_cats:
                    cat = classify_category(
                        slug=market_data.get("slug", ""),
                        question=market_data.get("question", ""),
                    )
                amount = payload.get("amount_usd", 0)
                div_ok, div_reason = check_diversification(cat, amount)
                if not div_ok:
                    return False, f"Diversification: {div_reason}"
        except Exception as e:
            self.log("debug", f"Diversification check skipped: {e}")

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
        allowed, reason = self._check_deduplication(market_id, side, suggestion_id=None)
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

            # Skip if THIS specific position was already cashed out or settled
            already_closed = engine.query_one(
                "SELECT id FROM trades WHERE id = ? "
                "AND result IN ('cashout', 'win', 'loss', 'settled')",
                (pos["id"],),
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

            # Cooldown: skip if we recently tried to cashout THIS specific position
            pos_id = pos["id"]
            recent = engine.query_one(
                "SELECT id FROM trades WHERE user_cmd = ? AND executed_at > ?",
                (f"cashout:{pos_id}", (datetime.utcnow() - timedelta(minutes=cooldown_min)).isoformat()),
            )
            if recent:
                continue

            # Get current market price (prefer live CLOB bid over possibly stale DB)
            market = engine.query_one(
                "SELECT yes_price, no_price, yes_token_id, no_token_id "
                "FROM markets WHERE id = ?",
                (market_id,),
            )
            if not market:
                continue

            current_price = market.get("yes_price") if pos["side"] == "YES" else market.get("no_price")

            # Fetch live best-bid from CLOB order book for accurate cashout decisions
            token_id_for_price = market.get("yes_token_id") if pos["side"] == "YES" else market.get("no_token_id")
            if token_id_for_price:
                try:
                    from config import AppConfig as _AC
                    from services.polymarket_client import PolymarketService
                    _svc = PolymarketService(_AC.from_env())
                    _book = _svc.get_order_book(token_id_for_price)
                    # Handle both dict and OrderBookSummary object
                    bids = _book.get("bids", []) if isinstance(_book, dict) else getattr(_book, "bids", []) or []
                    if bids:
                        bids = sorted(bids, key=lambda b: float(b.get("price", 0) if isinstance(b, dict) else getattr(b, "price", 0)), reverse=True)  # CLOB returns ascending
                        bid0 = bids[0]
                        best_bid = float(bid0.get("price", 0) if isinstance(bid0, dict) else getattr(bid0, "price", 0))
                        if best_bid > 0:
                            self.log("debug", f"Live bid for {market_id[:20]}: {best_bid} (DB: {current_price})")
                            current_price = best_bid
                except Exception:
                    pass  # fallback to DB price

            if not current_price or current_price <= 0:
                continue

            # Calculate profit for the portion being sold
            profit_pct = ((current_price - entry_price) / entry_price) * 100
            shares = pos["amount_usd"] / entry_price
            sold_shares = shares * sell_pct
            profit_usd = (current_price - entry_price) * sold_shares

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

            # Execute cashout: sell shares (CLOB API expects token count, not USD)
            sell_shares = round(shares * sell_pct, 2)
            token_id = market.get("yes_token_id") if pos["side"] == "YES" else market.get("no_token_id")
            if not token_id:
                continue

            # Cap sell_shares to actual on-chain balance to avoid "not enough balance" errors
            try:
                from py_clob_client.clob_types import BalanceAllowanceParams, AssetType
                _bal_params = BalanceAllowanceParams(asset_type=AssetType.CONDITIONAL, token_id=token_id)
                _bal_resp = service_for_balance._auth_client.get_balance_allowance(_bal_params) if "service_for_balance" in dir() else None
            except Exception:
                _bal_resp = None
            if _bal_resp is None:
                try:
                    from config import AppConfig as _AC2
                    from services.polymarket_client import PolymarketService as _PS2
                    _svc2 = _PS2(_AC2.from_env())
                    from py_clob_client.clob_types import BalanceAllowanceParams, AssetType
                    _bal_params = BalanceAllowanceParams(asset_type=AssetType.CONDITIONAL, token_id=token_id)
                    _bal_resp = _svc2._auth_client.get_balance_allowance(_bal_params)
                except Exception:
                    _bal_resp = None
            if _bal_resp and isinstance(_bal_resp, dict):
                raw_bal = int(_bal_resp.get("balance", "0"))
                actual_shares = raw_bal / 1e6  # Polymarket uses 6 decimals for token amounts
                if actual_shares > 0 and sell_shares > actual_shares:
                    self.log("info", f"Capping sell from {sell_shares:.2f} to {actual_shares:.2f} (on-chain balance)")
                    sell_shares = round(actual_shares * 0.99, 2)  # 1% buffer for rounding

            sell_value_usd = round(sell_shares * current_price, 2)

            self.log("info",
                f"CASHOUT: {pos['side']} Position in {market_id[:30]}... "
                f"Profit: {profit_pct:.1f}% (${profit_usd:.2f}), selling {sell_shares:.1f} shares (${sell_value_usd:.2f})")

            try:
                config = AppConfig.from_env()
                if not config.polymarket_private_key:
                    self.log("info", f"[PAPER-CASHOUT] Would sell {sell_shares:.1f} shares")
                    continue

                from services.polymarket_client import PolymarketService
                service = PolymarketService(config)
                result = service.place_sell_order(token_id=token_id, amount=sell_shares)

                if result.get("ok"):
                    # Record cashout trade
                    engine.execute(
                        """INSERT INTO trades
                           (market_id, market_question, side, amount_usd, price, status, agent_id, user_cmd, created_at, executed_at, result, pnl)
                           VALUES (?, ?, ?, ?, ?, 'executed', ?, ?, ?, ?, 'cashout', ?)""",
                        (market_id, f"CASHOUT: {pos.get('market_question', '')[:50]}",
                         pos["side"], -sell_value_usd, current_price,
                         self.id, f"cashout:{pos['id']}",
                         datetime.utcnow().isoformat(), datetime.utcnow().isoformat(),
                         round(profit_usd, 4)),
                    )

                    # Mark original trade as cashed out (NOT 'win' — only Settlement sets win/loss)
                    if sell_pct >= 1.0:
                        engine.execute(
                            "UPDATE trades SET result = 'cashout', pnl = ? WHERE id = ?",
                            (round(profit_usd, 4), pos["id"]),
                        )

                    self.log("info", f"Cashout done: SELL {sell_shares:.1f} shares @ ${current_price:.4f} (Profit: +${profit_usd:.2f})")

                    # Only send Telegram for full cashout (not partial sells)
                    if sell_pct >= 1.0:
                        try:
                            from services.telegram_alerts import get_alerts
                            alerts = get_alerts(config)
                            alerts.send(
                                f"💰 <b>Cashout abgeschlossen!</b>\n"
                                f"Markt: {pos.get('market_question', market_id)[:60]}\n"
                                f"Seite: {pos['side']} | Entry: {entry_price:.4f} → Sell: {current_price:.4f}\n"
                                f"Anteile: {shares:.1f} | Profit: +${profit_usd:.2f} ({profit_pct:.1f}%)"
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
    # Stop-Loss Management
    # ------------------------------------------------------------------

    def _check_stop_losses(self) -> int:
        """Check all open positions for stop-loss conditions.

        Three triggers:
        1. Percentage stop-loss: PnL <= stop_loss_pct (default -50%)
        2. Time-based: position older than max_hold_days_losing AND losing
        3. Dead market: bid <= dead_market_bid for > dead_market_hours
        """
        from config import AppConfig as _AC, load_platform_config

        platform_cfg = load_platform_config()
        rm_cfg = platform_cfg.get("risk_management", {})
        stop_loss_pct = rm_cfg.get("stop_loss_pct", -50)
        max_hold_days_losing = rm_cfg.get("max_hold_days_losing", 14)
        dead_market_bid = rm_cfg.get("dead_market_bid", 0.002)
        dead_market_hours = rm_cfg.get("dead_market_hours", 48)

        positions = engine.query(
            "SELECT id, market_id, market_question, side, amount_usd, price, executed_at, agent_id "
            "FROM trades WHERE status = 'executed' AND (result = 'open' OR result IS NULL) "
            "AND price IS NOT NULL AND price > 0 ORDER BY executed_at"
        )
        if not positions:
            return 0

        # Load stop-loss exclusions from config
        _sl_exclude_agents = set()
        try:
            _sl_arb_cfg = platform_cfg.get("arbitrage", {})
            if _sl_arb_cfg.get("exclude_from_stoploss", True):
                _sl_exclude_agents.add("arb_v2")
                _sl_exclude_agents.add("arbitrage-scanner")
        except Exception:
            _sl_exclude_agents = {"arb_v2", "arbitrage-scanner"}

        stopped = 0
        for pos in positions:
            # Skip arbitrage positions — these are long-term holds until settlement
            pos_agent = pos.get("agent_id", "") or ""
            if pos_agent in _sl_exclude_agents:
                continue
            market_id = pos["market_id"]
            entry_price = pos["price"]
            pos_id = pos["id"]

            # Cooldown: skip if ANY failed stop-loss attempt within 24h
            # This catches DEAD-MARKET, STOP-LOSS-ATTEMPT, and any other failed stoploss
            _failed_sl_check = engine.query_one(
                "SELECT id FROM trades WHERE user_cmd = ? AND status = 'failed' AND executed_at > ?",
                (f"stoploss:{pos_id}", (datetime.utcnow() - timedelta(hours=24)).isoformat()),
            )
            if _failed_sl_check:
                continue

            # Skip penny positions (entry price <= $0.01) - essentially dead/worthless
            if entry_price <= 0.01:
                self.log("info", f"Skipping penny position (entry={entry_price}): {pos.get('market_question', '')[:50]}")
                continue

            # Get market data
            market = engine.query_one(
                "SELECT yes_price, no_price, yes_token_id, no_token_id "
                "FROM markets WHERE id = ?",
                (market_id,),
            )
            if not market:
                continue

            token_id = market.get("yes_token_id") if pos["side"] == "YES" else market.get("no_token_id")
            current_price = market.get("yes_price") if pos["side"] == "YES" else market.get("no_price")

            # Fetch live best-bid from CLOB
            best_bid = None
            if token_id:
                try:
                    from services.polymarket_client import PolymarketService
                    _svc = PolymarketService(_AC.from_env())
                    _book = _svc.get_order_book(token_id)
                    bids = _book.get("bids", []) if isinstance(_book, dict) else getattr(_book, "bids", []) or []
                    if bids:
                        bids = sorted(bids, key=lambda b: float(b.get("price", 0) if isinstance(b, dict) else getattr(b, "price", 0)), reverse=True)  # CLOB returns ascending
                        bid0 = bids[0]
                        best_bid = float(bid0.get("price", 0) if isinstance(bid0, dict) else getattr(bid0, "price", 0))
                        if best_bid > 0:
                            current_price = best_bid
                except Exception:
                    pass

            if not current_price or current_price <= 0:
                continue

            # If best_bid is None (CLOB error) and current_price is very low,
            # treat as dead market to prevent spam
            if best_bid is None and current_price <= 0.01:
                best_bid = 0.0  # Force dead market detection

            # Calculate PnL
            pnl_pct = ((current_price - entry_price) / entry_price) * 100
            shares = pos["amount_usd"] / entry_price
            pnl_usd = (current_price - entry_price) * shares

            # Calculate position age
            age_days = 0
            if pos.get("executed_at"):
                try:
                    age_days = (datetime.utcnow() - datetime.fromisoformat(pos["executed_at"])).total_seconds() / 86400
                except (ValueError, TypeError):
                    pass

            trigger = None
            telegram_msg = None
            market_name = pos.get("market_question", market_id)[:60]

            # --- Trigger 1: Percentage stop-loss ---
            if pnl_pct <= stop_loss_pct:
                trigger = "pct_stop"
                self.log("warn", f"STOP-LOSS (PCT): {market_name} PnL={pnl_pct:.1f}% <= {stop_loss_pct}%")
                telegram_msg = (
                    "\U0001f6d1 <b>Stop-Loss ausgeloest!</b>\n"
                    f"Markt: {market_name}\n"
                    f"Entry: {entry_price:.4f} \u2192 Bid: {current_price:.4f}\n"
                    f"PnL: {pnl_pct:.1f}% (${pnl_usd:.2f})"
                )

            # --- Trigger 2: Time-based stop-loss ---
            elif age_days > max_hold_days_losing and pnl_pct < 0:
                trigger = "time_stop"
                self.log("warn", f"STOP-LOSS (TIME): {market_name} age={age_days:.0f}d, PnL={pnl_pct:.1f}%")
                telegram_msg = (
                    "\u23f0 <b>Zeit-Stop-Loss!</b>\n"
                    f"Markt: {market_name}\n"
                    f"Alter: {age_days:.0f} Tage | PnL: {pnl_pct:.1f}% (${pnl_usd:.2f})\n"
                    f"Entry: {entry_price:.4f} \u2192 Bid: {current_price:.4f}"
                )

            # --- Trigger 3: Dead market detection ---
            elif best_bid is not None and best_bid <= dead_market_bid:
                # Check if bid has been dead for > dead_market_hours
                # Use position age as proxy (if bid is dead now AND position is old enough)
                if age_days * 24 > dead_market_hours:
                    trigger = "dead_market"
                    self.log("warn", f"DEAD MARKET: {market_name} bid={best_bid}, age={age_days:.0f}d")
                    telegram_msg = (
                        "\U0001f480 <b>Dead Market!</b>\n"
                        f"Markt: {market_name}\n"
                        f"Kein Kaeufer seit >48h (bid={best_bid})\n"
                        f"Position als Verlust markiert (${pnl_usd:.2f})"
                    )

            if not trigger:
                continue

            # --- Redirect to dead-market handling if bid is too low ---
            # Even if pct_stop or time_stop triggered, if bid <= dead_market_bid
            # there is no point trying to sell. Use dead_market path with 24h cooldown.
            _is_dead = best_bid is not None and best_bid <= dead_market_bid
            if _is_dead and trigger != "dead_market":
                trigger = "dead_market"
                self.log("info", f"Redirecting {trigger} to dead_market handler (bid={best_bid})")
                telegram_msg = (
                    "\U0001f480 <b>Dead Market (Stop-Loss)!</b>\n"
                    f"Markt: {market_name}\n"
                    f"Bid={best_bid} zu niedrig zum Verkaufen\n"
                    f"Entry: {entry_price:.4f} | PnL: {pnl_pct:.1f}% (${pnl_usd:.2f})\n"
                    f"<i>Naechste Pruefung in 24h</i>"
                )
            config = _AC.from_env()

            if trigger == "dead_market":
                # Can not sell - no buyers. Don't close in DB (shares still exist on Polymarket).
                # Write a 24h cooldown record so we don't spam every cycle.
                engine.execute(
                    """INSERT INTO trades
                       (market_id, market_question, side, amount_usd, price, status, agent_id, user_cmd, created_at, executed_at, result, pnl)
                       VALUES (?, ?, ?, 0, 0, 'failed', ?, ?, ?, ?, 'failed', 0)""",
                    (market_id, f"DEAD-MARKET: {pos.get('market_question', '')[:40]}",
                     pos["side"], self.id, f"stoploss:{pos_id}",
                     datetime.utcnow().isoformat(), datetime.utcnow().isoformat()),
                )
                self.log("info", f"Dead market cooldown set (24h): {market_name} bid={best_bid}")
                stopped += 1

            else:
                # Try to sell
                if not token_id:
                    self.log("warn", f"No token_id for stop-loss sell: {market_name}")
                    continue

                # Do not try to sell if bid is too low (0.001 minimum)
                if best_bid is not None and best_bid <= 0.001:
                    self.log("warn", f"Bid too low to sell ({best_bid}), setting 24h cooldown: {market_name}")
                    # Don't close in DB - shares still exist on Polymarket. Set 24h cooldown.
                    engine.execute(
                        """INSERT INTO trades
                           (market_id, market_question, side, amount_usd, price, status, agent_id, user_cmd, created_at, executed_at, result, pnl)
                           VALUES (?, ?, ?, 0, 0, 'failed', ?, ?, ?, ?, 'failed', 0)""",
                        (market_id, f"DEAD-MARKET: {pos.get('market_question', '')[:40]}",
                         pos["side"], self.id, f"stoploss:{pos_id}",
                         datetime.utcnow().isoformat(), datetime.utcnow().isoformat()),
                    )
                    stopped += 1
                    # Send telegram notification before continuing
                    if telegram_msg:
                        try:
                            from services.telegram_alerts import get_alerts
                            alerts = get_alerts(config)
                            alerts.send(telegram_msg + "\n<i>(Bid zu niedrig, als Verlust markiert)</i>")
                        except Exception:
                            pass
                    continue

                sell_shares = round(shares, 2)
                sell_value_usd = round(sell_shares * current_price, 2)

                try:
                    if not config.polymarket_private_key:
                        self.log("info", f"[PAPER-STOPLOSS] Would sell {sell_shares:.1f} shares of {market_name}")
                        continue

                    from services.polymarket_client import PolymarketService
                    service = PolymarketService(config)
                    result = service.place_sell_order(token_id=token_id, amount=sell_shares)

                    if result.get("ok"):
                        # Record stop-loss trade
                        engine.execute(
                            """INSERT INTO trades
                               (market_id, market_question, side, amount_usd, price, status, agent_id, user_cmd, created_at, executed_at, result, pnl)
                               VALUES (?, ?, ?, ?, ?, 'executed', ?, ?, ?, ?, 'cashout', ?)""",
                            (market_id, f"STOP-LOSS ({trigger}): {pos.get('market_question', '')[:40]}",
                             pos["side"], -sell_value_usd, current_price,
                             self.id, f"stoploss:{pos_id}",
                             datetime.utcnow().isoformat(), datetime.utcnow().isoformat(),
                             round(pnl_usd, 4)),
                        )
                        # Mark original as cashed out (stop-loss is a type of cashout)
                        engine.execute(
                            "UPDATE trades SET result = 'cashout', pnl = ? WHERE id = ?",
                            (round(pnl_usd, 4), pos_id),
                        )
                        self.log("info", f"Stop-loss executed: SELL {sell_shares:.1f} shares @ ${current_price:.4f} (PnL: ${pnl_usd:.2f})")
                        stopped += 1
                    else:
                        self.log("warn", f"Stop-loss sell failed: {result.get('error', '?')}")
                        # Write cooldown record so we don't spam every 5 min
                        engine.execute(
                            """INSERT INTO trades
                               (market_id, market_question, side, amount_usd, price, status, agent_id, user_cmd, created_at, executed_at, result, pnl)
                               VALUES (?, ?, ?, 0, 0, 'failed', ?, ?, ?, ?, 'failed', 0)""",
                            (market_id, f"STOP-LOSS-ATTEMPT: {pos.get('market_question', '')[:40]}",
                             pos["side"], self.id, f"stoploss:{pos_id}",
                             datetime.utcnow().isoformat(), datetime.utcnow().isoformat()),
                        )

                except Exception as e:
                    self.log("error", f"Stop-loss Exception: {e}")
                    # Write cooldown record on exception too
                    try:
                        engine.execute(
                            """INSERT INTO trades
                               (market_id, market_question, side, amount_usd, price, status, agent_id, user_cmd, created_at, executed_at, result, pnl)
                               VALUES (?, ?, ?, 0, 0, 'failed', ?, ?, ?, ?, 'failed', 0)""",
                            (market_id, f"STOP-LOSS-ATTEMPT: {pos.get('market_question', '')[:40]}",
                             pos["side"], self.id, f"stoploss:{pos_id}",
                             datetime.utcnow().isoformat(), datetime.utcnow().isoformat()),
                        )
                    except Exception:
                        pass
                    continue

            # Send Telegram notification
            if telegram_msg:
                try:
                    from services.telegram_alerts import get_alerts
                    alerts = get_alerts(config)
                    alerts.send(telegram_msg)
                except Exception:
                    pass

        if stopped > 0:
            self.log("info", f"Stop-loss: {stopped} position(s) closed")
        return stopped

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
