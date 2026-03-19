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
from services.position_manager import PositionManager

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
                # Position management (TP, SL, breakeven, resolution) via on-chain data
                try:
                    pm = PositionManager(AppConfig.from_env())
                    pm.run_cycle()
                except Exception as e:
                    self.log("error", f"PositionManager cycle failed: {e}")
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
            # Atomic claim: mark as processing before executing
            engine.execute(
                "UPDATE suggestions SET status = 'processing' WHERE id = ? AND status = 'auto_approved'",
                (s["id"],),
            )
            verify = engine.query_one("SELECT status FROM suggestions WHERE id = ?", (s["id"],))
            if not verify or verify["status"] != "processing":
                logger.info(f"Suggestion {s['id']} already claimed by another instance, skipping")
                continue

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
                # Mark as failed with reason so we can debug
                engine.execute(
                    "UPDATE suggestions SET status = 'failed', resolved_at = ?, user_response = ? WHERE id = ?",
                    (datetime.utcnow().isoformat(), "FAIL: trade execution returned False", s["id"]),
                )
        return count

    def _process_user_approved(self) -> int:
        """Process suggestions that the user approved via Dashboard."""
        suggestions = engine.query(
            "SELECT * FROM suggestions WHERE status = 'approved' AND type = 'trade' ORDER BY created_at"
        )
        count = 0
        for s in suggestions:
            # Atomic claim: mark as processing to prevent double execution
            rows = engine.execute(
                "UPDATE suggestions SET status = 'processing' WHERE id = ? AND status = 'approved'",
                (s["id"],),
            )
            if not rows:
                continue  # Another worker already claimed it

            payload = json.loads(s.get("payload") or "{}")
            if self._execute_trade(payload, source=f"suggestion:{s['id']}"):
                engine.execute(
                    "UPDATE suggestions SET status = 'executed', resolved_at = ? WHERE id = ?",
                    (datetime.utcnow().isoformat(), s["id"]),
                )
                count += 1
            else:
                # Trade failed — revert to approved so user can retry
                engine.execute(
                    "UPDATE suggestions SET status = 'approved' WHERE id = ? AND status = 'processing'",
                    (s["id"],),
                )
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

            # Check if an active NO-bias strategy exists for this market's category
            category = (m.get("category") or "").strip()
            no_strategy = None
            if category:
                no_strategy = engine.query_one(
                    "SELECT id FROM strategies WHERE status = 'active' "
                    "AND definition LIKE ? AND definition LIKE ?",
                    (f'%"category_filter"%{category}%', '%"side": "NO"%'),
                )
            if no_strategy:
                side = "NO"
            else:
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
            "AND status = 'closed' AND result IS NOT NULL",
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

        # 5a. Keyword blacklist
        keyword_blacklist = trading_cfg.get("keyword_blacklist", [])
        question = str(payload.get("question", "") or payload.get("market_question", "")).lower()
        for kw in keyword_blacklist:
            if kw.lower() in question:
                return False, f"Keyword '{kw}' ist gesperrt"

        # 5b. Re-buy cooldown: wait 7 days after closing a position in same market
        rebuy_cooldown_days = trading_cfg.get("rebuy_cooldown_days", 7)
        last_closed = engine.query_one(
            "SELECT MAX(executed_at) as last_close FROM trades WHERE market_id = ? "
            "AND status = 'closed' AND result IS NOT NULL",
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

            # SAFETY CHECK: Verify position does NOT already exist on Polymarket
            try:
                import os, httpx
                funder = os.getenv("POLYMARKET_FUNDER", "")
                if funder:
                    resp = httpx.get(
                        f"https://data-api.polymarket.com/positions?user={funder}",
                        timeout=10,
                    )
                    if resp.status_code == 200:
                        for pos in resp.json():
                            pos_token = pos.get("asset", "")
                            pos_size = float(pos.get("size") or 0)
                            if pos_token == token_id and pos_size > 0.01:
                                self._finalize_trade(trade_id, "failed", payload,
                                    error=f"Position already exists on-chain ({pos_size:.2f} shares)")
                                self.log("warn", f"SAFETY: Position existiert bereits on-chain für {market_id[:30]} ({pos_size:.2f} shares)")
                                return False
            except Exception as e:
                self.log("debug", f"On-chain position check failed (proceeding): {e}")

            # Pre-check: verify orderbook exists (with retry)
            book = None
            for _ob_attempt in range(2):
                try:
                    book = service.get_order_book(token_id)
                    break
                except Exception as e:
                    if _ob_attempt == 0:
                        self.log("warn", f"Orderbook fetch failed (attempt 1), retrying: {e}")
                        import time; time.sleep(1)
                    else:
                        self._finalize_trade(trade_id, "failed", payload, error=f"Orderbook check failed after retry: {e}")
                        self.log("warn", f"Orderbook check failed after retry for {token_id[:16]}... - {e}")
                        return False
            if not book or not getattr(book, "bids", None):
                self._finalize_trade(trade_id, "failed", payload, error="No orderbook/bids for token")
                self.log("warn", f"No orderbook/bids for token {token_id[:16]}... - skipping gracefully")
                return False

            result = service.place_market_order(
                token_id=token_id,
                amount=amount,
                side=side,
            )

            if result.get("ok"):
                # Log CLOB response for debugging
                self.log("debug", f"CLOB order response: {str(result.get('result', ''))[:200]}")

                # --- Get ACTUAL fill price from on-chain balance ---
                actual_entry_price = None
                try:
                    from py_clob_client.clob_types import BalanceAllowanceParams, AssetType
                    import time as _time
                    _time.sleep(1)  # brief wait for on-chain settlement
                    _bal_params = BalanceAllowanceParams(asset_type=AssetType.CONDITIONAL, token_id=token_id)
                    _bal_resp = service._auth_client.get_balance_allowance(_bal_params)
                    if _bal_resp and isinstance(_bal_resp, dict):
                        raw_bal = int(_bal_resp.get("balance", "0"))
                        actual_shares = raw_bal / 1e6  # Polymarket uses 6 decimals
                        if actual_shares > 0:
                            actual_entry_price = round(amount / actual_shares, 6)
                            # Sanity check: Polymarket prices are always 0-1
                            if actual_entry_price > 1.0 or actual_entry_price < 0.01:
                                self.log("warning", f"Invalid calc price {actual_entry_price:.4f} from {amount:.2f}/{actual_shares:.4f} - using suggestion price")
                                actual_entry_price = None
                            else:
                                self.log("info", f"Actual fill: {actual_shares:.2f} shares @ ${actual_entry_price:.4f} (on-chain)")
                except Exception as _e:
                    self.log("debug", f"Could not get on-chain balance for fill price: {_e}")

                # Finalize trade with actual price
                self._finalize_trade(trade_id, "executed", payload, actual_price=actual_entry_price)
                self.log("info", f"Trade ausgeführt: {side} ${amount:.2f} auf '{question[:50]}'")

                # Alert with full sniper details
                try:
                    import html as _html
                    from services.telegram_alerts import get_alerts
                    alerts = get_alerts(config)
                    _confidence = payload.get("confidence", 0)
                    _edge = payload.get("edge", 0)
                    _source = payload.get("source", payload.get("data_source", source))
                    _detail = payload.get("detail", "")
                    # Use actual fill price, fallback to payload price
                    _price = actual_entry_price
                    if not _price or _price <= 0:
                        _price = payload.get("yes_price") if side == "YES" else payload.get("no_price")
                    if not _price or _price <= 0:
                        _price = payload.get("price", 0)
                    _price = _price or 0
                    msg = (
                        f"\u26a1 <b>Trade Executed</b>\n"
                        f"Markt: {_html.escape(question[:80])}\n"
                        f"Seite: {side} @ {_price:.4f}\n"
                        f"Betrag: ${amount:.2f}\n"
                        f"Confidence: {_confidence:.0%} | Edge: {_edge:+.1%}\n"
                        f"Quelle: {_html.escape(str(_source))}\n"
                        f"Detail: {_html.escape(str(_detail)[:120])}"
                    )
                    alerts.send(msg)
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

    def _finalize_trade(self, trade_id: int, status: str, payload: dict, error: str = None, actual_price: float = None) -> None:
        """Update trade record with final status."""
        if error:
            engine.execute(
                "UPDATE trades SET status = ?, executed_at = ?, result = ? WHERE id = ?",
                (status, datetime.utcnow().isoformat(), f"error: {error[:200]}", trade_id),
            )
        else:
            engine.execute(
                "UPDATE trades SET status = ?, executed_at = ? WHERE id = ?",
                (status, datetime.utcnow().isoformat(), trade_id),
            )
        # Update price: prefer actual fill price > payload price > market price
        if status == "executed":
            trade = engine.query_one("SELECT price, market_id, side FROM trades WHERE id = ?", (trade_id,))
            if trade:
                side = trade.get("side", "YES")
                # Priority 1: actual fill price from on-chain balance
                price = actual_price if actual_price and actual_price > 0 else None
                # Priority 2: existing DB price (only if no actual price available)
                if not price or price <= 0:
                    existing = trade.get("price")
                    if existing and existing > 0:
                        price = existing
                # Priority 3: payload price
                if not price or price <= 0:
                    price = payload.get("yes_price") if side == "YES" else payload.get("no_price")
                # Priority 4: fallback from markets table
                if not price or price <= 0:
                    market = engine.query_one(
                        "SELECT yes_price, no_price FROM markets WHERE id = ?",
                        (trade.get("market_id"),),
                    )
                    if market:
                        price = market.get("yes_price") if side == "YES" else market.get("no_price")
                if price and price > 0:
                    engine.execute("UPDATE trades SET price = ? WHERE id = ?", (price, trade_id))

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
                if not hasattr(self, "_hedge_svc") or self._hedge_svc is None:
                    self._hedge_svc = PolymarketService(config)
                service = self._hedge_svc
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
