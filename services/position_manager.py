"""
Position Manager - Polymarket as Single Source of Truth

Every 2 minutes:
1. Fetch ALL on-chain positions from Polymarket Data API
2. For each position with shares > 0:
   a. Market resolved? -> Record win/loss, send Telegram
   b. Not resolved -> Check real bid -> Apply SL/TP/Breakeven
   c. Exit triggered -> Sell + ONE Telegram notification
3. Reconcile: DB records without on-chain position -> mark as closed
"""

import os
import logging
import html
import time
from datetime import datetime, timedelta

from config import AppConfig, load_platform_config
from db import engine
from services.polymarket_client import PolymarketService
from services.telegram_alerts import get_alerts

logger = logging.getLogger(__name__)


class PositionManager:
    def __init__(self, config: AppConfig):
        self.app_config = config
        self.config = load_platform_config()  # dict for trading settings
        self.service = PolymarketService(self.app_config)
        self.alerts = get_alerts(self.app_config)
        self.funder = os.getenv("POLYMARKET_FUNDER", "")
        self._resolution_cache = {}  # condition_id -> result, cached per cycle
        self._sell_cooldowns = {}  # token_id -> datetime, cooldown after failed sell

    def run_cycle(self) -> dict:
        """Main loop. Called every 2 minutes by trader cycle."""
        stats = {"checked": 0, "settled": 0, "sold": 0, "errors": 0}

        if not self.funder:
            logger.warning("No POLYMARKET_FUNDER set, skipping position management")
            return stats

        # Step 1: Fetch on-chain positions
        try:
            onchain_positions = self._fetch_onchain_positions()
        except Exception as e:
            logger.error(f"Failed to fetch on-chain positions: {e}")
            return stats

        # Build set of active condition_ids from on-chain
        active_condition_ids = set()

        # Step 2: Process each on-chain position
        for pos in onchain_positions:
            stats["checked"] += 1
            try:
                condition_id = pos["condition_id"]
                active_condition_ids.add(condition_id)

                # 2a: LAYER 1 - Check redeemable from Data API (most reliable)
                if pos.get("redeemable"):
                    # Market resolved and we WON (redeemable=true means our side won)
                    resolution = {"resolved": True, "winning_side": pos["side"]}
                    logger.info(f"Market redeemable (Data API): {condition_id[:20]}... side={pos['side']}")
                    self._record_settlement(pos, resolution)
                    stats["settled"] += 1
                    continue

                # 2a: LAYER 2 - Check resolution via Gamma API
                resolution = self._check_resolution(condition_id, token_id=pos.get("token_id"))
                if resolution and resolution.get("resolved"):
                    self._record_settlement(pos, resolution)
                    stats["settled"] += 1
                    continue

                # 2b: Get live bid price
                token_id = pos["token_id"]
                best_bid = self._get_best_bid(token_id)
                if best_bid is None:
                    continue  # Can't evaluate without price

                # Skip if recent sell failed (10min cooldown)
                _cd = self._sell_cooldowns.get(pos.get("token_id"))
                if _cd and (datetime.utcnow() - _cd).total_seconds() < 600:
                    continue

                # 2c: Apply exit rules
                signal = self._apply_exit_rules(pos, best_bid)
                if signal:
                    success = self._execute_sell(pos, best_bid, signal)
                    if success:
                        stats["sold"] += 1

            except Exception as e:
                logger.error(f"Error processing position {pos.get('condition_id', '?')}: {e}")
                stats["errors"] += 1

        # Step 3: Reconcile DB - close records for positions no longer on-chain
        self._reconcile_db(active_condition_ids)

        if stats["settled"] > 0 or stats["sold"] > 0:
            logger.info(f"PositionManager: checked={stats['checked']}, settled={stats['settled']}, sold={stats['sold']}")

        return stats

    # -- Fetch on-chain positions -------------------------------------------

    def _fetch_onchain_positions(self) -> list:
        """Fetch all positions from Polymarket Data API and enrich with DB data."""
        raw_positions = self.service.get_user_positions(self.funder)
        positions = []

        for raw in raw_positions:
            try:
                # Data API returns various formats
                size = float(raw.get("size") or raw.get("currentValue") or 0)
                if size <= 0.01:  # Skip dust
                    continue

                condition_id = raw.get("conditionId") or raw.get("market", {}).get("conditionId") or ""
                if not condition_id:
                    continue

                # Token ID is in the "asset" field from the Data API
                token_id = raw.get("asset", "")
                # Outcome field gives direct YES/NO side
                outcome = raw.get("outcome", "")

                # Look up in our DB to get market info
                market = engine.query_one(
                    "SELECT id, question, yes_token_id, no_token_id, category FROM markets WHERE id = ?",
                    (condition_id,))

                if not market:
                    # Try matching by token_id
                    market = engine.query_one(
                        "SELECT id, question, yes_token_id, no_token_id, category FROM markets WHERE yes_token_id = ? OR no_token_id = ?",
                        (token_id, token_id))

                if not market:
                    continue  # Unknown market, skip

                # Determine side: prefer outcome field, fallback to token_id comparison
                side = "YES"
                if outcome.upper() in ("NO", "N"):
                    side = "NO"
                elif outcome.upper() in ("YES", "Y"):
                    side = "YES"
                elif token_id and market["no_token_id"] and token_id == market["no_token_id"]:
                    side = "NO"

                if not token_id:
                    token_id = market["yes_token_id"] if side == "YES" else market["no_token_id"]

                # Look up our DB trade record for entry price
                db_trade = engine.query_one(
                    """SELECT id, price, amount_usd, created_at, user_cmd
                       FROM trades WHERE market_id = ? AND side = ? AND status = 'executed'
                       AND (result IS NULL OR result = 'open')
                       ORDER BY created_at DESC LIMIT 1""",
                    (market["id"], side))

                if not db_trade:
                    # Fallback: check any executed trade (even settled/closed) for reference
                    db_trade = engine.query_one(
                        """SELECT id, price, amount_usd, created_at, user_cmd
                           FROM trades WHERE market_id = ? AND side = ?
                           AND status = 'executed'
                           ORDER BY created_at DESC LIMIT 1""",
                        (market["id"], side))
                    if not db_trade:
                        logger.warning(f"On-chain position with no DB trade: {market['id']} side={side} shares={size}")

                positions.append({
                    "condition_id": market["id"],
                    "token_id": token_id,
                    "side": side,
                    "shares": size,
                    "question": market["question"] or "",
                    "category": market.get("category") or "",
                    "entry_price": float(db_trade["price"]) if db_trade and db_trade["price"] else 0,
                    "amount_usd": float(db_trade["amount_usd"]) if db_trade and db_trade["amount_usd"] else 0,
                    "trade_id": db_trade["id"] if db_trade else None,
                    "trade_created": db_trade["created_at"] if db_trade else None,
                    "trade_source": db_trade["user_cmd"] if db_trade else None,
                    # Data API enrichment for resolution detection
                    "redeemable": bool(raw.get("redeemable", False)),
                    "cur_price": float(raw.get("curPrice") or 0),
                })
            except Exception as e:
                logger.debug(f"Skipping position: {e}")
                continue

        return positions

    # -- Resolution check ---------------------------------------------------

    def _check_resolution(self, condition_id: str, token_id: str = "") -> dict | None:
        """Check if market is resolved. Cached per cycle."""
        if condition_id in self._resolution_cache:
            return self._resolution_cache[condition_id]

        try:
            result = self.service.get_market_resolution(condition_id, token_id=token_id)
            self._resolution_cache[condition_id] = result
            return result
        except Exception as e:
            logger.debug(f"Resolution check failed for {condition_id}: {e}")
            self._resolution_cache[condition_id] = None
            return None

    # -- Get best bid -------------------------------------------------------

    def _get_best_bid(self, token_id: str) -> float | None:
        """Get the current best bid price from CLOB orderbook."""
        try:
            book = self.service.get_order_book(token_id)
            # Handle both dict and OrderBookSummary dataclass
            if isinstance(book, dict):
                bids = book.get("bids", [])
            else:
                bids = getattr(book, "bids", []) or []
            if not bids:
                return 0.0  # No bids = effectively 0
            # Bids may be dicts or OrderSummary dataclasses
            def _price(b):
                if isinstance(b, dict):
                    return float(b.get("price", 0))
                return float(getattr(b, "price", 0))
            best = max(_price(b) for b in bids)
            return best
        except Exception as e:
            logger.debug(f"Orderbook fetch failed for {token_id}: {e}")
            return None

    # -- Exit rules ---------------------------------------------------------

    def _apply_exit_rules(self, pos: dict, best_bid: float) -> dict | None:
        """Apply all exit rules. Returns signal dict or None."""
        entry_price = pos["entry_price"]
        if entry_price <= 0:
            return None  # Can't evaluate without entry price

        trade_created = pos.get("trade_created")
        question = pos.get("question", "")

        # Calculate current PnL
        pnl_pct = (best_bid - entry_price) / entry_price * 100
        pnl_usd = (best_bid - entry_price) * pos["shares"]

        # Load config
        cashout_cfg = self.config.get("trading", {}).get("cashout", {})
        risk_cfg = self.config.get("risk_management", {})
        ws_cfg = self.config.get("weather_sniper", {})

        stop_loss_pct = risk_cfg.get("stop_loss_pct", -25)
        max_hold_days = risk_cfg.get("max_hold_days_losing", 7)
        dead_bid = risk_cfg.get("dead_market_bid", 0.005)
        dead_hours = risk_cfg.get("dead_market_hours", 24)

        # Age calculation
        age_hours = 0
        if trade_created:
            try:
                created_dt = datetime.strptime(trade_created[:19], "%Y-%m-%d %H:%M:%S")
                age_hours = (datetime.utcnow() - created_dt).total_seconds() / 3600
            except:
                pass

        # -- Rule 1: Dead Market (no liquidity) --
        if best_bid <= dead_bid and age_hours > dead_hours:
            return {
                "type": "dead_market",
                "reason": f"Bid={best_bid:.4f}, kein Kaeufer seit {age_hours:.0f}h",
                "pnl_pct": -100,
                "pnl_usd": -pos["amount_usd"],
                "skip_sell": True,  # No point trying to sell
            }

        # -- Rule 2: Take Profit (tiered) --
        tp_pct = self._get_tiered_tp(entry_price, cashout_cfg)
        min_profit_usd = cashout_cfg.get("min_profit_usd", 0.05)

        if pnl_pct >= tp_pct and pnl_usd >= min_profit_usd:
            return {
                "type": "take_profit",
                "reason": f"TP {pnl_pct:.1f}% >= {tp_pct:.1f}%",
                "pnl_pct": pnl_pct,
                "pnl_usd": pnl_usd,
            }

        # -- Rule 3: Breakeven Stop (weather trades only) --
        if ws_cfg.get("breakeven_enabled", False):
            is_weather = any(kw in question.lower() for kw in
                ["temperature", "\u00b0f", "\u00b0c", "\u00baf", "\u00bac", "rainfall", "snowfall", "hurricane"])

            if is_weather:
                be_trigger = ws_cfg.get("breakeven_trigger_pct", 66) / 100.0
                be_sl = ws_cfg.get("breakeven_sl_pct", 1) / 100.0

                tp_distance = entry_price * (tp_pct / 100.0)
                trigger_price = entry_price + tp_distance * be_trigger

                if best_bid >= trigger_price or (pnl_pct > 0 and pnl_pct >= tp_pct * be_trigger):
                    # Price reached 66% of TP -- tighten SL to breakeven
                    if pnl_pct <= -(be_sl * 100):
                        return {
                            "type": "breakeven_stop",
                            "reason": f"Preis erreichte {ws_cfg.get('breakeven_trigger_pct', 66)}% des TP, fiel zurueck",
                            "pnl_pct": pnl_pct,
                            "pnl_usd": pnl_usd,
                        }

        # -- Rule 4: Stop Loss (percentage) --
        # LAYER 3: If best_bid is 0 or near 0, skip stop-loss.
        # This means the market is either resolved (won/lost) or truly dead.
        # Resolved markets are handled by Layer 1 (redeemable) and Layer 2 (Gamma).
        # Don't let a $0 bid falsely trigger stop-loss on a won market.
        if pnl_pct <= stop_loss_pct:
            if best_bid < 0.01:
                logger.info(f"Skipping stop-loss for {pos.get('condition_id', '?')[:20]}... "
                            f"bid={best_bid:.4f} (likely resolved or dead, handled elsewhere)")
                return None
            return {
                "type": "stop_loss",
                "reason": f"SL {pnl_pct:.1f}% <= {stop_loss_pct}%",
                "pnl_pct": pnl_pct,
                "pnl_usd": pnl_usd,
            }

        # -- Rule 5: Time Stop (max hold with loss) --
        if age_hours > max_hold_days * 24 and pnl_pct < 0:
            return {
                "type": "time_stop",
                "reason": f"Gehalten {age_hours/24:.0f} Tage im Minus",
                "pnl_pct": pnl_pct,
                "pnl_usd": pnl_usd,
            }

        # -- Rule 6: Max Hold (force sell even if profitable) --
        max_hold = cashout_cfg.get("max_hold_hours", 120)
        force_sell_pct = cashout_cfg.get("force_sell_profit_pct", 2)
        if age_hours > max_hold and pnl_pct >= force_sell_pct:
            return {
                "type": "force_sell",
                "reason": f"Max Hold {max_hold}h erreicht, Profit {pnl_pct:.1f}%",
                "pnl_pct": pnl_pct,
                "pnl_usd": pnl_usd,
            }

        return None  # No exit signal

    def _get_tiered_tp(self, entry_price: float, cashout_cfg: dict) -> float:
        """Get take-profit percentage based on entry price tier."""
        tiers = cashout_cfg.get("tiered_targets", [])
        for tier in tiers:
            pr = tier.get("price_range", [0, 1])
            if len(pr) == 2 and pr[0] <= entry_price < pr[1]:
                return float(tier.get("profit_target", 5))
        return float(cashout_cfg.get("min_profit_pct", 5))

    # -- Execute sell -------------------------------------------------------

    def _execute_sell(self, pos: dict, best_bid: float, signal: dict) -> bool:
        """Sell position and record result. Returns True on success."""
        trade_id = pos.get("trade_id")
        question = html.escape(pos.get("question", "?")[:70])
        side = pos["side"]
        entry_price = pos["entry_price"]
        shares = pos["shares"]
        signal_type = signal["type"]
        pnl_pct = signal.get("pnl_pct", 0)
        pnl_usd = signal.get("pnl_usd", 0)

        sold = False

        # Dead market: don't try to sell, just close in DB
        if signal.get("skip_sell"):
            self._close_trade_in_db(trade_id, signal_type, pnl_usd)
            self._send_dead_market_alert(pos, signal)
            return True

        # Try to sell
        try:
            sell_amount = round(shares, 2)
            if sell_amount < 0.01:
                self._close_trade_in_db(trade_id, signal_type, pnl_usd)
                return True

            result = self.service.place_sell_order(pos["token_id"], sell_amount)

            if result.get("ok"):
                # SAFETY CHECK: Verify position is actually gone from Polymarket
                import time as _time
                _time.sleep(2)  # Wait for on-chain settlement
                still_there = self._verify_position_gone(pos["token_id"])
                if still_there:
                    logger.warning(
                        f"SAFETY: Sell reported OK but position still on-chain! "
                        f"{pos['condition_id'][:30]} token={pos['token_id'][:20]}... "
                        f"shares_remaining={still_there:.2f}. NOT closing DB."
                    )
                    self._sell_cooldowns[pos["token_id"]] = datetime.utcnow()
                    sold = False
                else:
                    sold = True
                    actual_pnl = (best_bid - entry_price) * shares if entry_price > 0 else 0
                    self._close_trade_in_db(trade_id, signal_type, actual_pnl)
                    self._send_exit_alert(pos, best_bid, signal, sold=True)
            else:
                # Sell FAILED - do NOT close DB, do NOT alert. Retry next cycle.
                error_msg = result.get("error", "?")
                logger.warning(f"Sell failed for {pos['condition_id']}: {error_msg}")
                self._sell_cooldowns[pos["token_id"]] = datetime.utcnow()
                sold = False

        except Exception as e:
            logger.error(f"Sell exception for {pos['condition_id']}: {e}")
            self._sell_cooldowns[pos.get("token_id", "")] = datetime.utcnow()
            sold = False

        return sold

    # -- Record settlement --------------------------------------------------

    def _record_settlement(self, pos: dict, resolution: dict):
        """Record a resolved market as win or loss."""
        trade_id = pos.get("trade_id")
        if not trade_id:
            return  # No DB record to update

        # Already settled?
        existing = engine.query_one("SELECT result FROM trades WHERE id = ? AND status = 'closed'", (trade_id,))
        if existing:
            return  # Already processed

        winning_side = resolution.get("winning_side", "")
        our_side = pos["side"]
        entry_price = pos["entry_price"]
        amount = pos["amount_usd"]
        shares = pos["shares"]

        if our_side == winning_side:
            result = "win"
            pnl = shares * 1.0 - amount if amount > 0 else 0  # Each share pays $1
        else:
            result = "loss"
            pnl = -amount

        self._close_trade_in_db(trade_id, result, pnl)

        # Update circuit breaker
        self._update_circuit_breaker(result)

        # Telegram
        emoji = "\U0001f3c6" if result == "win" else "\u274c"
        question = html.escape(pos.get("question", "?")[:70])
        msg = (
            f"{emoji} <b>Settlement: {'Gewonnen' if result == 'win' else 'Verloren'}!</b>\n"
            f"Markt: {question}\n"
            f"Seite: {our_side} | Gewinner: {winning_side}\n"
            f"PnL: ${pnl:+.2f}"
        )
        try:
            self.alerts.send(msg)
        except Exception as e:
            logger.debug(f"Telegram alert failed: {e}")

    # -- Reconcile DB -------------------------------------------------------

    def _reconcile_db(self, active_condition_ids: set):
        """Close DB records for positions no longer on-chain."""
        db_open = engine.query(
            """SELECT id, market_id, side, amount_usd, price, created_at
               FROM trades WHERE status = 'executed' AND (result IS NULL OR result = 'open')""")

        for trade in db_open:
            market_id = trade["market_id"]
            if market_id in active_condition_ids:
                continue  # Still on-chain, nothing to do

            # Position is gone from chain. Check age (only reconcile if > 30 min old)
            try:
                created = datetime.strptime(trade["created_at"][:19], "%Y-%m-%d %H:%M:%S")
                if (datetime.utcnow() - created).total_seconds() < 1800:
                    continue  # Too new, might not be indexed yet
            except:
                continue

            # Check if market is resolved
            resolution = self._check_resolution(market_id)
            if resolution and resolution.get("resolved"):
                # Reconstruct position info for settlement
                pos = {
                    "condition_id": market_id,
                    "side": trade["side"],
                    "shares": float(trade["amount_usd"]) / float(trade["price"]) if trade["price"] and float(trade["price"]) > 0 else 0,
                    "entry_price": float(trade["price"] or 0),
                    "amount_usd": float(trade["amount_usd"] or 0),
                    "trade_id": trade["id"],
                    "question": "",
                }
                # Get question
                market = engine.query_one("SELECT question FROM markets WHERE id = ?", (market_id,))
                pos["question"] = market["question"] if market else ""

                self._record_settlement(pos, resolution)
            else:
                # Not resolved but gone from chain -- sold externally or dust
                # Only close if > 2 hours old
                try:
                    if (datetime.utcnow() - created).total_seconds() > 7200:
                        engine.execute(
                            "UPDATE trades SET status = 'closed', result = 'sold_external' WHERE id = ?",
                            (trade["id"],))
                        logger.info(f"Closed trade {trade['id']} - position gone from chain (sold externally)")
                except:
                    pass

    # -- Helper: verify position gone from chain -----------------------------

    def _verify_position_gone(self, token_id: str) -> float:
        """Check if position still exists on-chain after sell. Returns remaining shares or 0."""
        try:
            import httpx
            resp = httpx.get(
                f"https://data-api.polymarket.com/positions?user={self.funder}",
                timeout=10,
            )
            if resp.status_code == 200:
                for pos in resp.json():
                    if pos.get("asset") == token_id:
                        remaining = float(pos.get("size") or 0)
                        if remaining > 0.01:
                            return remaining
            return 0.0
        except Exception as e:
            logger.debug(f"Position verification failed: {e}")
            return 0.0  # Can't verify = assume gone (fallback)

    # -- Helper: close trade in DB ------------------------------------------

    def _close_trade_in_db(self, trade_id: int | None, result: str, pnl: float):
        """Mark a trade as closed in the database."""
        if not trade_id:
            return
        engine.execute(
            """UPDATE trades SET status = 'closed', result = ?, pnl = ?, executed_at = datetime('now')
               WHERE id = ? AND status != 'closed'""",
            (result, round(pnl, 4), trade_id))

    # -- Helper: circuit breaker --------------------------------------------

    def _update_circuit_breaker(self, result: str):
        """Update circuit breaker on settlement."""
        if result != "loss":
            return
        try:
            recent = engine.query(
                """SELECT result FROM trades WHERE status = 'closed' AND result IN ('win', 'loss')
                   ORDER BY executed_at DESC LIMIT 10""")
            consecutive_losses = 0
            for r in recent:
                if r["result"] == "loss":
                    consecutive_losses += 1
                else:
                    break

            max_losses = self.config.get("risk_management", {}).get("max_consecutive_losses", 3)
            if consecutive_losses >= max_losses:
                pause_hours = self.config.get("risk_management", {}).get("pause_hours", 24)
                paused_until = datetime.utcnow() + timedelta(hours=pause_hours)
                engine.execute(
                    "INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)",
                    ("circuit_breaker_until", paused_until.isoformat()))
                logger.warning(f"Circuit breaker: {consecutive_losses} losses, paused until {paused_until}")
                self.alerts.send(
                    f"\U0001f6a8 <b>Circuit Breaker!</b>\n{consecutive_losses} Verluste in Folge\nPause bis {paused_until.strftime('%H:%M UTC')}")
        except Exception as e:
            logger.debug(f"Circuit breaker update failed: {e}")

    # -- Telegram alerts ----------------------------------------------------

    def _send_exit_alert(self, pos: dict, best_bid: float, signal: dict, sold: bool):
        """Send ONE Telegram notification for an exit."""
        question = html.escape(pos.get("question", "?")[:70])
        signal_type = signal["type"]
        pnl_pct = signal.get("pnl_pct", 0)
        pnl_usd = signal.get("pnl_usd", 0)
        entry = pos["entry_price"]

        type_labels = {
            "take_profit": ("\U0001f4b0", "Cashout abgeschlossen!"),
            "stop_loss": ("\U0001f534", "Stop-Loss ausgeloest!"),
            "breakeven_stop": ("\U0001f6e1", "Breakeven-Stop ausgeloest!"),
            "time_stop": ("\u23f0", "Zeit-Stop-Loss!"),
            "force_sell": ("\u23f0", "Max Hold erreicht - verkauft!"),
        }
        emoji, title = type_labels.get(signal_type, ("\U0001f4ca", "Position geschlossen"))

        sell_status = "" if sold else "\n<i>Sell fehlgeschlagen, Position in DB geschlossen.</i>"

        msg = (
            f"{emoji} <b>{title}</b>\n"
            f"Markt: {question}\n"
            f"Seite: {pos['side']} | Entry: {entry:.4f} -> Bid: {best_bid:.4f}\n"
            f"Anteile: {pos['shares']:.1f} | PnL: {pnl_pct:+.1f}% (${pnl_usd:+.2f})"
            f"{sell_status}"
        )
        try:
            self.alerts.send(msg)
        except Exception as e:
            logger.debug(f"Telegram alert failed: {e}")

    def _send_dead_market_alert(self, pos: dict, signal: dict):
        """Send dead market notification."""
        question = html.escape(pos.get("question", "?")[:70])
        msg = (
            f"\U0001f480 <b>Position geschlossen (Dead Market)</b>\n"
            f"Markt: {question}\n"
            f"Kein Kaeufer verfuegbar\n"
            f"Verlust: ${pos['amount_usd']:.2f}\n"
            f"<i>Position in DB geschlossen, kein Retry.</i>"
        )
        try:
            self.alerts.send(msg)
        except Exception as e:
            logger.debug(f"Telegram alert failed: {e}")
