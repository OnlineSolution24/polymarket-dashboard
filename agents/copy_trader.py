"""
Copy Trader Agent — Monitors watched wallets for new trades and
copies selected positions automatically.

Reads copy trade selections from alpha_scanner_copytrades.json
and polls wallet activity for new trades matching those markets.
"""

import json
import logging
from datetime import datetime, timedelta, timezone

from agents.base_agent import BaseAgent
from config import AppConfig
from db import engine
from services.alpha_scanner_service import (
    load_copy_trades,
    load_copytrading_config,
)
from services.data_api_client import DataAPIClient

logger = logging.getLogger(__name__)


class CopyTraderAgent(BaseAgent):
    """
    Polls watched wallets, detects new trades on tracked markets,
    and copies them with configurable amount.
    """

    def run_cycle(self) -> dict:
        """One copy-trading cycle: check config → poll wallets → execute copies."""
        # Check if bot is paused
        try:
            from bot_main import bot_state
            if bot_state.paused:
                return {"ok": True, "summary": "Bot paused, skipping"}
        except ImportError:
            pass

        ct_config = load_copytrading_config()
        if not ct_config.get("enabled", False):
            return {"ok": True, "summary": "Copy trading disabled"}

        copy_trades = load_copy_trades()
        if not copy_trades:
            return {"ok": True, "summary": "No copy trades configured"}

        mode = ct_config.get("mode", "paper")
        amount = ct_config.get("amount_per_trade", 1.0)
        max_daily = ct_config.get("max_daily_trades", 10)
        max_daily_amount = ct_config.get("max_daily_amount", 20.0)

        self.log("debug", f"Copy trader cycle (mode={mode}, trades={len(copy_trades)})")

        # Check daily limits
        today = datetime.utcnow().strftime("%Y-%m-%d")
        daily_stats = self._get_daily_stats(today)
        if daily_stats["count"] >= max_daily:
            self.log("info", f"Daily trade limit reached ({max_daily})")
            return {"ok": True, "summary": f"Daily limit reached: {daily_stats['count']}/{max_daily}"}
        if daily_stats["amount"] >= max_daily_amount:
            self.log("info", f"Daily amount limit reached (${max_daily_amount})")
            return {"ok": True, "summary": f"Daily amount limit: ${daily_stats['amount']:.2f}/${max_daily_amount}"}

        # Group copy trades by wallet
        by_wallet: dict[str, list[dict]] = {}
        for ct in copy_trades:
            by_wallet.setdefault(ct["wallet_address"], []).append(ct)

        client = DataAPIClient(timeout=20)
        executed = 0

        try:
            for wallet_addr, trades in by_wallet.items():
                if daily_stats["count"] + executed >= max_daily:
                    break

                new_trades = self._check_wallet_for_new_trades(
                    client, wallet_addr, trades
                )

                for trade_info in new_trades:
                    if daily_stats["count"] + executed >= max_daily:
                        break
                    if daily_stats["amount"] + (executed * amount) >= max_daily_amount:
                        break

                    success = self._execute_copy(trade_info, amount, mode)
                    if success:
                        executed += 1

        finally:
            client.close()

        summary = f"Copy trader: {executed} trades executed (mode={mode})"
        self.log("info", summary)
        return {"ok": True, "summary": summary}

    def _check_wallet_for_new_trades(
        self,
        client: DataAPIClient,
        wallet_address: str,
        tracked_trades: list[dict],
    ) -> list[dict]:
        """Check if a wallet has made new trades on tracked markets."""
        # Get recent activity (last 10 minutes)
        ten_min_ago = int(
            (datetime.now(timezone.utc) - timedelta(minutes=10)).timestamp()
        )
        activities = client.get_user_activity(
            wallet_address, activity_type="TRADE", start=ten_min_ago, limit=50
        )

        if not activities:
            return []

        # Map condition_ids we're tracking for this wallet
        tracked_conditions = {t["condition_id"]: t for t in tracked_trades}

        new_trades = []
        for act in activities:
            cond_id = act.get("conditionId", act.get("condition_id", ""))
            if cond_id in tracked_conditions:
                # Check if we already copied this specific activity
                act_ts = act.get("timestamp", "")
                dedup_key = f"copy_{wallet_address[:10]}_{cond_id[:10]}_{act_ts}"
                if self._is_already_copied(dedup_key):
                    continue

                tracked = tracked_conditions[cond_id]
                new_trades.append({
                    "wallet_address": wallet_address,
                    "wallet_name": tracked.get("wallet_name", wallet_address[:12]),
                    "market_title": tracked.get("market_title", ""),
                    "condition_id": cond_id,
                    "outcome": act.get("outcome", tracked.get("outcome", "YES")),
                    "side": (act.get("side") or "BUY").upper(),
                    "dedup_key": dedup_key,
                    "source_size": float(act.get("size", 0) or 0),
                })

        return new_trades

    def _execute_copy(self, trade_info: dict, amount: float, mode: str) -> bool:
        """Execute a copy trade."""
        condition_id = trade_info["condition_id"]
        side = trade_info.get("outcome", "YES")
        market_title = trade_info["market_title"]
        wallet_name = trade_info["wallet_name"]
        dedup_key = trade_info["dedup_key"]

        self.log(
            "info",
            f"[COPY] {wallet_name} → {side} ${amount:.2f} auf '{market_title[:50]}'"
        )

        # Record the copy trade
        engine.execute(
            """INSERT INTO trades
               (market_id, market_question, side, amount_usd, price, status,
                agent_id, user_cmd, created_at, executed_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                condition_id,
                f"[COPY:{wallet_name[:20]}] {market_title[:80]}",
                side,
                amount,
                0,
                "paper" if mode == "paper" else "executing",
                self.id,
                f"copy_trade|{dedup_key}",
                datetime.utcnow().isoformat(),
                datetime.utcnow().isoformat() if mode == "paper" else None,
            ),
        )

        if mode == "paper":
            self.log("info", f"[PAPER COPY] {side} ${amount:.2f} — {market_title[:50]}")
            self._mark_copied(dedup_key)
            return True

        # Live mode: execute real trade
        try:
            config = AppConfig.from_env()
            if not config.polymarket_private_key:
                self.log("warn", "Kein Private Key — Paper Trade statt Live")
                self._mark_copied(dedup_key)
                return True

            from services.polymarket_client import PolymarketService
            service = PolymarketService(config)

            # Look up token IDs from markets table
            market_row = engine.query_one(
                "SELECT yes_token_id, no_token_id FROM markets WHERE id = ?",
                (condition_id,),
            )
            if not market_row:
                self.log("warn", f"Market {condition_id[:20]} nicht in DB — überspringe")
                self._mark_copied(dedup_key)
                return False

            token_id = (
                market_row.get("yes_token_id")
                if side == "YES"
                else market_row.get("no_token_id")
            )
            if not token_id:
                self.log("warn", f"Kein Token-ID für {condition_id[:20]} side={side}")
                self._mark_copied(dedup_key)
                return False

            result = service.place_market_order(
                token_id=token_id, amount=amount, side=side
            )

            if result.get("ok"):
                trade_id = engine.query_one(
                    "SELECT last_insert_rowid() as id"
                )["id"]
                engine.execute(
                    "UPDATE trades SET status = 'executed', executed_at = ? WHERE id = ?",
                    (datetime.utcnow().isoformat(), trade_id),
                )
                self.log("info", f"[LIVE COPY] Trade ausgeführt: {side} ${amount:.2f}")

                # Telegram alert
                try:
                    from services.telegram_alerts import get_alerts
                    alerts = get_alerts(config)
                    alerts.alert_trade_executed(
                        f"[COPY:{wallet_name[:15]}] {market_title[:40]}",
                        side,
                        amount,
                    )
                except Exception:
                    pass

                self._mark_copied(dedup_key)
                return True
            else:
                error = result.get("error", "Unknown")
                self.log("error", f"Copy trade fehlgeschlagen: {error}")
                return False

        except Exception as e:
            self.log("error", f"Copy trade Exception: {e}")
            return False

    def _get_daily_stats(self, date_str: str) -> dict:
        """Get today's copy trade stats."""
        row = engine.query_one(
            """SELECT COUNT(*) as count, COALESCE(SUM(amount_usd), 0) as amount
               FROM trades
               WHERE agent_id = ? AND created_at LIKE ?""",
            (self.id, f"{date_str}%"),
        )
        return {
            "count": row["count"] if row else 0,
            "amount": float(row["amount"]) if row else 0,
        }

    def _is_already_copied(self, dedup_key: str) -> bool:
        """Check if we already copied this trade."""
        row = engine.query_one(
            "SELECT id FROM trades WHERE user_cmd LIKE ?",
            (f"%{dedup_key}%",),
        )
        return row is not None

    def _mark_copied(self, dedup_key: str) -> None:
        """Mark a trade as copied (dedup_key is already in user_cmd via INSERT)."""
        pass
