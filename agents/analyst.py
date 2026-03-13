"""
Analyst Agent - Deep market analysis and edge calculation.
Uses order book data, price momentum, and volume signals for richer analysis.
When edge AND confidence are high enough, creates trade suggestions.
"""

import json
from datetime import datetime

from agents.base_agent import BaseAgent
from config import load_platform_config
from db import engine


class AnalystAgent(BaseAgent):
    """Performs deep analysis on markets flagged by the Observer."""

    def run_cycle(self) -> dict:
        self.log("info", "Analyst cycle gestartet")

        try:
            # Get high-volume, active markets with all available signals
            try:
                markets = engine.query(
                    """SELECT * FROM markets
                       WHERE volume > 10000 AND accepting_orders = 1
                       ORDER BY volume DESC LIMIT 10"""
                )
            except Exception:
                # Fallback if accepting_orders column doesn't exist yet
                markets = engine.query(
                    """SELECT * FROM markets
                       WHERE volume > 10000
                       ORDER BY volume DESC LIMIT 10"""
                )

            if not markets:
                self.log("info", "Keine Märkte für Analyse verfügbar.")
                return {"ok": True, "summary": "No markets to analyze"}

            analyzed = 0
            for market in markets[:5]:
                momentum = self._get_price_momentum(market["id"])

                # Build rich analysis prompt with all available signals
                lines = [
                    f"Tiefenanalyse für Polymarket-Markt:",
                    f"Frage: {market['question']}",
                ]

                # Market description contains resolution rules and context
                desc = market.get("description", "")
                if desc:
                    lines.append(f"Markt-Regeln & Kontext: {desc[:500]}")

                if market.get("end_date"):
                    lines.append(f"Ablaufdatum: {market['end_date']}")
                if market.get("category"):
                    lines.append(f"Kategorie: {market['category']}")

                lines.extend([
                    f"YES Preis: {market['yes_price']:.2f} (={market['yes_price']*100:.0f}%)",
                    f"NO Preis: {market['no_price']:.2f}",
                    f"Volumen: ${market['volume']:,.0f}",
                    f"Liquidität: ${market['liquidity']:,.0f}",
                ])

                # Order book signals
                if market.get("spread") is not None:
                    lines.append(f"Spread: {market['spread']:.4f}")
                if market.get("book_imbalance") is not None:
                    lines.append(f"Book Imbalance: {market['book_imbalance']:.4f} (positiv=Kaufdruck)")
                if market.get("bid_depth") and market.get("ask_depth"):
                    lines.append(f"Bid Depth: {market['bid_depth']:.0f} | Ask Depth: {market['ask_depth']:.0f}")

                # Volume momentum
                if market.get("volume_24h"):
                    lines.append(f"Volumen 24h: ${market['volume_24h']:,.0f}")
                if market.get("volume_1w"):
                    lines.append(f"Volumen 1W: ${market['volume_1w']:,.0f}")

                # Sentiment
                if market.get("sentiment_score") is not None:
                    lines.append(f"Sentiment Score: {market['sentiment_score']:.2f}")

                # Whale / Smart Money signals
                if market.get("whale_buy_count") or market.get("whale_sell_count"):
                    lines.append(f"Whale Buys: {market.get('whale_buy_count', 0)} | Whale Sells: {market.get('whale_sell_count', 0)}")
                    lines.append(f"Whale Net Flow: ${market.get('whale_net_flow', 0):,.0f}")
                if market.get("smart_money_score") is not None:
                    lines.append(f"Smart Money Score: {market['smart_money_score']:.1f}/100")
                if market.get("top_holder_concentration") is not None:
                    lines.append(f"Top-5 Holder Konzentration: {market['top_holder_concentration']:.1%}")
                if market.get("open_interest") is not None:
                    lines.append(f"Open Interest: ${market['open_interest']:,.0f}")
                if market.get("oi_change_24h") is not None:
                    lines.append(f"OI Veränderung 24h: {market['oi_change_24h']:+.1%}")

                # Price momentum from snapshots
                for label, key in [("1h", "change_1h"), ("6h", "change_6h"), ("24h", "change_24h")]:
                    val = momentum.get(key)
                    if val is not None:
                        lines.append(f"Preis-Momentum {label}: {val:+.4f}")

                lines.extend([
                    "",
                    "1. Was ist deine Einschätzung der wahren Wahrscheinlichkeit?",
                    "2. Gibt es einen Edge (Differenz Marktpreis vs. wahre Wahrscheinlichkeit)?",
                    "3. Order Book Signale: deutet die Imbalance eine Richtung an?",
                    "4. Whale/Smart-Money: was sagen die grossen Spieler (Kaufdruck vs Verkaufsdruck)?",
                    "5. Open Interest Trend: steigt oder fällt das Interesse?",
                    "6. Konfidenz-Level: niedrig/mittel/hoch?",
                    "Antworte mit: EDGE=+X.XX oder EDGE=-X.XX und KONFIDENZ=niedrig/mittel/hoch",
                ])

                response = self.think("\n".join(lines))

                if response:
                    edge = self._extract_edge(response)
                    confidence = self._extract_confidence(response)
                    if edge is not None:
                        engine.execute(
                            "UPDATE markets SET calculated_edge = ?, last_updated = datetime('now') WHERE id = ?",
                            (edge, market["id"]),
                        )
                    self.log("info", f"Analyse für '{market['question'][:40]}': Edge={edge}, Konfidenz={confidence}")

                    # Create trade suggestion if edge + confidence are high enough
                    if edge is not None and confidence == "hoch":
                        self._maybe_create_suggestion(market, edge, confidence, response)

                    analyzed += 1

            return {"ok": True, "summary": f"Analyzed {analyzed} markets"}

        except Exception as e:
            self.log("error", f"Analyst cycle fehlgeschlagen: {e}")
            return {"ok": False, "summary": str(e)}

    def _get_price_momentum(self, market_id: str) -> dict:
        """Compute price changes from snapshots."""
        result = {}
        current = engine.query_one("SELECT yes_price FROM markets WHERE id = ?", (market_id,))
        if not current or not current.get("yes_price"):
            return result

        for label, interval in [("change_1h", "-1 hour"), ("change_6h", "-6 hours"), ("change_24h", "-24 hours")]:
            snap = engine.query_one(
                "SELECT yes_price FROM market_snapshots WHERE market_id = ? "
                "AND snapshot_at <= datetime('now', ?) ORDER BY snapshot_at DESC LIMIT 1",
                (market_id, interval),
            )
            if snap and snap.get("yes_price"):
                result[label] = round(current["yes_price"] - snap["yes_price"], 4)
        return result

    def _maybe_create_suggestion(self, market: dict, edge: float, confidence: str, analysis: str) -> None:
        """Create a trade suggestion if edge is high enough and all safety checks pass."""
        platform_cfg = load_platform_config()
        trading_cfg = platform_cfg.get("trading", {})
        analyst_cfg = trading_cfg.get("analyst_suggestions", {})

        min_edge = analyst_cfg.get("min_edge", 0.20)  # 20% default — very conservative
        min_liquidity = analyst_cfg.get("min_liquidity", 50000)
        max_amount = analyst_cfg.get("max_amount_usd", 3.0)

        abs_edge = abs(edge)
        if abs_edge < min_edge:
            return

        # Liquidity check
        liquidity = market.get("liquidity", 0) or 0
        if liquidity < min_liquidity:
            self.log("debug", f"Skip suggestion: liquidity ${liquidity:,.0f} < ${min_liquidity:,.0f}")
            return

        market_id = market["id"]
        side = "YES" if edge > 0 else "NO"

        # Skip if open position exists
        open_pos = engine.query_one(
            "SELECT id FROM trades WHERE market_id = ? AND status IN ('executed', 'executing') "
            "AND (result IS NULL OR result = 'open')",
            (market_id,),
        )
        if open_pos:
            return

        # Rebuy cooldown
        rebuy_days = trading_cfg.get("rebuy_cooldown_days", 7)
        last_closed = engine.query_one(
            "SELECT MAX(executed_at) as last_close FROM trades WHERE market_id = ? "
            "AND result IN ('cashout', 'win', 'loss', 'settled')",
            (market_id,),
        )
        if last_closed and last_closed.get("last_close"):
            try:
                closed_at = datetime.fromisoformat(last_closed["last_close"])
                if (datetime.utcnow() - closed_at).days < rebuy_days:
                    return
            except (ValueError, TypeError):
                pass

        # Skip if pending/approved suggestion exists
        existing = engine.query_one(
            "SELECT id FROM suggestions WHERE type = 'trade' "
            "AND status IN ('pending', 'auto_approved', 'approved') "
            "AND payload LIKE ?",
            (f'%"market_id": "{market_id}"%',),
        )
        if existing:
            return

        # Calculate amount (conservative: scale by edge, cap at max_amount)
        amount = min(round(abs_edge * 10, 2), max_amount)
        amount = max(amount, 1.0)

        price = market["yes_price"] if side == "YES" else market["no_price"]
        mode = trading_cfg.get("mode", "paper")
        status = "auto_approved" if mode == "full-auto" else "pending"

        payload = {
            "market_id": market_id,
            "market_question": market["question"],
            "side": side,
            "amount_usd": amount,
            "price": price,
            "edge": edge,
            "confidence": confidence,
            "strategy_name": "AI Analyst Edge",
        }

        engine.execute(
            """INSERT INTO suggestions (agent_id, type, title, description, payload, status, created_at)
               VALUES (?, ?, ?, ?, ?, ?, datetime('now'))""",
            (
                self.id,
                "trade",
                f"Analyst: {side} '{market['question'][:50]}...'",
                f"Edge: {edge:+.1%} | Konfidenz: {confidence} | "
                f"Preis: {price:.2f} | Liquidität: ${liquidity:,.0f}",
                json.dumps(payload),
                status,
            ),
        )
        self.log("info",
            f"Trade-Suggestion erstellt: {side} '{market['question'][:40]}' "
            f"Edge={edge:+.1%}, ${amount:.2f}")

    @staticmethod
    def _extract_edge(response: str) -> float | None:
        """Try to extract edge value from agent response.
        Normalizes values >1.0 (e.g. 20 -> 0.20) and clamps to [-0.95, 0.95].
        """
        import re
        import logging
        match = re.search(r"EDGE\s*=\s*([+-]?\d+\.?\d*)", response, re.IGNORECASE)
        if match:
            try:
                value = float(match.group(1))
                # Normalize: if AI returns percentage like 20.0 instead of 0.20
                if abs(value) > 1.0:
                    logging.getLogger("analyst").warning(
                        f"Edge normalization: {value} -> {value / 100.0} (was >1.0, dividing by 100)"
                    )
                    value = value / 100.0
                # Clamp to [-0.95, 0.95] - no edge can realistically be 95%+
                if abs(value) > 0.95:
                    logging.getLogger("analyst").warning(
                        f"Edge clamped: {value} -> {0.95 if value > 0 else -0.95} (exceeded 0.95 limit)"
                    )
                    value = 0.95 if value > 0 else -0.95
                return value
            except ValueError:
                pass
        return None

    @staticmethod
    def _extract_confidence(response: str) -> str:
        """Extract confidence level from agent response."""
        import re
        match = re.search(r"KONFIDENZ\s*=\s*(niedrig|mittel|hoch)", response, re.IGNORECASE)
        if match:
            return match.group(1).lower()
        # Fallback: look for keywords
        lower = (response or "").lower()
        if "hoch" in lower and "konfidenz" in lower:
            return "hoch"
        return "mittel"
