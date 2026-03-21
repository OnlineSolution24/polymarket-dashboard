"""
New Market Scanner — detects newly created Polymarket markets
and analyzes them for mispricing opportunities in the first hours
when prices are most inefficient.

Polls Gamma API every 15 minutes for recently created markets,
applies quick filters, and optionally uses AI (haiku) for assessment.
"""

import json
import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Optional

from config import AppConfig

logger = logging.getLogger(__name__)


class NewMarketScanner:
    """Scan for newly created markets and detect mispricing opportunities."""

    # Markets are considered 'new' for this many hours
    NEWNESS_HOURS = 4
    # Minimum volume to consider (filters out completely dead markets)
    MIN_VOLUME = 100
    # Minimum liquidity to be tradeable
    MIN_LIQUIDITY = 50
    # Price ranges that are 'obviously fair' — skip these
    FAIR_PRICE_LOW = 0.35
    FAIR_PRICE_HIGH = 0.65
    # AI confidence threshold to create a suggestion
    AI_CONFIDENCE_THRESHOLD = 0.70
    # Max amount per new-market trade
    MAX_TRADE_AMOUNT = 10.0

    def __init__(self, config: AppConfig):
        self.config = config
        self._analyzed_ids: set[str] = set()
        self._load_analyzed_ids()

    def _load_analyzed_ids(self):
        """Load previously analyzed market IDs from DB to avoid re-scanning."""
        try:
            from db import engine
            rows = engine.query(
                "SELECT id FROM markets WHERE is_new_market = 1"
            )
            self._analyzed_ids = {r["id"] for r in rows}
            logger.debug(f"NewMarketScanner: loaded {len(self._analyzed_ids)} previously analyzed IDs")
        except Exception:
            # Column might not exist yet
            self._analyzed_ids = set()

    def scan_new_markets(self) -> list[dict]:
        """
        Fetch recently created markets from Gamma API.
        Returns list of raw market dicts that are new and not yet analyzed.
        """
        import httpx

        try:
            resp = httpx.get(
                "https://gamma-api.polymarket.com/markets",
                params={
                    "order": "startDate",
                    "ascending": "false",
                    "active": "true",
                    "closed": "false",
                    "limit": 50,
                },
                timeout=30,
                headers={"Accept": "application/json"},
            )
            resp.raise_for_status()
            raw_markets = resp.json()
        except Exception as e:
            logger.error(f"NewMarketScanner: Gamma API fetch failed: {e}")
            return []

        cutoff = datetime.now(timezone.utc) - timedelta(hours=self.NEWNESS_HOURS)
        new_markets = []

        for item in raw_markets:
            market_id = item.get("conditionId", item.get("condition_id", item.get("id", "")))
            if not market_id:
                continue

            # Skip already analyzed
            if market_id in self._analyzed_ids:
                continue

            # Check if market is new enough
            start_date_str = item.get("startDate") or item.get("createdAt") or ""
            if not start_date_str:
                continue

            try:
                # Parse ISO format
                start_date = datetime.fromisoformat(start_date_str.replace("Z", "+00:00"))
                if start_date.tzinfo is None:
                    start_date = start_date.replace(tzinfo=timezone.utc)
            except (ValueError, TypeError):
                continue

            if start_date < cutoff:
                continue

            # Basic filter: must be accepting orders
            if not item.get("acceptingOrders", True):
                continue

            # Parse prices
            outcome_prices = item.get("outcomePrices", "")
            if isinstance(outcome_prices, str):
                try:
                    outcome_prices = json.loads(outcome_prices)
                except Exception:
                    outcome_prices = []

            yes_price = float(outcome_prices[0]) if len(outcome_prices) >= 1 else 0.0
            no_price = float(outcome_prices[1]) if len(outcome_prices) >= 2 else 1.0 - yes_price

            volume = float(item.get("volumeNum", 0) or 0)
            liquidity = float(item.get("liquidityNum", 0) or 0)

            # Extract token IDs
            clob_token_ids = item.get("clobTokenIds", "")
            if isinstance(clob_token_ids, str):
                try:
                    clob_token_ids = json.loads(clob_token_ids)
                except Exception:
                    clob_token_ids = []

            market_age_minutes = (datetime.now(timezone.utc) - start_date).total_seconds() / 60

            new_markets.append({
                "id": market_id,
                "question": item.get("question", "Unknown"),
                "description": item.get("description", ""),
                "slug": item.get("slug", ""),
                "yes_price": yes_price,
                "no_price": no_price,
                "volume": volume,
                "liquidity": liquidity,
                "end_date": item.get("endDate"),
                "start_date": start_date_str,
                "age_minutes": round(market_age_minutes, 1),
                "yes_token_id": clob_token_ids[0] if len(clob_token_ids) >= 1 else "",
                "no_token_id": clob_token_ids[1] if len(clob_token_ids) >= 2 else "",
                "best_bid": float(item.get("bestBid", 0) or 0),
                "best_ask": float(item.get("bestAsk", 0) or 0),
                "spread": float(item.get("spread", 0) or 0),
            })

        logger.info(f"NewMarketScanner: found {len(new_markets)} new markets (last {self.NEWNESS_HOURS}h)")
        return new_markets

    def analyze_new_market(self, market: dict) -> dict:
        """
        Quick rule-based analysis of a new market for mispricing.

        Returns:
            {
                'opportunity': bool,
                'side': 'yes' or 'no' or None,
                'confidence': float (0-1),
                'reason': str,
                'skip_reason': str or None,
            }
        """
        yes_price = market["yes_price"]
        no_price = market["no_price"]
        liquidity = market["liquidity"]
        spread = market.get("spread", 0)

        # Filter 1: No liquidity = can't trade
        if liquidity < self.MIN_LIQUIDITY:
            return {
                "opportunity": False,
                "side": None,
                "confidence": 0,
                "reason": "Insufficient liquidity",
                "skip_reason": f"liquidity={liquidity:.0f} < {self.MIN_LIQUIDITY}",
            }

        # Filter 2: Price in 'fair' range (35-65c) — likely already efficient
        if self.FAIR_PRICE_LOW <= yes_price <= self.FAIR_PRICE_HIGH:
            return {
                "opportunity": False,
                "side": None,
                "confidence": 0,
                "reason": "Price in fair range",
                "skip_reason": f"yes_price={yes_price:.2f} in fair range [{self.FAIR_PRICE_LOW}-{self.FAIR_PRICE_HIGH}]",
            }

        # Filter 3: Spread too wide (> 15c) — expensive to enter
        if spread > 0.15:
            return {
                "opportunity": False,
                "side": None,
                "confidence": 0,
                "reason": "Spread too wide",
                "skip_reason": f"spread={spread:.2f} > 0.15",
            }

        # Passed basic filters — this market has extreme pricing
        # Determine which side looks interesting
        if yes_price < 0.20:
            side = "yes"
            price = yes_price
            edge_description = f"YES at {yes_price:.2f} — potentially undervalued"
        elif yes_price > 0.80:
            side = "no"
            price = no_price
            edge_description = f"NO at {no_price:.2f} — YES might be overvalued"
        elif yes_price < self.FAIR_PRICE_LOW:
            side = "yes"
            price = yes_price
            edge_description = f"YES at {yes_price:.2f} — below fair range"
        else:
            side = "no"
            price = no_price
            edge_description = f"NO at {no_price:.2f} — above fair range"

        return {
            "opportunity": True,
            "side": side,
            "price": price,
            "confidence": 0.5,  # Base confidence, AI will refine
            "reason": edge_description,
            "skip_reason": None,
            "needs_ai": True,
        }

    def quick_ai_assessment(self, market: dict, analysis: dict) -> dict:
        """
        Use AI (haiku model) to quickly assess if a new market is mispriced.
        Cost: ~$0.002-0.005 per call.

        Returns:
            {
                'fair_value': float (0-1),
                'confidence': float (0-1),
                'reasoning': str,
                'recommendation': 'buy_yes' | 'buy_no' | 'skip',
            }
        """
        from services.llm_client import call_llm

        question = market["question"]
        description = market.get("description", "No description available.")
        yes_price = market["yes_price"]
        age_min = market.get("age_minutes", 0)

        prompt = (
            "You are a prediction market analyst. A NEW market was just created on Polymarket.\n\n"
            f"MARKET: {question}\n"
            f"DESCRIPTION: {description[:500]}\n"
            f"CURRENT YES PRICE: {yes_price:.2f} (= {yes_price*100:.0f}% implied probability)\n"
            f"CURRENT NO PRICE: {market['no_price']:.2f}\n"
            f"MARKET AGE: {age_min:.0f} minutes\n"
            f"VOLUME SO FAR: ${market['volume']:.0f}\n"
            f"LIQUIDITY: ${market['liquidity']:.0f}\n"
            f"END DATE: {market.get('end_date', 'Unknown')}\n\n"
            "This market is very new and prices may be inefficient. Analyze:\n\n"
            "1. Is the current price reasonable given what you know?\n"
            "2. What should the fair probability be? (0.00 to 1.00)\n"
            "3. Is there a clear mispricing opportunity?\n\n"
            "Respond in this EXACT JSON format (no other text):\n"
            '{\n'
            '    "fair_value_yes": <float 0-1>,\n'
            '    "confidence": <float 0-1, how confident you are>,\n'
            '    "recommendation": "buy_yes" or "buy_no" or "skip",\n'
            '    "reasoning": "<1-2 sentences>"\n'
            '}'
        )

        try:
            response = call_llm(
                prompt=prompt,
                system_prompt="You are a concise prediction market analyst. Respond only with JSON.",
                model="anthropic/claude-3.5-haiku",
                task_type="new_market_analysis",
                max_tokens=300,
                temperature=0.2,
            )

            if not response:
                return {"fair_value": 0.5, "confidence": 0, "reasoning": "AI call failed", "recommendation": "skip"}

            # Parse JSON from response — strip markdown code blocks if present
            clean = response.strip()
            if clean.startswith("```"):
                clean = clean.split("\n", 1)[-1]
                clean = clean.rsplit("```", 1)[0]
            clean = clean.strip()

            result = json.loads(clean)
            return {
                "fair_value": float(result.get("fair_value_yes", 0.5)),
                "confidence": float(result.get("confidence", 0)),
                "reasoning": result.get("reasoning", ""),
                "recommendation": result.get("recommendation", "skip"),
            }

        except json.JSONDecodeError as e:
            logger.warning(f"NewMarketScanner: AI response not valid JSON: {e}")
            return {"fair_value": 0.5, "confidence": 0, "reasoning": "Parse error", "recommendation": "skip"}
        except Exception as e:
            logger.error(f"NewMarketScanner: AI assessment failed: {e}")
            return {"fair_value": 0.5, "confidence": 0, "reasoning": str(e), "recommendation": "skip"}

    def run_scan(self) -> list[dict]:
        """
        Full scan cycle: discover new markets -> filter -> AI assess -> create suggestions.
        Returns list of opportunities found.
        """
        from db import engine

        opportunities = []
        new_markets = self.scan_new_markets()

        if not new_markets:
            logger.debug("NewMarketScanner: no new markets found")
            return []

        for market in new_markets:
            market_id = market["id"]

            # Mark as analyzed (even if we skip it)
            self._analyzed_ids.add(market_id)

            # Quick analysis (no AI, just rule-based filtering)
            analysis = self.analyze_new_market(market)

            # Store market in DB with first_seen_at and is_new_market flag
            now = datetime.utcnow().isoformat()
            try:
                existing = engine.query_one("SELECT id, first_seen_at FROM markets WHERE id = ?", (market_id,))
                if existing:
                    if not existing.get("first_seen_at"):
                        engine.execute(
                            "UPDATE markets SET first_seen_at = ?, is_new_market = 1 WHERE id = ?",
                            (now, market_id),
                        )
                    else:
                        engine.execute(
                            "UPDATE markets SET is_new_market = 1 WHERE id = ?",
                            (market_id,),
                        )
                else:
                    engine.execute(
                        """INSERT INTO markets
                           (id, question, description, slug, yes_price, no_price, volume, liquidity,
                            end_date, yes_token_id, no_token_id, best_bid, best_ask, spread,
                            accepting_orders, first_seen_at, is_new_market, last_updated)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, 1, ?)""",
                        (market_id, market["question"], market.get("description", ""),
                         market.get("slug", ""), market["yes_price"], market["no_price"],
                         market["volume"], market["liquidity"], market.get("end_date"),
                         market.get("yes_token_id", ""), market.get("no_token_id", ""),
                         market.get("best_bid", 0), market.get("best_ask", 0),
                         market.get("spread", 0), now, now),
                    )
            except Exception as e:
                logger.error(f"NewMarketScanner: DB update failed for {market_id}: {e}")

            if not analysis["opportunity"]:
                logger.debug(
                    f"NewMarketScanner: skip '{market['question'][:40]}' — {analysis.get('skip_reason', 'no opportunity')}"
                )
                continue

            # AI assessment (only for markets that passed filters)
            ai_result = self.quick_ai_assessment(market, analysis)

            # Combine: use AI confidence if available, else base confidence
            final_confidence = ai_result["confidence"] if ai_result["confidence"] > 0 else analysis["confidence"]
            recommendation = ai_result["recommendation"]

            # Determine side from AI recommendation
            if recommendation == "buy_yes":
                side = "yes"
                price = market["yes_price"]
            elif recommendation == "buy_no":
                side = "no"
                price = market["no_price"]
            else:
                logger.info(
                    f"NewMarketScanner: AI skipped '{market['question'][:40]}' — {ai_result['reasoning'][:80]}"
                )
                continue

            # Minimum confidence threshold
            if final_confidence < self.AI_CONFIDENCE_THRESHOLD:
                logger.info(
                    f"NewMarketScanner: low confidence ({final_confidence:.2f}) for '{market['question'][:40]}'"
                )
                continue

            # Calculate edge
            fair_value = ai_result.get("fair_value", 0.5)
            if side == "yes":
                edge = fair_value - market["yes_price"]
            else:
                edge = (1 - fair_value) - market["no_price"]

            if edge < 0.05:
                logger.info(f"NewMarketScanner: edge too small ({edge:.2f}) for '{market['question'][:40]}'")
                continue

            opportunity = {
                "market_id": market_id,
                "market_question": market["question"],
                "side": side,
                "price": price,
                "fair_value": fair_value,
                "edge": edge,
                "confidence": final_confidence,
                "ai_reasoning": ai_result["reasoning"],
                "market_age_minutes": market.get("age_minutes", 0),
                "volume": market["volume"],
                "liquidity": market["liquidity"],
                "amount_usd": min(self.MAX_TRADE_AMOUNT, market["liquidity"] * 0.05),
                "yes_token_id": market.get("yes_token_id", ""),
                "no_token_id": market.get("no_token_id", ""),
                "strategy_id": "strat_new_market",
                "strategy_name": "New Market Scanner",
            }
            opportunities.append(opportunity)

            # Create suggestion in DB
            try:
                from config import load_platform_config
                platform_cfg = load_platform_config()
                mode = platform_cfg.get("trading", {}).get("mode", "paper")
                status = "auto_approved" if mode == "full-auto" else "pending"

                engine.execute(
                    """INSERT INTO suggestions (agent_id, type, title, description, payload, status, created_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    (
                        "new-market-scanner",
                        "trade",
                        f"New Market: {side.upper()} auf '{market['question'][:50]}'",
                        (
                            f"Neuer Markt ({market.get('age_minutes', 0):.0f} Min alt) | "
                            f"Edge: {edge:.1%} | Preis: {price:.2f} | "
                            f"AI: {ai_result['reasoning'][:100]}"
                        ),
                        json.dumps(opportunity),
                        status,
                        now,
                    ),
                )

                # Track as strategy trade
                engine.execute(
                    """INSERT INTO strategy_trades
                       (strategy_id, market_id, side, entry_price, amount_usd, is_backtest, created_at)
                       VALUES (?, ?, ?, ?, ?, 0, ?)""",
                    ("strat_new_market", market_id, side, price, opportunity["amount_usd"], now),
                )

            except Exception as e:
                logger.error(f"NewMarketScanner: suggestion creation failed: {e}")

            # Send Telegram notification
            try:
                from services.telegram_alerts import get_alerts
                alerts = get_alerts(self.config)
                alerts.send(
                    f"<b>New Market Found</b>\n"
                    f"<b>Q:</b> {market['question'][:80]}\n"
                    f"<b>Side:</b> {side.upper()} @ {price:.2f}\n"
                    f"<b>Edge:</b> {edge:.1%} | Confidence: {final_confidence:.0%}\n"
                    f"<b>Age:</b> {market.get('age_minutes', 0):.0f} min\n"
                    f"<b>AI:</b> {ai_result['reasoning'][:120]}"
                )
            except Exception as e:
                logger.warning(f"NewMarketScanner: Telegram notification failed: {e}")

        logger.info(f"NewMarketScanner: scan complete — {len(opportunities)} opportunities from {len(new_markets)} new markets")
        return opportunities


def run_new_market_scan(config: AppConfig) -> list[dict]:
    """Convenience function for scheduler integration."""
    scanner = NewMarketScanner(config)
    return scanner.run_scan()
