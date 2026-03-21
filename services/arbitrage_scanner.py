"""
Arbitrage Scanner for Polymarket.
Finds price discrepancies between related markets for near-riskless profit.

Type 1: YES/NO — buy both sides when YES+NO < 0.96 (guaranteed profit at settlement)
Type 2: Multi-Outcome — buy all outcomes when sum < 0.95 in exclusive events
Type 3: Correlated — find logically related markets with inconsistent prices
"""

import json
import logging
import re
from datetime import datetime
from typing import Optional

import httpx

logger = logging.getLogger("arbitrage_scanner")

GAMMA_API = "https://gamma-api.polymarket.com"
POLYMARKET_FEE = 0.02  # 2% fee on winnings


class ArbitrageOpportunity:
    """Represents a found arbitrage opportunity."""

    def __init__(self, arb_type: str, markets: list, profit_usd: float,
                 description: str, confidence: float = 0.9):
        self.arb_type = arb_type  # "yes_no", "multi_outcome", "correlated"
        self.markets = markets  # list of {market_id, question, side, price, amount}
        self.profit_usd = profit_usd
        self.description = description
        self.confidence = confidence
        self.found_at = datetime.utcnow().isoformat()

    def to_dict(self) -> dict:
        return {
            "arb_type": self.arb_type,
            "markets": self.markets,
            "profit_usd": round(self.profit_usd, 4),
            "description": self.description,
            "confidence": self.confidence,
            "found_at": self.found_at,
        }


class ArbitrageScanner:
    """Scans Polymarket for arbitrage opportunities across three types."""

    def __init__(self, fee: float = POLYMARKET_FEE):
        self.fee = fee
        self._http = httpx.Client(timeout=30, headers={"Accept": "application/json"})

    def close(self):
        self._http.close()

    # ------------------------------------------------------------------
    # Type 1: YES/NO Arbitrage
    # ------------------------------------------------------------------

    def scan_yes_no_arb(self, min_profit: float = 0.01) -> list:
        """Check all DB markets where YES+NO deviates significantly from 1.0.

        If YES + NO < (1.0 - fee) -> buy BOTH sides for guaranteed profit.
        If YES + NO > (1.0 + fee*2) -> one side is overpriced.
        """
        from db import engine

        markets = engine.query(
            "SELECT id, question, yes_price, no_price, volume_24h, liquidity, "
            "yes_token_id, no_token_id, accepting_orders "
            "FROM markets WHERE yes_price > 0 AND no_price > 0 AND accepting_orders = 1"
        )

        opportunities = []
        # Threshold: buying both sides costs YES+NO, settlement pays $1.
        # After fees: profit = 1.0*(1-fee) - (YES+NO)
        # So we need: YES+NO < 1.0*(1-fee) = 0.98

        for m in markets:
            yes = m["yes_price"]
            no = m["no_price"]
            total = yes + no

            # Guaranteed profit from buying both sides
            profit_per_dollar = (1.0 * (1.0 - self.fee)) - total
            if profit_per_dollar > min_profit:
                # Scale: invest up to $25 split across YES and NO
                invest = min(25.0, (m.get("liquidity") or 100))
                profit = profit_per_dollar * invest

                opp = ArbitrageOpportunity(
                    arb_type="yes_no",
                    markets=[
                        {"market_id": m["id"], "question": m["question"],
                         "side": "YES", "price": yes,
                         "token_id": m.get("yes_token_id", "")},
                        {"market_id": m["id"], "question": m["question"],
                         "side": "NO", "price": no,
                         "token_id": m.get("no_token_id", "")},
                    ],
                    profit_usd=profit,
                    description=(
                        f"YES({yes:.3f}) + NO({no:.3f}) = {total:.3f} < 0.98. "
                        f"Buy both -> guaranteed ${profit:.2f} profit at settlement."
                    ),
                    confidence=0.95,
                )
                opportunities.append(opp)
                logger.info(f"YES/NO arb: {m['question'][:50]} | total={total:.3f} | profit=${profit:.2f}")

            # Overpriced detection: if total > 1.0 + fees, one side is overpriced
            elif total > 1.0 + self.fee * 2:
                overpriced_side = "YES" if yes > no else "NO"
                overpriced_price = max(yes, no)
                opp = ArbitrageOpportunity(
                    arb_type="yes_no",
                    markets=[
                        {"market_id": m["id"], "question": m["question"],
                         "side": overpriced_side, "price": overpriced_price,
                         "action": "SELL/SHORT"},
                    ],
                    profit_usd=0,  # Cannot calculate without short position
                    description=(
                        f"YES({yes:.3f}) + NO({no:.3f}) = {total:.3f} > 1.04. "
                        f"{overpriced_side} side appears overpriced."
                    ),
                    confidence=0.7,
                )
                opportunities.append(opp)

        return opportunities

    # ------------------------------------------------------------------
    # Type 2: Multi-Outcome Arbitrage (via Gamma Events API)
    # ------------------------------------------------------------------

    def scan_multi_outcome_arb(self, min_profit: float = 0.01,
                                max_events: int = 50, max_outcomes: int = 20) -> list:
        """Group markets by event, check if exclusive outcome prices sum to ~1.0.

        Only considers negRisk=True events (mutually exclusive outcomes).
        If sum < (1.0 - fees) -> buy all outcomes cheaply.
        If one outcome is clearly overpriced -> flag it.
        """
        opportunities = []

        try:
            events = self._fetch_events(limit=max_events)
        except Exception as e:
            logger.error(f"Failed to fetch events for multi-outcome scan: {e}")
            return opportunities

        for event in events:
            # Only consider negRisk events (mutually exclusive outcomes)
            if not (event.get("negRisk") or event.get("enableNegRisk")):
                continue

            markets = event.get("markets", [])
            if len(markets) < 2:
                continue

            # Skip events with too many outcomes (expensive to buy all)
            if len(markets) > max_outcomes:
                logger.debug(f"Skipping event with {len(markets)} outcomes > {max_outcomes}: {event.get('title', '')[:40]}")
                continue

            # Collect active market prices
            outcome_prices = []
            for m in markets:
                if not m.get("acceptingOrders", True):
                    continue

                prices = m.get("outcomePrices", "[]")
                if isinstance(prices, str):
                    try:
                        prices = json.loads(prices)
                    except Exception:
                        continue

                yes_price = float(prices[0]) if prices else 0
                if yes_price <= 0:
                    continue

                outcome_prices.append({
                    "market_id": m.get("conditionId", m.get("id", "")),
                    "question": m.get("question", ""),
                    "yes_price": yes_price,
                    "liquidity": float(m.get("liquidityNum", 0) or 0),
                    "volume": float(m.get("volumeNum", 0) or 0),
                })

            if len(outcome_prices) < 2:
                continue

            total = sum(op["yes_price"] for op in outcome_prices)

            # Buy-all-outcomes arb: cost = total, payout = $1, profit = 1*(1-fee) - total
            profit_per_share = (1.0 * (1.0 - self.fee)) - total
            if profit_per_share > min_profit:
                # Scale investment by minimum liquidity across outcomes
                min_liq = min((op.get("liquidity") or 10) for op in outcome_prices)
                invest = min(25.0, max(1.0, min_liq))
                profit = profit_per_share * invest

                market_entries = [
                    {"market_id": op["market_id"],
                     "question": op["question"][:80],
                     "side": "YES", "price": op["yes_price"]}
                    for op in outcome_prices
                ]

                opp = ArbitrageOpportunity(
                    arb_type="multi_outcome",
                    markets=market_entries,
                    profit_usd=profit,
                    description=(
                        f"Event '{event.get('title', '')[:60]}': "
                        f"{len(outcome_prices)} outcomes sum to {total:.3f} < {1.0 - self.fee:.2f}. "
                        f"Buy all -> guaranteed ${profit:.2f} profit."
                    ),
                    confidence=0.90,
                )
                # Attach end_date for settlement time filtering
                opp._end_date = event.get("endDate") or event.get("end_date")
                opportunities.append(opp)
                logger.info(
                    f"Multi-outcome arb: {event.get('title', '')[:40]} | "
                    f"sum={total:.3f} | profit=${profit:.2f}"
                )

            # Check for overpriced single outcome (sum > 1.0 + fees)
            elif total > 1.0 + self.fee * 2:
                # Find the most overpriced outcome
                sorted_outcomes = sorted(outcome_prices, key=lambda x: x["yes_price"], reverse=True)
                top = sorted_outcomes[0]
                fair_price = 1.0 - sum(op["yes_price"] for op in sorted_outcomes[1:])

                if top["yes_price"] > fair_price + 0.05:
                    opp = ArbitrageOpportunity(
                        arb_type="multi_outcome",
                        markets=[{
                            "market_id": top["market_id"],
                            "question": top["question"][:80],
                            "side": "NO",
                            "price": top["yes_price"],
                            "fair_price": round(fair_price, 3),
                            "action": "BUY NO (overpriced YES)",
                        }],
                        profit_usd=0,
                        description=(
                            f"Event '{event.get('title', '')[:60]}': "
                            f"sum={total:.3f} > 1.04. "
                            f"'{top['question'][:40]}' at {top['yes_price']:.3f} "
                            f"vs fair value ~{fair_price:.3f}."
                        ),
                        confidence=0.75,
                    )
                    opportunities.append(opp)

        return opportunities

    # ------------------------------------------------------------------
    # Type 3: Correlated Market Arbitrage
    # ------------------------------------------------------------------

    def scan_correlated_arb(self) -> list:
        """Find related markets with price inconsistencies.

        Logic:
        - Subset relationships: "X wins March" must be <= "X wins Q1"
        - Superset relationships: "Republican wins" must be >= "Trump wins"
        - Contradictions: same question with different time horizons
        """
        from db import engine

        markets = engine.query(
            "SELECT id, question, yes_price, no_price, slug, volume_24h "
            "FROM markets WHERE yes_price > 0 AND accepting_orders = 1 "
            "ORDER BY volume_24h DESC LIMIT 500"
        )

        opportunities = []

        # Build keyword index for fast matching
        keyword_groups = self._group_by_keywords(markets)

        seen_pairs = set()
        for key, group in keyword_groups.items():
            if len(group) < 2 or len(group) > 20:
                continue

            for i in range(len(group)):
                for j in range(i + 1, len(group)):
                    m1 = group[i]
                    m2 = group[j]
                    pair_key = tuple(sorted([m1["id"], m2["id"]]))
                    if pair_key in seen_pairs:
                        continue
                    seen_pairs.add(pair_key)

                    opp = self._check_pair_arbitrage(m1, m2)
                    if opp:
                        opportunities.append(opp)

        return opportunities

    def _group_by_keywords(self, markets: list) -> dict:
        """Group markets by extracted key entities for correlation detection."""
        groups = {}

        for m in markets:
            q = m["question"].lower()
            q = re.sub(r'^will\s+', '', q)
            q = re.sub(r'\?$', '', q)

            # Extract significant words
            words = re.findall(r'[a-z]+', q)
            stop_words = {'the', 'a', 'an', 'be', 'on', 'in', 'at', 'by', 'of', 'to',
                          'win', 'hit', 'reach', 'price', 'before', 'after', 'end',
                          'between', 'above', 'below', 'from', 'and', 'or', 'vs', 'for',
                          'will', 'this', 'that', 'its', 'is', 'are', 'was', 'were'}
            significant = [w for w in words if w not in stop_words and len(w) > 2]

            if len(significant) >= 2:
                key = " ".join(significant[:2])
                groups.setdefault(key, []).append(m)

            # Also group by named entities (proper nouns in original question)
            entities = re.findall(r'[A-Z][a-z]+(?:\s+[A-Z][a-z]+)*', m["question"])
            for entity in entities:
                if len(entity) > 3:
                    ent_key = entity.lower()
                    groups.setdefault(ent_key, []).append(m)

        return groups

    def _check_pair_arbitrage(self, m1: dict, m2: dict) -> Optional[ArbitrageOpportunity]:
        """Check if two related markets have inconsistent prices."""
        q1 = m1["question"].lower()
        q2 = m2["question"].lower()

        if m1["id"] == m2["id"]:
            return None

        # Pattern 1: Subset time relationship
        # "X by March" should be <= "X by June"
        time_order = self._compare_time_scope(q1, q2)
        if time_order is not None:
            shorter, longer = (m1, m2) if time_order < 0 else (m2, m1)
            # Shorter timeframe must have lower or equal probability
            if shorter["yes_price"] > longer["yes_price"] + 0.05:
                diff = shorter["yes_price"] - longer["yes_price"]
                return ArbitrageOpportunity(
                    arb_type="correlated",
                    markets=[
                        {"market_id": shorter["id"], "question": shorter["question"][:80],
                         "side": "NO", "price": shorter["yes_price"],
                         "action": "BUY NO (overpriced)"},
                        {"market_id": longer["id"], "question": longer["question"][:80],
                         "side": "YES", "price": longer["yes_price"],
                         "action": "BUY YES (underpriced)"},
                    ],
                    profit_usd=0,
                    description=(
                        f"Time subset: shorter deadline at {shorter['yes_price']:.3f} > "
                        f"longer deadline at {longer['yes_price']:.3f}. "
                        f"Diff: {diff:.3f}"
                    ),
                    confidence=0.80,
                )

        # Pattern 2: Threshold relationship (e.g., BTC > 90k implies BTC > 80k)
        threshold_check = self._compare_thresholds(q1, q2, m1, m2)
        if threshold_check:
            return threshold_check

        return None

    def _compare_time_scope(self, q1: str, q2: str) -> Optional[int]:
        """Compare time scope. Returns -1 if q1 shorter, +1 if q2 shorter, None if unclear."""
        months = {"january": 1, "february": 2, "march": 3, "april": 4, "may": 5,
                  "june": 6, "july": 7, "august": 8, "september": 9, "october": 10,
                  "november": 11, "december": 12}
        quarters = {"q1": 3, "q2": 6, "q3": 9, "q4": 12}

        def extract_deadline(q):
            for month, num in months.items():
                if f"by {month}" in q or f"before {month}" in q or f"in {month}" in q:
                    return num
            for qtr, num in quarters.items():
                if qtr in q:
                    return num
            date_match = re.search(r'by\s+(\d{4}-\d{2}-\d{2})', q)
            if date_match:
                try:
                    dt = datetime.fromisoformat(date_match.group(1))
                    return dt.month + dt.day / 31.0
                except Exception:
                    pass
            year_match = re.search(r'(?:by|before|in)\s+(\d{4})', q)
            if year_match:
                return int(year_match.group(1)) * 12
            return None

        # Check both questions share core topic (at least 2 common significant words)
        def get_core(q):
            words = re.findall(r'[a-z]+', q)
            stop = {'will', 'the', 'a', 'an', 'be', 'on', 'in', 'at', 'by', 'of', 'to',
                     'before', 'after', 'end', 'hit', 'reach', 'price'}
            return {w for w in words if w not in stop and len(w) > 2}

        core1 = get_core(q1)
        core2 = get_core(q2)
        if len(core1 & core2) < 2:
            return None

        d1 = extract_deadline(q1)
        d2 = extract_deadline(q2)

        if d1 is not None and d2 is not None and d1 != d2:
            return -1 if d1 < d2 else 1
        return None

    def _compare_thresholds(self, q1: str, q2: str, m1: dict, m2: dict) -> Optional[ArbitrageOpportunity]:
        """Check for threshold relationships where pricing violates logical constraints.

        For "reach/hit/above X": higher X -> should be LESS likely (lower price)
        For "dip/below/under X": lower X -> should be LESS likely (lower price)

        Only flags TRUE anomalies where the relationship is VIOLATED.
        """
        def extract_threshold(q):
            match = re.search(r'\$?([\d,]+)(?:k)?', q)
            if match:
                val = match.group(1).replace(',', '')
                try:
                    v = float(val)
                    if 'k' in q[match.end()-1:match.end()+1].lower():
                        v *= 1000
                    return v
                except ValueError:
                    pass
            return None

        def get_core(q):
            words = re.findall(r'[a-z]+', q)
            stop = {'will', 'the', 'a', 'an', 'be', 'on', 'in', 'at', 'by', 'of', 'to',
                     'before', 'after', 'end', 'hit', 'reach', 'price', 'above', 'below',
                     'dip', 'under', 'between'}
            return {w for w in words if w not in stop and len(w) > 2}

        core1 = get_core(q1)
        core2 = get_core(q2)
        if len(core1 & core2) < 2:
            return None

        t1 = extract_threshold(q1)
        t2 = extract_threshold(q2)

        if t1 is None or t2 is None or t1 == t2:
            return None

        # Determine direction: "reach/hit/above" = upward, "dip/below/under" = downward
        upward_words = {'reach', 'hit', 'above', 'high', 'over'}
        downward_words = {'dip', 'below', 'under', 'low', 'drop', 'fall'}

        def get_direction(q):
            words = set(re.findall(r'[a-z]+', q))
            if words & upward_words:
                return "up"
            if words & downward_words:
                return "down"
            return None

        dir1 = get_direction(q1)
        dir2 = get_direction(q2)

        # Both must have same direction for comparison to be valid
        if dir1 != dir2 or dir1 is None:
            return None

        # For upward targets: higher threshold = harder = should be cheaper
        # Anomaly: higher threshold is MORE expensive than lower threshold
        if dir1 == "up":
            higher_t_m, lower_t_m = (m1, m2) if t1 > t2 else (m2, m1)
            # This is an anomaly ONLY if higher target is priced HIGHER (more likely)
            if higher_t_m["yes_price"] > lower_t_m["yes_price"] + 0.05:
                diff = higher_t_m["yes_price"] - lower_t_m["yes_price"]
                return ArbitrageOpportunity(
                    arb_type="correlated",
                    markets=[
                        {"market_id": higher_t_m["id"], "question": higher_t_m["question"][:80],
                         "side": "NO", "price": higher_t_m["yes_price"],
                         "action": "BUY NO (harder target overpriced)"},
                        {"market_id": lower_t_m["id"], "question": lower_t_m["question"][:80],
                         "side": "YES", "price": lower_t_m["yes_price"],
                         "action": "BUY YES (easier target underpriced)"},
                    ],
                    profit_usd=0,
                    description=(
                        f"Threshold arb (upward): harder target at {higher_t_m['yes_price']:.3f} > "
                        f"easier target at {lower_t_m['yes_price']:.3f}. Diff: {diff:.3f}"
                    ),
                    confidence=0.80,
                )

        # For downward targets: lower threshold = harder = should be cheaper
        # Anomaly: lower threshold is priced HIGHER
        elif dir1 == "down":
            lower_t_m, higher_t_m = (m1, m2) if t1 < t2 else (m2, m1)
            if lower_t_m["yes_price"] > higher_t_m["yes_price"] + 0.05:
                diff = lower_t_m["yes_price"] - higher_t_m["yes_price"]
                return ArbitrageOpportunity(
                    arb_type="correlated",
                    markets=[
                        {"market_id": lower_t_m["id"], "question": lower_t_m["question"][:80],
                         "side": "NO", "price": lower_t_m["yes_price"],
                         "action": "BUY NO (harder target overpriced)"},
                        {"market_id": higher_t_m["id"], "question": higher_t_m["question"][:80],
                         "side": "YES", "price": higher_t_m["yes_price"],
                         "action": "BUY YES (easier target underpriced)"},
                    ],
                    profit_usd=0,
                    description=(
                        f"Threshold arb (downward): harder target at {lower_t_m['yes_price']:.3f} > "
                        f"easier target at {higher_t_m['yes_price']:.3f}. Diff: {diff:.3f}"
                    ),
                    confidence=0.80,
                )

        return None

    # ------------------------------------------------------------------
    # Profit Calculation
    # ------------------------------------------------------------------

    def calculate_arb_profit(self, prices: list, investment: float = 1.0,
                              fees: float = None) -> float:
        """Calculate guaranteed profit after fees for buying all outcomes.

        Args:
            prices: list of YES prices for all outcomes (should sum < 1.0)
            investment: total USD to invest (split proportionally)
            fees: override fee rate (default: self.fee)

        Returns:
            Guaranteed profit in USD (negative = no arb)
        """
        if fees is None:
            fees = self.fee

        total_cost = sum(prices)
        if total_cost <= 0:
            return -1.0

        # Buy 1 share of each outcome: cost = sum of prices
        # At settlement: exactly one pays $1, fee applies to winnings
        payout = 1.0 * (1.0 - fees)
        profit = payout - total_cost

        # Scale by investment
        shares = investment / total_cost if total_cost > 0 else 0
        return profit * shares

    # ------------------------------------------------------------------
    # Full Scan (all three types)
    # ------------------------------------------------------------------

    def scan_all(self, min_profit_usd: float = 0.50, arb_config: dict = None) -> list:
        """Run all three arbitrage scans and return opportunities above threshold.

        Args:
            min_profit_usd: Minimum absolute profit in USD (overridden by arb_config)
            arb_config: Config dict from platform_config.yaml arbitrage section
        """
        if arb_config is None:
            arb_config = {}

        min_profit_pct = arb_config.get("min_profit_pct", 5.0)
        max_settlement_days = arb_config.get("max_settlement_days", 30)
        max_outcomes = arb_config.get("max_outcomes", 20)
        min_abs_profit = arb_config.get("min_absolute_profit", min_profit_usd)
        max_investment = arb_config.get("max_investment", 25.0)

        all_opps = []

        logger.info(
            f"Starting arbitrage scan (min_profit=${min_abs_profit}, "
            f"min_pct={min_profit_pct}%, max_settle={max_settlement_days}d, "
            f"max_outcomes={max_outcomes})..."
        )

        # Type 1: YES/NO
        try:
            yes_no = self.scan_yes_no_arb(min_profit=0.005)
            all_opps.extend(yes_no)
            logger.info(f"Type 1 (YES/NO): {len(yes_no)} opportunities found")
        except Exception as e:
            logger.error(f"YES/NO scan failed: {e}")

        # Type 2: Multi-Outcome (pass max_outcomes filter)
        try:
            multi = self.scan_multi_outcome_arb(min_profit=0.005, max_outcomes=max_outcomes)
            all_opps.extend(multi)
            logger.info(f"Type 2 (Multi-Outcome): {len(multi)} opportunities found")
        except Exception as e:
            logger.error(f"Multi-outcome scan failed: {e}")

        # Type 3: Correlated
        try:
            corr = self.scan_correlated_arb()
            all_opps.extend(corr)
            logger.info(f"Type 3 (Correlated): {len(corr)} opportunities found")
        except Exception as e:
            logger.error(f"Correlated scan failed: {e}")

        # --- Apply filters ---
        filtered = []
        for opp in all_opps:
            # Filter 1: Min absolute profit
            if opp.profit_usd < min_abs_profit:
                continue

            # Filter 2: Min profit percentage (profit / investment * 100)
            if max_investment > 0:
                profit_pct = (opp.profit_usd / max_investment) * 100
            else:
                profit_pct = 0
            if profit_pct < min_profit_pct:
                logger.debug(
                    f"Skipping arb: profit_pct={profit_pct:.1f}% < {min_profit_pct}% | {opp.description[:60]}"
                )
                continue

            # Filter 3: Max settlement days (check end_date if available)
            if hasattr(opp, '_end_date') and opp._end_date:
                try:
                    end_dt = datetime.fromisoformat(str(opp._end_date).replace('Z', '+00:00'))
                    days_until = (end_dt.replace(tzinfo=None) - datetime.utcnow()).days
                    if days_until > max_settlement_days:
                        logger.debug(
                            f"Skipping arb: settles in {days_until}d > {max_settlement_days}d | {opp.description[:60]}"
                        )
                        continue
                except (ValueError, TypeError):
                    pass

            filtered.append(opp)

        # Sort by profit descending
        filtered.sort(key=lambda x: x.profit_usd, reverse=True)

        logger.info(
            f"Arbitrage scan complete: {len(all_opps)} raw, {len(filtered)} after filters "
            f"(min ${min_abs_profit:.2f}, min {min_profit_pct:.0f}%, max {max_settlement_days}d)"
        )

        return filtered

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _fetch_events(self, limit: int = 50) -> list:
        """Fetch active events from Gamma API."""
        all_events = []
        offset = 0
        page_size = min(limit, 100)

        while len(all_events) < limit:
            resp = self._http.get(
                f"{GAMMA_API}/events",
                params={
                    "active": "true",
                    "closed": "false",
                    "order": "volume",
                    "ascending": "false",
                    "limit": page_size,
                    "offset": offset,
                },
            )
            resp.raise_for_status()
            page = resp.json()
            if not page:
                break
            all_events.extend(page)
            offset += page_size

        return all_events[:limit]
