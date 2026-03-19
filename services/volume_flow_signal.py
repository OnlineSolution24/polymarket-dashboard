"""
Volume Flow Signal — Live monitoring of order book imbalance across active markets.

Based on the strongest edge discovered in 288M blockchain trades:
When one side has significantly more buying pressure early in a market's life,
that side wins 77-95% of the time (depending on imbalance ratio).

This service uses live order book data as a real-time proxy for volume flow.
"""

import json
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional

from config import AppConfig, load_platform_config

logger = logging.getLogger(__name__)


@dataclass
class FlowSignal:
    """A detected volume flow imbalance signal."""
    condition_id: str
    question: str
    dominant_side: str          # "YES" or "NO"
    flow_ratio: float           # e.g. 3.2 means dominant side has 3.2x more depth
    yes_bid_depth: float
    no_bid_depth: float
    yes_price: float
    no_price: float
    estimated_edge: float       # mapped from historical flow_ratio → edge
    market_volume: float
    market_end_date: Optional[str]


# Historical edge mapping from strategy discovery results (288M trades):
#   flow_ratio 2-3x  → ~12% edge
#   flow_ratio 3-5x  → ~27% edge
#   flow_ratio 5x+   → ~45% edge
FLOW_RATIO_EDGE = [
    (5.0, 0.30),   # 5x+ imbalance → ~30% edge (conservative vs 45% historical)
    (3.0, 0.18),   # 3-5x → ~18% edge (conservative vs 27%)
    (2.0, 0.08),   # 2-3x → ~8% edge (conservative vs 12%)
]


def estimate_edge_from_ratio(ratio: float) -> float:
    """Map a flow ratio to estimated edge based on historical data.

    We use conservative estimates (roughly 65% of historical edge)
    because order book depth is a proxy, not exact volume flow.
    """
    for threshold, edge in FLOW_RATIO_EDGE:
        if ratio >= threshold:
            return edge
    return 0.0


def is_market_early(market: dict) -> bool:
    """Check if a market is still in its early phase (first half of lifecycle).

    The historical edge only applies to early volume flow — once a market
    matures, the flow information is already priced in.
    """
    end_date_str = market.get("end_date")
    if not end_date_str:
        return True  # No end date = assume ongoing

    try:
        end_date = datetime.fromisoformat(end_date_str.replace("Z", "+00:00"))
        now = datetime.now(end_date.tzinfo) if end_date.tzinfo else datetime.utcnow()

        # Market must end at least 24h from now (not about to close)
        if end_date - now < timedelta(hours=24):
            return False

        # We don't know exact creation date from Gamma API, but we can
        # filter out markets ending very soon as "late lifecycle"
        return True
    except Exception:
        return True


def scan_markets_for_flow_signals(config: AppConfig) -> list[FlowSignal]:
    """Scan active markets for order book imbalance signals.

    1. Fetch active markets from Gamma API
    2. For each market with YES+NO token IDs, get order book depth
    3. Compute flow ratio (dominant side depth / weaker side depth)
    4. Return signals where ratio exceeds threshold
    """
    from services.polymarket_client import PolymarketService

    platform_cfg = load_platform_config()
    vf_cfg = platform_cfg.get("scheduler", {}).get("volume_flow", {})
    min_flow_ratio = vf_cfg.get("min_flow_ratio", 2.0)
    min_volume = vf_cfg.get("min_volume_usd", 10000)
    max_markets = vf_cfg.get("max_markets_per_scan", 80)

    service = PolymarketService(config)
    try:
        markets = service.fetch_markets(limit=max_markets)
    except Exception as e:
        logger.error(f"Failed to fetch markets: {e}")
        return []

    signals = []

    for market in markets:
        yes_token = market.get("yes_token_id", "")
        no_token = market.get("no_token_id", "")
        condition_id = market.get("id", "")

        if not yes_token or not no_token or not condition_id:
            continue

        # Filter: minimum volume
        volume = float(market.get("volume", 0) or 0)
        if volume < min_volume:
            continue

        # Filter: early lifecycle only
        if not is_market_early(market):
            continue

        # Get order book depth for both sides
        try:
            yes_book = service.get_order_book_analysis(yes_token)
            no_book = service.get_order_book_analysis(no_token)
        except Exception as e:
            logger.debug(f"Order book fetch failed for {condition_id[:20]}: {e}")
            continue

        yes_bid_depth = yes_book.get("bid_depth", 0)
        no_bid_depth = no_book.get("bid_depth", 0)

        # Need meaningful depth on at least one side
        if yes_bid_depth < 100 and no_bid_depth < 100:
            continue

        # Compute flow ratio
        if yes_bid_depth > no_bid_depth and no_bid_depth > 0:
            flow_ratio = yes_bid_depth / no_bid_depth
            dominant_side = "YES"
        elif no_bid_depth > yes_bid_depth and yes_bid_depth > 0:
            flow_ratio = no_bid_depth / yes_bid_depth
            dominant_side = "NO"
        else:
            continue

        if flow_ratio < min_flow_ratio:
            continue

        estimated_edge = estimate_edge_from_ratio(flow_ratio)
        if estimated_edge <= 0:
            continue

        yes_price = float(market.get("yes_price", market.get("best_bid", 0)) or 0)
        no_price = 1.0 - yes_price if yes_price > 0 else 0

        signals.append(FlowSignal(
            condition_id=condition_id,
            question=market.get("question", "")[:200],
            dominant_side=dominant_side,
            flow_ratio=round(flow_ratio, 2),
            yes_bid_depth=round(yes_bid_depth, 2),
            no_bid_depth=round(no_bid_depth, 2),
            yes_price=yes_price,
            no_price=no_price,
            estimated_edge=estimated_edge,
            market_volume=volume,
            market_end_date=market.get("end_date"),
        ))

    # Sort by flow ratio (strongest signal first)
    signals.sort(key=lambda s: s.flow_ratio, reverse=True)
    logger.info(f"Volume Flow scan: {len(markets)} markets checked, {len(signals)} signals found")
    return signals


def create_suggestions_from_signals(signals: list[FlowSignal]) -> int:
    """Insert trade suggestions for flow signals into the database.

    Returns number of suggestions created.
    """
    from db import engine

    platform_cfg = load_platform_config()
    vf_cfg = platform_cfg.get("scheduler", {}).get("volume_flow", {})
    amount_usd = vf_cfg.get("amount_usd", 2.0)
    max_suggestions = vf_cfg.get("max_suggestions_per_scan", 5)

    created = 0

    for signal in signals[:max_suggestions]:
        # Price for the dominant side
        price = signal.yes_price if signal.dominant_side == "YES" else signal.no_price

        if price <= 0 or price >= 0.95:
            continue

        payload = {
            "market_id": signal.condition_id,
            "market_question": signal.question,
            "side": signal.dominant_side,
            "amount_usd": amount_usd,
            "price": price,
            "edge": signal.estimated_edge,
            "strategy_name": "Volume Flow Signal",
            "flow_ratio": signal.flow_ratio,
            "yes_bid_depth": signal.yes_bid_depth,
            "no_bid_depth": signal.no_bid_depth,
        }

        # Check for existing pending/approved suggestion on same market
        try:
            existing = engine.execute(
                """SELECT COUNT(*) FROM suggestions
                   WHERE json_extract(payload, '$.market_id') = ?
                   AND status IN ('pending', 'approved')""",
                (signal.condition_id,),
            ).fetchone()

            if existing and existing[0] > 0:
                logger.debug(f"Skipping duplicate suggestion for {signal.condition_id[:20]}")
                continue
        except Exception:
            pass  # If dedup check fails, still create the suggestion

        status = "approved"  # full-auto mode

        try:
            engine.execute(
                """INSERT INTO suggestions (agent_id, type, title, description, payload, status, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, datetime('now'))""",
                (
                    "volume-flow-signal",
                    "trade",
                    f"VFlow: {signal.dominant_side} '{signal.question[:50]}'",
                    f"Flow Ratio: {signal.flow_ratio:.1f}x | Edge: {signal.estimated_edge:+.0%} | "
                    f"YES depth: {signal.yes_bid_depth:.0f} / NO depth: {signal.no_bid_depth:.0f} | "
                    f"Vol: ${signal.market_volume:,.0f}",
                    json.dumps(payload),
                    status,
                ),
            )
            created += 1
            logger.info(
                f"Created suggestion: {signal.dominant_side} on '{signal.question[:40]}' "
                f"(ratio={signal.flow_ratio:.1f}x, edge={signal.estimated_edge:+.0%})"
            )
        except Exception as e:
            logger.error(f"Failed to create suggestion: {e}")

    return created


def run_volume_flow_scan(config: AppConfig) -> dict:
    """Main entry point — scan markets and create suggestions.

    Called by scheduler job or manually.
    """
    signals = scan_markets_for_flow_signals(config)
    created = 0

    if signals:
        created = create_suggestions_from_signals(signals)

    return {
        "ok": True,
        "markets_scanned": len(signals),
        "signals_found": len(signals),
        "suggestions_created": created,
        "top_signal": {
            "market": signals[0].question[:80],
            "side": signals[0].dominant_side,
            "ratio": signals[0].flow_ratio,
            "edge": signals[0].estimated_edge,
        } if signals else None,
    }
