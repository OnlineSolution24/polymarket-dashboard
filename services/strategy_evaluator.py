"""
Strategy Rule Evaluator.
Evaluates strategy entry/exit rules against market data.
Used by both the backtest service and the REST API for signal matching.
"""

from datetime import datetime
from typing import Optional

from db import engine


# Supported comparison operators
_OPS = {
    "gt": lambda a, b: a > b,
    "lt": lambda a, b: a < b,
    "gte": lambda a, b: a >= b,
    "lte": lambda a, b: a <= b,
    "eq": lambda a, b: a == b,
}


def evaluate_rules(market: dict, rules: list[dict]) -> bool:
    """Check if a market matches ALL rules.

    Each rule: {"field": "yes_price", "op": "lt", "value": 0.30}
    Supported fields: yes_price, no_price, volume, liquidity,
                      sentiment_score, calculated_edge, days_to_expiry,
                      whale_buy_count, whale_sell_count, whale_net_flow,
                      top_holder_concentration, open_interest, oi_change_24h,
                      smart_money_score, book_imbalance, spread,
                      volume_24h, volume_1w, bid_depth, ask_depth
    """
    for rule in rules:
        field = rule.get("field", "")
        op = rule.get("op", "")
        value = rule.get("value")

        if op not in _OPS or value is None:
            return False

        # Compute derived fields
        if field == "days_to_expiry":
            end_date = market.get("end_date")
            if not end_date:
                return False
            try:
                expiry = datetime.fromisoformat(str(end_date).replace("Z", "+00:00"))
                market_value = (expiry - datetime.utcnow()).days
            except (ValueError, TypeError):
                return False
        else:
            market_value = market.get(field)

        if market_value is None:
            return False

        try:
            if not _OPS[op](float(market_value), float(value)):
                return False
        except (ValueError, TypeError):
            return False

    return True


def find_matching_markets(definition: dict, limit: int = 50) -> list[dict]:
    """Find current markets that match a strategy's entry rules.

    Args:
        definition: Strategy definition dict with entry_rules, category_filter, min_liquidity
        limit: Max markets to check

    Returns:
        List of matching market dicts
    """
    entry_rules = definition.get("entry_rules", [])
    category_filter = definition.get("category_filter", [])
    min_liquidity = definition.get("min_liquidity", 0)

    # Build base query
    query = "SELECT * FROM markets WHERE 1=1"
    params = []

    if min_liquidity > 0:
        query += " AND liquidity >= ?"
        params.append(min_liquidity)

    if category_filter:
        placeholders = ",".join("?" for _ in category_filter)
        query += f" AND LOWER(category) IN ({placeholders})"
        params.extend([c.lower() for c in category_filter])

    query += " ORDER BY volume DESC LIMIT ?"
    params.append(limit)

    markets = engine.query(query, tuple(params))
    if not markets:
        return []

    matched = []
    for market in markets:
        if evaluate_rules(market, entry_rules):
            matched.append(market)

    return matched


def compute_trade_params(market: dict, trade_params: dict, capital: float = 100.0) -> Optional[dict]:
    """Compute trade details for a matching market.

    Returns dict with side, amount_usd, or None if trade not viable.
    """
    side = trade_params.get("side", "YES")
    sizing_method = trade_params.get("sizing_method", "kelly")
    sizing_value = trade_params.get("sizing_value", 0.03)
    min_edge = trade_params.get("min_edge", 0.03)

    # Use market's calculated_edge if available, otherwise fall back to
    # the strategy's own declared edge (from discovery/backtest).
    edge = market.get("calculated_edge", 0) or 0
    strategy_edge = trade_params.get("strategy_edge", 0)
    if edge <= 0 and strategy_edge > 0:
        edge = strategy_edge
    if edge < min_edge:
        return None

    if sizing_method == "kelly":
        fraction = min(edge, sizing_value)
        amount = capital * fraction
    elif sizing_method == "fixed_pct":
        amount = capital * sizing_value
    elif sizing_method == "fixed_amount":
        amount = trade_params.get("fixed_amount_usd", sizing_value)
    else:
        amount = capital * 0.03

    if amount < 1.0:
        return None

    price = market.get("yes_price", 0.5) if side == "YES" else market.get("no_price", 0.5)

    return {
        "market_id": market["id"],
        "market_question": market.get("question", ""),
        "side": side,
        "amount_usd": round(amount, 2),
        "price": price,
        "edge": edge,
    }
