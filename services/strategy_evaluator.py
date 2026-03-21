"""
Strategy Rule Evaluator.
Evaluates strategy entry/exit rules against market data.
Used by both the backtest service and the REST API for signal matching.
"""

import logging
from datetime import datetime
from typing import Optional

from db import engine

logger = logging.getLogger("strategy_evaluator")


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


def _load_market_filters() -> dict:
    """Load global market filters from platform_config.yaml."""
    from config import load_platform_config
    cfg = load_platform_config()
    trading = cfg.get("trading", {})
    return trading.get("market_filters", {})


def _passes_global_filters(market: dict, filters: dict):
    """Check if market passes global filters. Returns (bool, str)."""
    min_price = filters.get("min_price", 0)
    max_price = filters.get("max_price", 1.0)
    min_volume = filters.get("min_volume_24h", 0)
    min_liq = filters.get("min_liquidity", 0)
    min_depth = filters.get("min_orderbook_depth", 0)

    yes_price = market.get("yes_price", 0) or 0
    volume = market.get("volume_24h") or market.get("volume", 0) or 0
    liquidity = market.get("liquidity", 0) or 0
    bid_depth = market.get("bid_depth", 0) or 0

    if yes_price < min_price:
        return False, "price %.4f < min %.2f" % (yes_price, min_price)
    if yes_price > max_price:
        return False, "price %.4f > max %.2f" % (yes_price, max_price)
    if volume < min_volume:
        return False, "volume $%.0f < min $%.0f" % (volume, min_volume)
    if liquidity < min_liq:
        return False, "liquidity $%.0f < min $%.0f" % (liquidity, min_liq)
    if min_depth > 0 and bid_depth < min_depth:
        return False, "bid_depth %s < min %s" % (bid_depth, min_depth)

    return True, ""


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

    # Load global market filters from platform config
    global_filters = _load_market_filters()

    markets = engine.query(query, tuple(params))
    if not markets:
        return []

    matched = []
    skipped = 0
    for market in markets:
        # Apply global filters BEFORE strategy-specific rules
        passed, reason = _passes_global_filters(market, global_filters)
        if not passed:
            logger.debug(
                "SKIP [global filter] %s: %s",
                str(market.get("question", market.get("id", "?")))[:60],
                reason,
            )
            skipped += 1
            continue

        if evaluate_rules(market, entry_rules):
            # Diversification check: skip if category is over-concentrated
            try:
                from services.diversification import classify_category, check_diversification
                cat = market.get("category") or ""
                known_cats = {"Sports", "Politics", "Economics", "Crypto", "Weather",
                              "Science & Tech", "Entertainment", "Other"}
                if cat not in known_cats:
                    cat = classify_category(
                        slug=market.get("slug", ""),
                        question=market.get("question", ""),
                    )
                div_ok, div_reason = check_diversification(cat, 1.0)  # preliminary check with $1
                if not div_ok:
                    logger.info("SKIP [diversification] %s: %s",
                                str(market.get("question", "?"))[:60], div_reason)
                    skipped += 1
                    continue
            except Exception as e:
                logger.debug("Diversification check skipped: %s", e)

            matched.append(market)

    if skipped > 0:
        logger.info(
            "Global filters skipped %d/%d markets (min_price=%.2f, max_price=%.2f, min_vol=$%.0f, min_liq=$%.0f)",
            skipped, len(markets),
            global_filters.get("min_price", 0), global_filters.get("max_price", 1),
            global_filters.get("min_volume_24h", 0), global_filters.get("min_liquidity", 0),
        )

    return matched


def compute_trade_params(market: dict, trade_params: dict, capital: float = 100.0) -> Optional[dict]:
    """Compute trade details for a matching market.

    Returns dict with side, amount_usd, or None if trade not viable.
    Enforces per-strategy max_amount and global max_position_pct from config.
    """
    import logging
    from config import load_platform_config

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

    # --- Enforce limits from platform config ---
    platform_cfg = load_platform_config()
    trading_cfg = platform_cfg.get("trading", {})
    limits = trading_cfg.get("limits", {})

    # Global max position: max_position_pct of capital
    max_position_pct = limits.get("max_position_pct", 5)
    max_position_usd = capital * (max_position_pct / 100.0)

    # Per-strategy max_amount override (e.g. weather strategies may set max_amount=5)
    strategy_max = trade_params.get("max_amount", None)

    # Weather strategy cap: use weather-specific limit if category matches
    category = (market.get("category") or "").lower()
    is_weather = "weather" in category or "weather" in (market.get("question") or "").lower()
    if is_weather:
        # Weather strategies capped at $5 (or strategy-specific max if lower)
        weather_cap = 5.0
        if strategy_max is not None:
            weather_cap = min(weather_cap, strategy_max)
        if amount > weather_cap:
            logging.getLogger("strategy_evaluator").warning(
                f"Weather trade capped: ${amount:.2f} -> ${weather_cap:.2f}"
            )
            amount = weather_cap
    elif strategy_max is not None and amount > strategy_max:
        logging.getLogger("strategy_evaluator").warning(
            f"Strategy max_amount cap: ${amount:.2f} -> ${strategy_max:.2f}"
        )
        amount = strategy_max

    # Global position cap
    if amount > max_position_usd:
        logging.getLogger("strategy_evaluator").warning(
            f"Position size cap ({max_position_pct}% of ${capital:.0f}): ${amount:.2f} -> ${max_position_usd:.2f}"
        )
        amount = max_position_usd

    if amount < 1.0:
        return None

    # --- Diversification check ---
    try:
        from services.diversification import classify_category, check_diversification
        cat = (market.get("category") or "")
        known_cats = {"Sports", "Politics", "Economics", "Crypto", "Weather",
                      "Science & Tech", "Entertainment", "Other"}
        if cat not in known_cats:
            cat = classify_category(
                slug=market.get("slug", ""),
                question=market.get("question", ""),
            )
        div_ok, div_reason = check_diversification(cat, amount)
        if not div_ok:
            logging.getLogger("strategy_evaluator").info(
                f"Trade blocked by diversification: {div_reason} | {market.get('question', '')[:50]}"
            )
            return None
    except Exception as e:
        logging.getLogger("strategy_evaluator").debug(f"Diversification check skipped: {e}")

    price = market.get("yes_price", 0.5) if side == "YES" else market.get("no_price", 0.5)

    return {
        "market_id": market["id"],
        "market_question": market.get("question", ""),
        "side": side,
        "amount_usd": round(amount, 2),
        "price": price,
        "edge": edge,
    }

