"""
Portfolio Diversification Module.
Prevents over-concentration in a single category by enforcing:
- Max positions per category
- Max total open positions
- Max exposure (USD) per category as % of capital

Category is derived from Polymarket event tags, slug patterns, and question text.
"""

import logging
from typing import Optional

from config import load_platform_config
from db import engine

logger = logging.getLogger("diversification")

# --------------------------------------------------------------------------
# High-level category classification
# --------------------------------------------------------------------------

# Slug prefix / keyword -> category mapping (order matters: first match wins)
_SLUG_CATEGORY_MAP = [
    # Sports
    ("nba", "Sports"), ("nfl", "Sports"), ("nhl", "Sports"), ("mlb", "Sports"),
    ("epl", "Sports"), ("soccer", "Sports"), ("football", "Sports"),
    ("champions-league", "Sports"), ("la-liga", "Sports"), ("bundesliga", "Sports"),
    ("serie-a", "Sports"), ("ligue-1", "Sports"), ("atp", "Sports"),
    ("wta", "Sports"), ("tennis", "Sports"), ("ufc", "Sports"), ("mma", "Sports"),
    ("boxing", "Sports"), ("cricket", "Sports"), ("dota", "Sports"),
    ("csgo", "Sports"), ("esport", "Sports"), ("league-of-legends", "Sports"),
    ("valorant", "Sports"), ("formula-1", "Sports"), ("f1-", "Sports"),
    ("nascar", "Sports"), ("golf", "Sports"), ("pga", "Sports"),
    ("stanley-cup", "Sports"), ("world-cup", "Sports"), ("olympic", "Sports"),
    ("fifa", "Sports"), ("ncaa", "Sports"), ("college-", "Sports"),
    ("spread-", "Sports"), ("match-winner", "Sports"),
    # Politics
    ("president", "Politics"), ("election", "Politics"), ("democrat", "Politics"),
    ("republican", "Politics"), ("senate", "Politics"), ("congress", "Politics"),
    ("governor", "Politics"), ("trump", "Politics"), ("biden", "Politics"),
    ("parliament", "Politics"), ("prime-minister", "Politics"),
    ("geopolitic", "Politics"), ("nato", "Politics"), ("war-", "Politics"),
    ("ukraine", "Politics"), ("china-", "Politics"), ("russia-", "Politics"),
    ("iran-", "Politics"), ("israel-", "Politics"), ("venezuela", "Politics"),
    ("immigration", "Politics"), ("tariff", "Politics"),
    # Economics / Fed
    ("fed-", "Economics"), ("fed-rate", "Economics"), ("interest-rate", "Economics"),
    ("inflation", "Economics"), ("gdp", "Economics"), ("recession", "Economics"),
    ("economy", "Economics"), ("unemployment", "Economics"), ("cpi-", "Economics"),
    ("stock-market", "Economics"), ("s-p-500", "Economics"), ("nasdaq", "Economics"),
    ("dow-jones", "Economics"), ("treasury", "Economics"),
    # Crypto
    ("bitcoin", "Crypto"), ("btc-", "Crypto"), ("ethereum", "Crypto"),
    ("eth-", "Crypto"), ("crypto", "Crypto"), ("solana", "Crypto"),
    ("sol-", "Crypto"), ("defi", "Crypto"), ("nft", "Crypto"),
    ("memecoin", "Crypto"), ("dogecoin", "Crypto"),
    # Weather
    ("weather", "Weather"), ("temperature", "Weather"), ("climate", "Weather"),
    ("hurricane", "Weather"), ("tornado", "Weather"), ("rainfall", "Weather"),
    ("snowfall", "Weather"),
    # Science & Tech
    ("ai-", "Science & Tech"), ("artificial-intelligence", "Science & Tech"),
    ("spacex", "Science & Tech"), ("nasa", "Science & Tech"),
    ("space-", "Science & Tech"), ("tech-", "Science & Tech"),
    ("apple-", "Science & Tech"), ("google-", "Science & Tech"),
    ("meta-", "Science & Tech"), ("openai", "Science & Tech"),
    ("gta-", "Entertainment"), ("movie-", "Entertainment"),
    ("oscar", "Entertainment"), ("grammy", "Entertainment"),
    ("super-bowl", "Entertainment"), ("emmy", "Entertainment"),
    ("tv-", "Entertainment"), ("game-", "Entertainment"),
    ("release", "Entertainment"),
]

# Event tag label -> high-level category (from Polymarket event tags)
_TAG_CATEGORY_MAP = {
    "sports": "Sports", "soccer": "Sports", "basketball": "Sports",
    "nba": "Sports", "nfl": "Sports", "nhl": "Sports", "mlb": "Sports",
    "tennis": "Sports", "cricket": "Sports", "mma": "Sports",
    "esports": "Sports", "golf": "Sports", "formula 1": "Sports",
    "politics": "Politics", "elections": "Politics", "geopolitics": "Politics",
    "us election": "Politics", "global elections": "Politics",
    "world elections": "Politics",
    "economy": "Economics", "fed rates": "Economics", "fed": "Economics",
    "economic policy": "Economics",
    "crypto": "Crypto", "bitcoin": "Crypto", "ethereum": "Crypto",
    "defi": "Crypto",
    "weather": "Weather", "climate": "Weather",
    "science": "Science & Tech", "technology": "Science & Tech",
    "ai": "Science & Tech", "space": "Science & Tech",
    "entertainment": "Entertainment", "music": "Entertainment",
    "movies": "Entertainment", "gaming": "Entertainment",
}


def classify_category(slug: str = "", question: str = "",
                       event_tags: list = None) -> str:
    """Derive a high-level category for a market.

    Priority:
    1. Event tags from Gamma API (most reliable)
    2. Slug pattern matching
    3. Question text keyword matching
    4. "Other" fallback
    """
    # 1. Try event tags first
    if event_tags:
        for tag in event_tags:
            label = (tag.get("label") or "").lower().strip()
            if label in _TAG_CATEGORY_MAP:
                return _TAG_CATEGORY_MAP[label]

    # 2. Slug-based matching
    slug_lower = (slug or "").lower()
    for pattern, cat in _SLUG_CATEGORY_MAP:
        if pattern in slug_lower:
            return cat

    # 3. Question-based matching (less reliable, broader patterns)
    q_lower = (question or "").lower()
    question_hints = [
        ("temperature", "Weather"), ("\u00b0f", "Weather"), ("\u00b0c", "Weather"),
        ("rainfall", "Weather"), ("hurricane", "Weather"),
        ("bitcoin", "Crypto"), ("ethereum", "Crypto"), ("btc", "Crypto"),
        ("interest rate", "Economics"), ("fed ", "Economics"), ("gdp", "Economics"),
        ("inflation", "Economics"),
        ("president", "Politics"), ("election", "Politics"), ("congress", "Politics"),
        ("prime minister", "Politics"), ("governor", "Politics"),
        ("nba", "Sports"), ("nfl", "Sports"), ("nhl", "Sports"),
        ("stanley cup", "Sports"), ("champions league", "Sports"),
        ("premier league", "Sports"), ("world cup", "Sports"),
        ("gta ", "Entertainment"), ("released", "Entertainment"),
    ]
    for pattern, cat in question_hints:
        if pattern in q_lower:
            return cat

    return "Other"


# --------------------------------------------------------------------------
# Diversification checks
# --------------------------------------------------------------------------

def _get_diversification_config() -> dict:
    """Load diversification config from platform_config.yaml."""
    cfg = load_platform_config()
    return cfg.get("diversification", {
        "max_positions_per_category": 3,
        "max_positions_total": 15,
        "max_exposure_per_category_pct": 30,
        "preferred_categories": [],
    })


def _get_open_positions() -> list[dict]:
    """Get all currently open positions with their category."""
    return engine.query(
        "SELECT t.id, t.market_id, t.market_question, t.amount_usd, t.side, "
        "       m.category, m.slug, m.question as m_question "
        "FROM trades t "
        "LEFT JOIN markets m ON t.market_id = m.id "
        "WHERE t.status = 'executed' "
        "  AND (t.result IS NULL OR t.result = 'open') "
        "  AND t.amount_usd > 0"
    )


def get_portfolio_summary() -> dict:
    """Return current portfolio breakdown by category.

    Returns:
        {
            "total_positions": int,
            "total_exposure_usd": float,
            "by_category": {
                "Sports": {"count": 3, "exposure_usd": 15.0},
                ...
            }
        }
    """
    positions = _get_open_positions()
    by_cat: dict = {}
    total_exposure = 0.0

    known_cats = {"Sports", "Politics", "Economics", "Crypto", "Weather",
                  "Science & Tech", "Entertainment", "Other"}

    for pos in positions:
        cat = pos.get("category") or "Other"
        # Reclassify if category looks like a groupItemTitle (not a proper category)
        if cat not in known_cats:
            cat = classify_category(
                slug=pos.get("slug", ""),
                question=pos.get("m_question") or pos.get("market_question", ""),
            )

        if cat not in by_cat:
            by_cat[cat] = {"count": 0, "exposure_usd": 0.0}
        by_cat[cat]["count"] += 1
        by_cat[cat]["exposure_usd"] += pos.get("amount_usd", 0)
        total_exposure += pos.get("amount_usd", 0)

    return {
        "total_positions": len(positions),
        "total_exposure_usd": total_exposure,
        "by_category": by_cat,
    }


def check_diversification(market_category: str, trade_amount_usd: float) -> tuple:
    """Check if a new trade passes diversification rules.

    Args:
        market_category: High-level category of the market (e.g. "Sports")
        trade_amount_usd: Amount of the proposed trade in USD

    Returns:
        (allowed, reason) -- if allowed=False, reason explains why
    """
    div_cfg = _get_diversification_config()
    max_per_cat = div_cfg.get("max_positions_per_category", 3)
    max_total = div_cfg.get("max_positions_total", 15)
    max_exposure_pct = div_cfg.get("max_exposure_per_category_pct", 30)

    # Get capital from trading config
    platform_cfg = load_platform_config()
    capital = platform_cfg.get("trading", {}).get("capital_usd", 100)

    summary = get_portfolio_summary()
    cat_data = summary["by_category"].get(market_category, {"count": 0, "exposure_usd": 0.0})

    # 1. Max total positions
    if summary["total_positions"] >= max_total:
        msg = (f"Diversification: max total positions reached "
               f"({summary['total_positions']}/{max_total})")
        logger.info(msg)
        return False, msg

    # 2. Max positions per category
    if cat_data["count"] >= max_per_cat:
        msg = (f"Diversification: max positions in '{market_category}' reached "
               f"({cat_data['count']}/{max_per_cat})")
        logger.info(msg)
        return False, msg

    # 3. Max exposure per category (as % of capital)
    new_exposure = cat_data["exposure_usd"] + trade_amount_usd
    max_exposure_usd = capital * (max_exposure_pct / 100.0)
    if new_exposure > max_exposure_usd:
        msg = (f"Diversification: exposure in '{market_category}' would be "
               f"${new_exposure:.2f} > ${max_exposure_usd:.2f} "
               f"({max_exposure_pct}% of ${capital:.0f})")
        logger.info(msg)
        return False, msg

    return True, "OK"
