"""
One-time script: Update existing strategies to use new Data API signals.
Adds whale tracking, smart money score, OI, and holder concentration rules.
Also fills in skeleton strategies with real entry rules.

Run via: docker exec polymarket-bot python3 scripts/update_strategies_v2.py
"""

import json
import sys
sys.path.insert(0, "/app")

from db import engine


STRATEGY_UPDATES = {
    # Smart Money Follow — now uses real whale data from Data API
    "strat_7461153d": {
        "entry_rules": [
            {"field": "smart_money_score", "op": "gte", "value": 65},
            {"field": "whale_buy_count", "op": "gte", "value": 3},
            {"field": "whale_net_flow", "op": "gt", "value": 0},
            {"field": "volume", "op": "gte", "value": 50000},
        ],
        "trade_params": {
            "side": "YES",
            "sizing_method": "fixed_amount",
            "sizing_value": 10,
            "min_edge": 0.03,
        },
    },

    # Hype Cycle Fade — shorts when volume spikes but smart money is selling
    "strat_84849168": {
        "entry_rules": [
            {"field": "volume_24h", "op": "gte", "value": 50000},
            {"field": "whale_sell_count", "op": "gte", "value": 3},
            {"field": "smart_money_score", "op": "lte", "value": 35},
            {"field": "yes_price", "op": "gte", "value": 0.60},
        ],
        "trade_params": {
            "side": "NO",
            "sizing_method": "fixed_amount",
            "sizing_value": 10,
            "min_edge": 0.03,
        },
    },

    # Low Price Contrarian — buy cheap markets with whale accumulation
    "strat_ebd37a24": {
        "entry_rules": [
            {"field": "yes_price", "op": "lte", "value": 0.25},
            {"field": "volume", "op": "gte", "value": 10000},
            {"field": "whale_buy_count", "op": "gte", "value": 2},
            {"field": "smart_money_score", "op": "gte", "value": 55},
        ],
        "trade_params": {
            "side": "YES",
            "sizing_method": "fixed_amount",
            "sizing_value": 5,
            "min_edge": 0.05,
        },
    },

    # High Confidence Momentum — high-priced markets with rising OI and whale support
    "strat_569b0255": {
        "entry_rules": [
            {"field": "yes_price", "op": "gte", "value": 0.70},
            {"field": "volume_24h", "op": "gte", "value": 10000},
            {"field": "smart_money_score", "op": "gte", "value": 60},
            {"field": "book_imbalance", "op": "gte", "value": 0.1},
        ],
        "trade_params": {
            "side": "YES",
            "sizing_method": "fixed_amount",
            "sizing_value": 10,
            "min_edge": 0.03,
        },
    },

    # Weather Oracle Edge — use holder concentration + OI as proxy for informed trading
    "strat_85bce613": {
        "entry_rules": [
            {"field": "calculated_edge", "op": "gte", "value": 0.10},
            {"field": "top_holder_concentration", "op": "gte", "value": 0.4},
            {"field": "volume", "op": "gte", "value": 5000},
        ],
        "category_filter": ["weather", "climate", "science"],
        "trade_params": {
            "side": "YES",
            "sizing_method": "fixed_amount",
            "sizing_value": 5,
            "min_edge": 0.10,
        },
    },

    # Emotional Fade — add smart money confirmation
    "strat_e264f5e1": {
        "entry_rules": [
            {"field": "sentiment_score", "op": "lt", "value": -0.5},
            {"field": "smart_money_score", "op": "gte", "value": 55},
            {"field": "volume", "op": "gte", "value": 10000},
        ],
        "trade_params": {
            "side": "YES",
            "sizing_method": "fixed_amount",
            "sizing_value": 10,
            "min_edge": 0.05,
        },
    },

    # News Overreaction — add whale buying confirmation
    "strat_b42a9174": {
        "entry_rules": [
            {"field": "sentiment_score", "op": "lt", "value": -0.3},
            {"field": "whale_buy_count", "op": "gte", "value": 2},
            {"field": "whale_net_flow", "op": "gt", "value": 0},
            {"field": "volume", "op": "gte", "value": 10000},
        ],
        "trade_params": {
            "side": "YES",
            "sizing_method": "fixed_amount",
            "sizing_value": 10,
            "min_edge": 0.05,
        },
    },

    # Panic Dip Buy — buy dips confirmed by whale accumulation
    "strat_42230105": {
        "entry_rules": [
            {"field": "yes_price", "op": "lte", "value": 0.30},
            {"field": "whale_buy_count", "op": "gte", "value": 2},
            {"field": "smart_money_score", "op": "gte", "value": 55},
            {"field": "volume", "op": "gte", "value": 10000},
        ],
        "trade_params": {
            "side": "YES",
            "sizing_method": "fixed_amount",
            "sizing_value": 10,
            "min_edge": 0.05,
        },
    },
}

# New strategies based on 288M blockchain trade analysis (strategy_discovery)
NEW_STRATEGIES = [
    {
        "id": "politics_edge",
        "name": "Politics Category Edge",
        "description": "Political markets are systematically underpriced (+3.1% edge on 16.2M historical trades). Buys YES on politics markets in the 20-75¢ range.",
        "category": "politics",
        "discovered_by": "strategy_discovery",
        "definition": {
            "entry_rules": [
                {"field": "yes_price", "op": "gte", "value": 0.20},
                {"field": "yes_price", "op": "lte", "value": 0.75},
                {"field": "volume", "op": "gte", "value": 5000},
                {"field": "liquidity", "op": "gte", "value": 1000},
            ],
            "category_filter": ["politics"],
            "trade_params": {
                "side": "YES",
                "sizing_method": "fixed_amount",
                "fixed_amount_usd": 3.0,
                "min_edge": 0.03,
            },
        },
    },
    {
        "id": "whale_favorites",
        "name": "Whale Favorites Edge",
        "description": "When whales ($500+) buy favorites (50-80¢), hit rate is 2.5-3% above fair price. Based on 2M+ trades from blockchain analysis.",
        "discovered_by": "strategy_discovery",
        "definition": {
            "entry_rules": [
                {"field": "yes_price", "op": "gte", "value": 0.50},
                {"field": "yes_price", "op": "lte", "value": 0.80},
                {"field": "whale_buy_count", "op": "gte", "value": 2},
                {"field": "whale_net_flow", "op": "gt", "value": 0},
                {"field": "volume", "op": "gte", "value": 10000},
                {"field": "liquidity", "op": "gte", "value": 2000},
            ],
            "trade_params": {
                "side": "YES",
                "sizing_method": "fixed_amount",
                "fixed_amount_usd": 3.0,
                "min_edge": 0.025,
            },
        },
    },
]


def main():
    updated = 0
    for strat_id, updates in STRATEGY_UPDATES.items():
        row = engine.query_one(
            "SELECT id, definition FROM strategies WHERE id = ?", (strat_id,)
        )
        if not row:
            print(f"  SKIP {strat_id} — not found")
            continue

        defn = json.loads(row["definition"])

        # Merge updates into definition
        for key, val in updates.items():
            defn[key] = val

        engine.execute(
            "UPDATE strategies SET definition = ?, updated_at = datetime('now') WHERE id = ?",
            (json.dumps(defn), strat_id),
        )
        print(f"  OK {strat_id} — {defn.get('name', '?')}: {len(updates.get('entry_rules', []))} rules")
        updated += 1

    print(f"\nDone: {updated} strategies updated with whale/smart-money signals.")

    # Insert new data-driven strategies
    created = 0
    for strat in NEW_STRATEGIES:
        existing = engine.query_one(
            "SELECT id FROM strategies WHERE id = ?", (strat["id"],)
        )
        if existing:
            print(f"  EXISTS {strat['id']} — {strat['name']} (skipping)")
            continue

        engine.execute(
            """INSERT INTO strategies (id, name, description, definition, status, category,
               discovered_by, created_at, updated_at)
               VALUES (?, ?, ?, ?, 'active', ?, ?, datetime('now'), datetime('now'))""",
            (
                strat["id"],
                strat["name"],
                strat["description"],
                json.dumps(strat["definition"]),
                strat.get("category", ""),
                strat.get("discovered_by", "manual"),
            ),
        )
        print(f"  NEW {strat['id']} — {strat['name']} (status: active)")
        created += 1

    if created:
        print(f"\n{created} new data-driven strategies created.")


if __name__ == "__main__":
    main()
