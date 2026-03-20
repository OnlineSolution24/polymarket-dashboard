"""
One-time script: Add strategies discovered by Strategy Discovery v2.

New edges from mining 388M blockchain trades (5 new dimensions):
  - Contrarian Whale: When whales buy and retail doesn't → 95.5% hit rate
  - Whale Clustering: 5+ whale trades within ~30min → 67.7% hit rate

Run via: docker exec polymarket-bot python3 scripts/update_strategies_v3.py
"""

import json
import sys
sys.path.insert(0, "/app")

from db import engine


NEW_STRATEGIES = [
    {
        "id": "contrarian_whale",
        "name": "Contrarian Whale Signal",
        "description": (
            "When whales ($500+) dominate buying on one side with minimal retail "
            "activity, that side wins 70-95% of the time. Based on 185K+ outcomes "
            "from 388M blockchain trades. Edge: +20% (whale dominant) to +45% (whale only)."
        ),
        "category": "",
        "discovered_by": "strategy_discovery_v2",
        "definition": {
            "entry_rules": [
                {"field": "whale_buy_count", "op": "gte", "value": 3},
                {"field": "whale_net_flow", "op": "gt", "value": 0},
                {"field": "smart_money_score", "op": "gte", "value": 65},
                {"field": "yes_price", "op": "gte", "value": 0.10},
                {"field": "yes_price", "op": "lte", "value": 0.90},
                {"field": "volume", "op": "gte", "value": 5000},
            ],
            "trade_params": {
                "side": "YES",
                "sizing_method": "fixed_amount",
                "fixed_amount_usd": 5.0,
                "min_edge": 0.05,
            },
        },
    },
    {
        "id": "whale_cluster_mega",
        "name": "Whale Mega-Cluster",
        "description": (
            "When 5+ whale trades ($500+) cluster on the same side within a short "
            "time window (~30 min), that side wins 67.7% of the time. Based on 82K+ "
            "outcomes from 388M blockchain trades. Edge: +17.7%."
        ),
        "category": "",
        "discovered_by": "strategy_discovery_v2",
        "definition": {
            "entry_rules": [
                {"field": "whale_buy_count", "op": "gte", "value": 5},
                {"field": "whale_net_flow", "op": "gt", "value": 0},
                {"field": "yes_price", "op": "gte", "value": 0.10},
                {"field": "yes_price", "op": "lte", "value": 0.90},
                {"field": "volume", "op": "gte", "value": 10000},
                {"field": "liquidity", "op": "gte", "value": 2000},
            ],
            "trade_params": {
                "side": "YES",
                "sizing_method": "fixed_amount",
                "fixed_amount_usd": 4.0,
                "min_edge": 0.04,
            },
        },
    },
]


def main():
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
                strat.get("discovered_by", "strategy_discovery_v2"),
            ),
        )
        print(f"  NEW {strat['id']} — {strat['name']} (status: active)")
        created += 1

    if created:
        print(f"\n{created} new Discovery v2 strategies created.")
    else:
        print("\nNo new strategies created (all already exist).")


if __name__ == "__main__":
    main()
