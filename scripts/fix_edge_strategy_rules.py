"""
Fix the 4 edge strategies with broken entry_rules format.
They were stored as dicts instead of lists of rule objects.

Run via: docker exec polymarket-bot python3 scripts/fix_edge_strategy_rules.py
"""

import json
import sys
sys.path.insert(0, "/app")

from db import engine

# Map strategy names to their proper list-format entry_rules
# These use fields that ACTUALLY exist in the markets table
STRATEGY_FIXES = {
    "strat_odds_edge": {
        "entry_rules": [
            {"field": "calculated_edge", "op": "gte", "value": 0.05},
            {"field": "volume", "op": "gte", "value": 5000},
            {"field": "yes_price", "op": "gte", "value": 0.10},
            {"field": "yes_price", "op": "lte", "value": 0.85},
        ],
        "category_filter": ["Sports"],
        "trade_params": {
            "side": "YES",
            "sizing_method": "fixed_amount",
            "sizing_value": 5,
            "min_edge": 0.05,
            "max_amount": 5.0,
        },
    },
    "strat_crypto_prob": {
        "entry_rules": [
            {"field": "calculated_edge", "op": "gte", "value": 0.05},
            {"field": "volume", "op": "gte", "value": 5000},
            {"field": "yes_price", "op": "gte", "value": 0.10},
            {"field": "yes_price", "op": "lte", "value": 0.85},
        ],
        "category_filter": ["Crypto"],
        "trade_params": {
            "side": "YES",
            "sizing_method": "fixed_amount",
            "sizing_value": 5,
            "min_edge": 0.05,
            "max_amount": 5.0,
        },
    },
    "strat_fedwatch": {
        "entry_rules": [
            {"field": "calculated_edge", "op": "gte", "value": 0.05},
            {"field": "volume", "op": "gte", "value": 5000},
            {"field": "yes_price", "op": "gte", "value": 0.10},
            {"field": "yes_price", "op": "lte", "value": 0.85},
        ],
        "category_filter": ["Economics", "Politics"],
        "trade_params": {
            "side": "YES",
            "sizing_method": "fixed_amount",
            "sizing_value": 5,
            "min_edge": 0.05,
            "max_amount": 5.0,
        },
    },
    "strat_cross_platform": {
        "entry_rules": [
            {"field": "calculated_edge", "op": "gte", "value": 0.05},
            {"field": "volume", "op": "gte", "value": 5000},
            {"field": "yes_price", "op": "gte", "value": 0.10},
            {"field": "yes_price", "op": "lte", "value": 0.85},
        ],
        "trade_params": {
            "side": "YES",
            "sizing_method": "fixed_amount",
            "sizing_value": 5,
            "min_edge": 0.05,
            "max_amount": 5.0,
        },
    },
}


def main():
    fixed = 0
    for strat_id, updates in STRATEGY_FIXES.items():
        row = engine.query_one(
            "SELECT id, definition FROM strategies WHERE id = ?", (strat_id,)
        )
        if not row:
            print(f"  SKIP {strat_id} — not found in DB")
            continue

        defn = json.loads(row["definition"]) if isinstance(row["definition"], str) else row["definition"]

        # Show old rules for comparison
        old_rules = defn.get("entry_rules", "MISSING")
        old_type = type(old_rules).__name__
        print(f"  {strat_id}: old entry_rules type={old_type}")

        # Merge updates
        for key, val in updates.items():
            defn[key] = val

        engine.execute(
            "UPDATE strategies SET definition = ?, updated_at = datetime('now') WHERE id = ?",
            (json.dumps(defn), strat_id),
        )

        new_rules = defn["entry_rules"]
        print(f"  FIXED {strat_id}: {len(new_rules)} proper rules")
        fixed += 1

    print(f"\nDone: {fixed} strategies fixed (dict -> list format).")


if __name__ == "__main__":
    main()
