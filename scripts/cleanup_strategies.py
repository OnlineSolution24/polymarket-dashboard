"""
Strategy Cleanup: Retire broken strategies, update descriptions for keepers.

After audit of 17 active strategies:
- 10 depend on `calculated_edge` which is NULL for 95% of markets → never trigger
- 6 are viable and get updated descriptions + strategy_edge fallback

Run via: docker exec polymarket-bot python3 scripts/cleanup_strategies.py
"""

import json
import sys
sys.path.insert(0, "/app")

from db import engine


# ─── RETIRE: These strategies can never trigger ───────────────────────────────
RETIRE_IDS = [
    "strat_simple_edge",      # calculated_edge ≥ 0.03 → NULL
    "strat_low_price_simple",  # calculated_edge ≥ 0.03 → NULL
    "strat_fedwatch",          # calculated_edge ≥ 0.05 → NULL
    "strat_odds_edge",         # calculated_edge ≥ 0.05 → NULL
    "strat_live_sports",       # calculated_edge ≥ 0.05 → NULL
    "strat_85bce613",          # Weather Forecast Edge — redundant with weather_edge job
    "strat_weather_no",        # Weather NO Sniper — redundant
    "strat_sports_no",         # Backtest was negative (-$2.95)
    "strat_econ_no",           # Backtest was negative (-$4.22)
    "strat_politics_no",       # Contradicts politics_edge; calculated_edge → NULL
]

# ─── UPDATE: Viable strategies with full descriptions + strategy_edge ─────────
STRATEGY_UPDATES = {
    "politics_edge": {
        "name": "Politics YES Bias",
        "description": (
            "Politische Märkte sind systematisch unterbewertet. Analyse von 16.2M Trades "
            "zeigt +3.1% Edge wenn man YES kauft. Grund: Die Crowd beantwortet politische "
            "Fragen eher mit Nein als die Realität zeigt.\n\n"
            "Edge: +3.1% | Basis: 16.2M Trades (388M Gesamt-DB)\n"
            "Preis-Range: 20-75¢ | Min. Volume: $5.000 | Min. Liquidity: $1.000"
        ),
        "definition_updates": {
            "trade_params": {
                "side": "YES",
                "sizing_method": "fixed_amount",
                "fixed_amount_usd": 3.0,
                "min_edge": 0.03,
                "strategy_edge": 0.031,
            },
        },
    },
    "whale_favorites": {
        "name": "Whale Favorites (50-80¢)",
        "description": (
            "Wenn Whales ($500+) und Medium-Trader ($50-500) bei Favoriten (50-80¢) kaufen, "
            "liegt die Hit-Rate 2.5-3% über Fair Value. Nur auf Top-20-Märkten aktiv "
            "(Whale-Daten nur dort verfügbar).\n\n"
            "Edge: +2.5-3% | Basis: 2M+ Trades (388M Gesamt-DB)\n"
            "Preis-Range: 50-80¢ | Min. Whale Buys: 2 | Min. Volume: $10.000"
        ),
        "definition_updates": {
            "trade_params": {
                "side": "YES",
                "sizing_method": "fixed_amount",
                "fixed_amount_usd": 3.0,
                "min_edge": 0.025,
                "strategy_edge": 0.028,
            },
        },
    },
    "contrarian_whale": {
        "name": "Contrarian Whale Signal",
        "description": (
            "Wenn Whales ($500+) auf einer Seite dominieren und kaum Retail-Aktivität "
            "vorhanden ist, gewinnt die Whale-Seite in 70-95% der Fälle. Stärkstes "
            "Signal aus der Discovery v2.\n\n"
            "Edge: +20-45% | Basis: 185K+ Outcomes (388M Gesamt-DB)\n"
            "Min. Whale Buys: 3 | Smart Money Score: ≥65 | Min. Volume: $5.000"
        ),
        "definition_updates": {
            "trade_params": {
                "side": "YES",
                "sizing_method": "fixed_amount",
                "fixed_amount_usd": 5.0,
                "min_edge": 0.05,
                "strategy_edge": 0.20,
            },
        },
    },
    "whale_cluster_mega": {
        "name": "Whale Mega-Cluster",
        "description": (
            "Wenn 5+ Whale-Trades ($500+) innerhalb von ~30 Minuten auf der gleichen "
            "Seite eines Markts landen, gewinnt diese Seite in 67.7% der Fälle. "
            "Clustering = koordinierte Information.\n\n"
            "Edge: +17.7% | Basis: 82K+ Outcomes (388M Gesamt-DB)\n"
            "Min. Whale Buys: 5 | Min. Volume: $10.000 | Min. Liquidity: $2.000"
        ),
        "definition_updates": {
            "trade_params": {
                "side": "YES",
                "sizing_method": "fixed_amount",
                "fixed_amount_usd": 4.0,
                "min_edge": 0.04,
                "strategy_edge": 0.177,
            },
        },
    },
    "strat_resolution_sniper": {
        "name": "Resolution Sniper",
        "description": (
            "Kauft nahe am Markt-Ende (≤3 Tage) wenn das Ergebnis sehr wahrscheinlich ist "
            "(≥85¢). Nutzt die Informationsklarheit kurz vor Resolution. Historisch: "
            "100% Win-Rate (10 Trades, +$59).\n\n"
            "Edge: ~15% | Preis: ≥85¢ | Max. Restlaufzeit: 3 Tage\n"
            "Min. Volume: $10.000 | Min. Liquidity: $2.000"
        ),
        "definition_updates": {
            "trade_params": {
                "side": "YES",
                "sizing_method": "fixed_amount",
                "fixed_amount_usd": 5.0,
                "min_edge": 0.02,
                "strategy_edge": 0.15,
            },
        },
    },
}


def main():
    # Step 1: Retire broken strategies
    retired = 0
    for strat_id in RETIRE_IDS:
        row = engine.query_one("SELECT id, name, status FROM strategies WHERE id = ?", (strat_id,))
        if not row:
            print(f"  SKIP {strat_id} — not found")
            continue
        if row["status"] == "retired":
            print(f"  ALREADY RETIRED {strat_id} — {row['name']}")
            continue

        engine.execute(
            "UPDATE strategies SET status = 'retired', updated_at = datetime('now') WHERE id = ?",
            (strat_id,),
        )
        print(f"  RETIRED {strat_id} — {row['name']}")
        retired += 1

    print(f"\n{retired} strategies retired.\n")

    # Step 2: Update descriptions and add strategy_edge to keepers
    updated = 0
    for strat_id, updates in STRATEGY_UPDATES.items():
        row = engine.query_one("SELECT id, name, definition FROM strategies WHERE id = ?", (strat_id,))
        if not row:
            print(f"  SKIP {strat_id} — not found")
            continue

        # Update name and description
        engine.execute(
            "UPDATE strategies SET name = ?, description = ?, updated_at = datetime('now') WHERE id = ?",
            (updates["name"], updates["description"], strat_id),
        )

        # Merge definition updates (preserving entry_rules, category_filter)
        defn = json.loads(row["definition"]) if isinstance(row["definition"], str) else row["definition"]
        for key, val in updates.get("definition_updates", {}).items():
            defn[key] = val

        engine.execute(
            "UPDATE strategies SET definition = ? WHERE id = ?",
            (json.dumps(defn), strat_id),
        )
        print(f"  UPDATED {strat_id} — {updates['name']}")
        updated += 1

    print(f"\n{updated} strategies updated with descriptions + strategy_edge.\n")

    # Step 3: Show final state
    print("=== ACTIVE STRATEGIES ===")
    rows = engine.query("SELECT id, name, status FROM strategies WHERE status = 'active' ORDER BY name")
    for r in rows:
        print(f"  {r['id']} | {r['name']}")
    print(f"\nTotal active: {len(rows)}")


if __name__ == "__main__":
    main()
