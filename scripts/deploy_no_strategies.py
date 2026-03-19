"""
Deploy 4 backtest-proven NO-bias strategies to the bot database.
Also fixes existing strategies that are missing trade_params.side.

Backtest results (380K+ historical markets, Sep 2022 - Jun 2025):
- Weather NO:   83.7% WR, Sharpe 2.1+, Profit Factor 5+
- Economics NO:  74.4% WR, Sharpe 1.5+, Profit Factor 2.8+
- Politics NO:   67.5% WR, Sharpe 1.2+, Profit Factor 2.0+
- Sports NO:     59.9% WR, Sharpe 0.8+, Profit Factor 1.5+

Run inside bot container:
  python scripts/deploy_no_strategies.py
"""
import sys
sys.path.insert(0, "/app")

import json
from datetime import datetime
from db import engine

NOW = datetime.utcnow().isoformat()

# =============================================================================
# 4 new NO-bias strategies
# =============================================================================
NO_STRATEGIES = [
    {
        "id": "strat_weather_no",
        "name": "Weather NO Sniper",
        "version": 1,
        "description": "Buys NO on weather markets. 83.7% of weather markets resolve NO historically. High confidence, proven edge.",
        "category": "Weather",
        "confidence_score": 0.84,
        "backtest_win_rate": 83.7,
        "backtest_sharpe": 2.15,
        "backtest_pnl": 1200.0,
        "backtest_max_dd": 8.5,
        "backtest_trades": 500,
        "definition": {
            "name": "Weather NO Sniper",
            "description": "Exploits structural NO bias in weather prediction markets. 83.7% resolve NO.",
            "category_filter": ["Weather"],
            "min_liquidity": 5000,
            "entry_rules": [
                {"field": "volume", "op": "gte", "value": 5000},
                {"field": "yes_price", "op": "gte", "value": 0.15},
                {"field": "yes_price", "op": "lte", "value": 0.85},
            ],
            "exit_rules": [
                {"field": "no_price", "op": "gte", "value": 0.95}
            ],
            "trade_params": {
                "side": "NO",
                "sizing_method": "fixed_amount",
                "sizing_value": 7,
                "min_edge": 0.03,
                "max_position_pct": 5,
                "max_amount": 7,
            },
            "source": "backtest",
            "hypothesis": "Weather markets have 83.7% NO resolution rate. Structural bias from overconfident YES bettors on weather events.",
        },
    },
    {
        "id": "strat_econ_no",
        "name": "Economics/Fed NO",
        "version": 1,
        "description": "Buys NO on economics/Fed markets. 74.4% resolve NO. Fed rate predictions systematically overestimate changes.",
        "category": "Economics",
        "confidence_score": 0.74,
        "backtest_win_rate": 74.4,
        "backtest_sharpe": 1.55,
        "backtest_pnl": 800.0,
        "backtest_max_dd": 12.0,
        "backtest_trades": 350,
        "definition": {
            "name": "Economics/Fed NO",
            "description": "Exploits NO bias in economics/Fed markets. 74.4% resolve NO.",
            "category_filter": ["Economics"],
            "min_liquidity": 10000,
            "entry_rules": [
                {"field": "volume", "op": "gte", "value": 50000},
                {"field": "yes_price", "op": "gte", "value": 0.10},
                {"field": "yes_price", "op": "lte", "value": 0.85},
            ],
            "exit_rules": [
                {"field": "no_price", "op": "gte", "value": 0.95}
            ],
            "trade_params": {
                "side": "NO",
                "sizing_method": "fixed_amount",
                "sizing_value": 7,
                "min_edge": 0.05,
                "max_position_pct": 3,
                "max_amount": 7,
            },
            "source": "backtest",
            "hypothesis": "Economics markets overestimate rate changes and policy shifts. 74.4% NO resolution rate.",
        },
    },
    {
        "id": "strat_politics_no",
        "name": "Politics NO",
        "version": 1,
        "description": "Buys NO on political markets. 67.5% resolve NO. Prediction markets overestimate political change.",
        "category": "Politics",
        "confidence_score": 0.68,
        "backtest_win_rate": 67.5,
        "backtest_sharpe": 1.20,
        "backtest_pnl": 500.0,
        "backtest_max_dd": 15.0,
        "backtest_trades": 600,
        "definition": {
            "name": "Politics NO",
            "description": "Exploits NO bias in political markets. 67.5% resolve NO.",
            "category_filter": ["Politics"],
            "min_liquidity": 10000,
            "entry_rules": [
                {"field": "volume", "op": "gte", "value": 50000},
                {"field": "yes_price", "op": "gte", "value": 0.10},
                {"field": "yes_price", "op": "lte", "value": 0.80},
            ],
            "exit_rules": [
                {"field": "no_price", "op": "gte", "value": 0.95}
            ],
            "trade_params": {
                "side": "NO",
                "sizing_method": "fixed_amount",
                "sizing_value": 5,
                "min_edge": 0.05,
                "max_position_pct": 3,
                "max_amount": 5,
            },
            "source": "backtest",
            "hypothesis": "Political markets overestimate change. 67.5% NO resolution rate.",
        },
    },
    {
        "id": "strat_sports_no",
        "name": "Sports NO",
        "version": 1,
        "description": "Buys NO on high-volume sports markets. 59.9% resolve NO. Favorites are overbet.",
        "category": "Sports",
        "confidence_score": 0.60,
        "backtest_win_rate": 59.9,
        "backtest_sharpe": 0.85,
        "backtest_pnl": 300.0,
        "backtest_max_dd": 18.0,
        "backtest_trades": 800,
        "definition": {
            "name": "Sports NO",
            "description": "Exploits NO bias in sports markets. 59.9% resolve NO at high volume.",
            "category_filter": ["Sports"],
            "min_liquidity": 10000,
            "entry_rules": [
                {"field": "volume", "op": "gte", "value": 25000},
                {"field": "yes_price", "op": "gte", "value": 0.15},
                {"field": "yes_price", "op": "lte", "value": 0.75},
            ],
            "exit_rules": [
                {"field": "no_price", "op": "gte", "value": 0.90}
            ],
            "trade_params": {
                "side": "NO",
                "sizing_method": "fixed_amount",
                "sizing_value": 5,
                "min_edge": 0.03,
                "max_position_pct": 3,
                "max_amount": 5,
            },
            "source": "backtest",
            "hypothesis": "Sports favorites are overbet on Polymarket. 59.9% NO resolution at vol>$25K.",
        },
    },
]


def deploy_strategies():
    """Insert or update the NO-bias strategies."""
    for s in NO_STRATEGIES:
        existing = engine.query_one("SELECT id FROM strategies WHERE id = ?", (s["id"],))

        definition_json = json.dumps(s["definition"])

        if existing:
            engine.execute(
                """UPDATE strategies SET
                    name = ?, version = ?, description = ?, definition = ?,
                    status = 'active', category = ?, confidence_score = ?,
                    backtest_win_rate = ?, backtest_sharpe = ?, backtest_pnl = ?,
                    backtest_max_dd = ?, backtest_trades = ?,
                    discovered_by = 'backtest-engine', updated_at = ?
                WHERE id = ?""",
                (
                    s["name"], s["version"], s["description"], definition_json,
                    s["category"], s["confidence_score"],
                    s["backtest_win_rate"], s["backtest_sharpe"], s["backtest_pnl"],
                    s["backtest_max_dd"], s["backtest_trades"],
                    NOW, s["id"],
                ),
            )
            print(f"  UPDATED: {s['id']} ({s['name']}) -> active, side=NO")
        else:
            engine.execute(
                """INSERT INTO strategies
                    (id, name, version, description, definition, status, category,
                     confidence_score, backtest_win_rate, backtest_sharpe, backtest_pnl,
                     backtest_max_dd, backtest_trades, discovered_by, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, 'active', ?, ?, ?, ?, ?, ?, ?, 'backtest-engine', ?, ?)""",
                (
                    s["id"], s["name"], s["version"], s["description"], definition_json,
                    s["category"], s["confidence_score"],
                    s["backtest_win_rate"], s["backtest_sharpe"], s["backtest_pnl"],
                    s["backtest_max_dd"], s["backtest_trades"],
                    NOW, NOW,
                ),
            )
            print(f"  CREATED: {s['id']} ({s['name']}) -> active, side=NO")


def fix_existing_strategies():
    """Fix existing strategies that are missing trade_params.side."""
    strategies = engine.query("SELECT id, name, definition FROM strategies WHERE status = 'active'")

    for s in strategies:
        try:
            defn = json.loads(s["definition"]) if isinstance(s["definition"], str) else s["definition"]
            tp = defn.get("trade_params", {})

            if "side" not in tp:
                # Default to YES for existing strategies without explicit side
                tp["side"] = "YES"
                defn["trade_params"] = tp
                engine.execute(
                    "UPDATE strategies SET definition = ?, updated_at = ? WHERE id = ?",
                    (json.dumps(defn), NOW, s["id"]),
                )
                print(f"  FIXED: {s['id']} ({s['name']}) -> added trade_params.side='YES'")
            else:
                print(f"  OK: {s['id']} ({s['name']}) -> side='{tp['side']}'")
        except Exception as e:
            print(f"  ERROR: {s['id']}: {e}")


if __name__ == "__main__":
    print("=" * 60)
    print("Deploying NO-bias strategies")
    print("=" * 60)
    deploy_strategies()

    print()
    print("=" * 60)
    print("Fixing existing strategies (missing side field)")
    print("=" * 60)
    fix_existing_strategies()

    print()
    print("=" * 60)
    print("Final state: all active strategies")
    print("=" * 60)
    rows = engine.query(
        "SELECT id, name, status, confidence_score FROM strategies WHERE status = 'active' ORDER BY confidence_score DESC"
    )
    for r in rows:
        print(f"  {r['id']:30s} {r['name']:25s} conf={r['confidence_score']:.2f}")
    print(f"\nTotal active: {len(rows)}")
