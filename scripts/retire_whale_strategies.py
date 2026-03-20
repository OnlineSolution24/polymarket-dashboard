"""
Retire whale-based strategies after Kelly backtest v2 showed bankruptcy.

Backtest result: All whale strategies (contrarian_whale, whale_cluster_mega)
ended at ~$1.50 from $1,000 start capital. Entry prices ~0.67 provide
insufficient asymmetric payoff. Actual win rate 69-73% vs assumed 70-95%.

Run via: docker exec polymarket-bot python3 scripts/retire_whale_strategies.py
"""

import sys
sys.path.insert(0, "/app")

from db import engine


RETIRE_IDS = [
    "contrarian_whale",      # Backtest: bankrupt. Entry price ~0.67, real win rate ~70%
    "whale_cluster_mega",    # Same whale data, same problem
]


def main():
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

    print(f"\n{retired} whale strategies retired.")

    # Show remaining active strategies
    print("\n=== ACTIVE STRATEGIES ===")
    rows = engine.query("SELECT id, name FROM strategies WHERE status = 'active' ORDER BY name")
    for r in rows:
        print(f"  {r['id']} | {r['name']}")
    print(f"Total active: {len(rows)}")


if __name__ == "__main__":
    main()
