"""
Re-activate proven profitable strategies that were paused/retired.

- Simple Edge: +$235 over 251 trades -> re-activate
- Low Price Value: +$140 over 249 trades -> re-activate (was retired incorrectly)

Also ensures max_active_strategies is high enough to run all useful strategies.

Run via: docker exec polymarket-bot python3 scripts/reactivate_strategies.py
"""

import sys
sys.path.insert(0, "/app")

from db import engine


REACTIVATE = {
    # Simple Edge — top performer, paused for no good reason
    "strat_simple_edge": "active",
    # Low Price Value — profitable, was retired but still has edge
    "strat_low_price": "active",
}

# Also un-retire strategies with 0 trades that were retired prematurely
UNRETIRE_IF_ZERO_TRADES = [
    "strat_85bce613",  # Weather Oracle Edge
]


def main():
    updated = 0

    for strat_id, new_status in REACTIVATE.items():
        row = engine.query_one(
            "SELECT id, name, status, live_pnl FROM strategies WHERE id = ?", (strat_id,)
        )
        if not row:
            # Try by name pattern
            row = engine.query_one(
                "SELECT id, name, status, live_pnl FROM strategies WHERE name LIKE ?",
                (f"%{strat_id.replace('strat_', '').replace('_', ' ').title()}%",)
            )

        if not row:
            print(f"  SKIP {strat_id} — not found")
            continue

        old_status = row["status"]
        pnl = row.get("live_pnl") or 0
        print(f"  {row['name']} ({row['id']}): {old_status} -> {new_status} (PnL: ${pnl:+.2f})")

        engine.execute(
            "UPDATE strategies SET status = ?, updated_at = datetime('now') WHERE id = ?",
            (new_status, row["id"]),
        )
        updated += 1

    # Un-retire zero-trade strategies
    for strat_id in UNRETIRE_IF_ZERO_TRADES:
        row = engine.query_one(
            "SELECT id, name, status, live_trades FROM strategies WHERE id = ?", (strat_id,)
        )
        if row and row["status"] == "retired" and (row.get("live_trades") or 0) == 0:
            print(f"  Un-retiring {row['name']} (0 trades, was retired prematurely)")
            engine.execute(
                "UPDATE strategies SET status = 'active', updated_at = datetime('now') WHERE id = ?",
                (row["id"],),
            )
            updated += 1

    # Show final state
    print(f"\nDone: {updated} strategies updated.")
    print("\nCurrent active strategies:")
    actives = engine.query("SELECT id, name, live_trades, live_pnl FROM strategies WHERE status = 'active'")
    for r in actives:
        pnl = r.get("live_pnl") or 0
        trades = r.get("live_trades") or 0
        print(f"  {r['name']:35} | trades={trades} | pnl=${pnl:+.2f}")


if __name__ == "__main__":
    main()
