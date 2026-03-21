#!/usr/bin/env python3
"""Insert new strategies and retire broken ones."""
import sqlite3
import json
import sys

DB_PATH = sys.argv[1] if len(sys.argv) > 1 else "/opt/polymarket-bot/data/dashboard.db"
conn = sqlite3.connect(DB_PATH)
conn.row_factory = sqlite3.Row

# ── 1. Insert contrarian_whale ──
contrarian_def = json.dumps({
    "entry_rules": [
        {"field": "whale_buy_count", "op": ">=", "value": 3},
        {"field": "smart_money_score", "op": ">=", "value": 65},
        {"field": "price", "op": "<=", "value": 0.75},
        {"field": "price", "op": ">=", "value": 0.15},
        {"field": "volume_24h", "op": ">=", "value": 5000}
    ],
    "trade_params": {
        "sizing_method": "fixed_amount",
        "fixed_amount_usd": 5.0,
        "min_edge": 0.05,
        "strategy_edge": 0.20,
        "max_positions": 5
    },
    "method_description": (
        "Contrarian Whale Strategy: Based on analysis of 388M blockchain trades. "
        "Identifies markets where whale traders ($500+ trades) accumulate positions "
        "that diverge from retail sentiment. Historically +20-45% edge when whale_buy_count >= 3 "
        "and smart_money_score >= 65. Uses Strategy Evaluator with 15-min scan cycle."
    )
})

# ── 2. Insert whale_cluster_mega ──
whale_cluster_def = json.dumps({
    "entry_rules": [
        {"field": "whale_buy_count", "op": ">=", "value": 5},
        {"field": "price", "op": "<=", "value": 0.80},
        {"field": "price", "op": ">=", "value": 0.10},
        {"field": "volume_24h", "op": ">=", "value": 3000}
    ],
    "trade_params": {
        "sizing_method": "fixed_amount",
        "fixed_amount_usd": 4.0,
        "min_edge": 0.04,
        "strategy_edge": 0.177,
        "max_positions": 5
    },
    "method_description": (
        "Whale Cluster Mega Strategy: Based on 388M trade analysis. "
        "Detects markets with mega-clusters of 5+ whale trades in concentrated time windows. "
        "Historically +17.7% edge. Higher whale threshold than contrarian_whale for higher confidence. "
        "Uses Strategy Evaluator with 15-min scan cycle."
    )
})

# ── 3. Strategies to retire (no real edge, 0 trades) ──
retire_ids = [
    "strat_odds_edge",       # Sports Odds - no data source connected
    "strat_fedwatch",        # CME FedWatch - no data source connected
    "strat_cross_platform",  # Cross-Platform Arb - no data source connected
    "strat_live_sports",     # Live Sports - no data source connected
    "strat_high_prob_edge",  # High Prob - no proven edge
]

# ── Execute ──
print("=== Strategy Setup ===\n")

# Insert new strategies (upsert)
for strat_id, name, definition, edge, category in [
    ("contrarian_whale", "Contrarian Whale", contrarian_def, 0.20, "whale"),
    ("whale_cluster_mega", "Whale Cluster Mega", whale_cluster_def, 0.177, "whale"),
]:
    existing = conn.execute("SELECT id FROM strategies WHERE id = ?", (strat_id,)).fetchone()
    if existing:
        conn.execute(
            "UPDATE strategies SET definition = ?, status = 'active', confidence_score = ?, "
            "updated_at = CURRENT_TIMESTAMP, retired_at = NULL WHERE id = ?",
            (definition, edge, strat_id)
        )
        print(f"  UPDATED: {strat_id} -> active")
    else:
        conn.execute(
            "INSERT INTO strategies (id, name, definition, status, category, confidence_score, "
            "discovered_by, description) VALUES (?, ?, ?, 'active', ?, ?, 'strategy_discovery_v2', ?)",
            (strat_id, name, definition, category, edge,
             f"Blockchain-analysierte Strategie mit {edge*100:.1f}% historischem Edge")
        )
        print(f"  INSERTED: {strat_id} (active, edge={edge*100:.1f}%)")

# Retire broken strategies
for rid in retire_ids:
    result = conn.execute(
        "UPDATE strategies SET status = 'retired', retired_at = CURRENT_TIMESTAMP, "
        "updated_at = CURRENT_TIMESTAMP WHERE id = ? AND status != 'retired'",
        (rid,)
    )
    if result.rowcount > 0:
        print(f"  RETIRED: {rid}")
    else:
        print(f"  SKIP (already retired or not found): {rid}")

conn.commit()

# Show final state
print("\n=== Final Strategy Status ===\n")
rows = conn.execute(
    "SELECT id, name, status, live_trades, live_pnl, confidence_score FROM strategies ORDER BY status, id"
).fetchall()
for r in rows:
    print(f"  [{r['status']:8s}] {r['id']:25s} | trades={r['live_trades']:3d} | pnl=${r['live_pnl']:.2f} | conf={r['confidence_score']:.3f}")

conn.close()
print("\nDone!")
