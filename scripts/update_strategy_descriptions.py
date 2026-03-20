"""
Update all active strategy descriptions with method details.
Also retires Politics YES Bias (edge too thin after fees).

Run via: docker exec polymarket-bot python3 scripts/update_strategy_descriptions.py
"""

import json
import sys
sys.path.insert(0, "/app")

from db import engine


# Retire Politics YES Bias — 3.1% edge minus ~2% Polymarket fee = ~1% net, too thin
RETIRE_IDS = ["politics_edge"]

STRATEGY_UPDATES = {
    "contrarian_whale": {
        "name": "Contrarian Whale Signal",
        "description": (
            "Wenn Whales ($500+) auf einer Seite dominieren und kaum Retail-Aktivität "
            "vorhanden ist, gewinnt die Whale-Seite in 70-95% der Fälle.\n\n"
            "=== METHODE ===\n"
            "System: Strategy Evaluator (alle 15 Min)\n"
            "Der Strategy Evaluator prüft alle 15 Minuten die Top-20-Märkte (nach Volume) "
            "gegen die Entry Rules. Whale-Daten (Käufe, Net Flow, Smart Money Score) werden "
            "von der Data API nur für die Top-20-Märkte geladen. Wenn ALLE Regeln gleichzeitig "
            "erfüllt sind, wird automatisch ein Trade erstellt.\n\n"
            "=== EDGE ===\n"
            "Quelle: Strategy Discovery v2 (388M Blockchain-Trades)\n"
            "Edge: +20% (whale dominant) bis +45% (whale only)\n"
            "Basis: 185.000+ Outcomes, out-of-sample validiert\n\n"
            "=== ENTRY RULES (alle müssen zutreffen) ===\n"
            "• whale_buy_count ≥ 3 (mindestens 3 Whale-Käufe)\n"
            "• whale_net_flow > 0 (Netto-Kaufdruck der Whales)\n"
            "• smart_money_score ≥ 65 (Smart Money bestätigt)\n"
            "• yes_price 10-90¢ (keine Penny-Tokens, keine Near-Certain)\n"
            "• volume ≥ $5.000\n\n"
            "=== TRADE ===\n"
            "Seite: YES | Einsatz: $5.00 | Min. Edge: 5%"
        ),
    },
    "whale_cluster_mega": {
        "name": "Whale Mega-Cluster",
        "description": (
            "Wenn 5+ Whale-Trades ($500+) innerhalb von ~30 Minuten auf der gleichen "
            "Seite eines Markts landen, gewinnt diese Seite in 67.7% der Fälle. "
            "Clustering deutet auf koordinierte Information hin.\n\n"
            "=== METHODE ===\n"
            "System: Strategy Evaluator (alle 15 Min)\n"
            "Prüft Top-20-Märkte gegen Entry Rules. Whale-Daten werden von der Data API "
            "für die volumenstärksten Märkte geladen. Trade wird automatisch erstellt wenn "
            "ALLE Regeln gleichzeitig erfüllt sind.\n\n"
            "=== EDGE ===\n"
            "Quelle: Strategy Discovery v2 (388M Blockchain-Trades)\n"
            "Edge: +17.7%\n"
            "Basis: 82.000+ Outcomes, statistisch signifikant\n\n"
            "=== ENTRY RULES (alle müssen zutreffen) ===\n"
            "• whale_buy_count ≥ 5 (mindestens 5 Whale-Käufe = Mega-Cluster)\n"
            "• whale_net_flow > 0 (Netto-Kaufdruck)\n"
            "• yes_price 10-90¢\n"
            "• volume ≥ $10.000\n"
            "• liquidity ≥ $2.000\n\n"
            "=== TRADE ===\n"
            "Seite: YES | Einsatz: $4.00 | Min. Edge: 4%"
        ),
    },
    "whale_favorites": {
        "name": "Whale Favorites (50-80¢)",
        "description": (
            "Wenn Whales ($500+) und Medium-Trader ($50-500) bei Favoriten (50-80¢) kaufen, "
            "liegt die Hit-Rate 2.5-3% über Fair Value. Nur auf Top-20-Märkten aktiv.\n\n"
            "=== METHODE ===\n"
            "System: Strategy Evaluator (alle 15 Min)\n"
            "Prüft Top-20-Märkte gegen Entry Rules. Whale-Daten stammen von der Data API. "
            "Enge Preis-Range (50-80¢) filtert auf Favoriten wo Whales informiert handeln.\n\n"
            "=== EDGE ===\n"
            "Quelle: Strategy Discovery (388M Blockchain-Trades)\n"
            "Edge: +2.5-3% (nach Fees ~1% netto — dünnster aktiver Edge)\n"
            "Basis: 2M+ Trades, statistisch hochsignifikant\n\n"
            "=== ENTRY RULES (alle müssen zutreffen) ===\n"
            "• yes_price 50-80¢ (nur Favoriten)\n"
            "• whale_buy_count ≥ 2\n"
            "• whale_net_flow > 0\n"
            "• volume ≥ $10.000\n"
            "• liquidity ≥ $2.000\n\n"
            "=== TRADE ===\n"
            "Seite: YES | Einsatz: $3.00 | Min. Edge: 2.5%"
        ),
    },
    "strat_resolution_sniper": {
        "name": "Resolution Sniper",
        "description": (
            "Kauft nahe am Markt-Ende (≤3 Tage) wenn das Ergebnis sehr wahrscheinlich ist "
            "(≥85¢). Nutzt die Informationsklarheit kurz vor Resolution.\n\n"
            "=== METHODE ===\n"
            "System: Strategy Evaluator (alle 15 Min)\n"
            "Scannt ALLE Märkte (nicht nur Top 20) nach Preis ≥85¢ und Restlaufzeit ≤3 Tage. "
            "Diese Kombination bedeutet: Ergebnis ist fast sicher, aber der Markt bietet noch "
            "15% Rendite auf das eingesetzte Kapital.\n\n"
            "=== EDGE ===\n"
            "Quelle: Logisches Prinzip + historische Performance\n"
            "Edge: ~15% (85¢ → $1.00 = 17.6% Rendite)\n"
            "Historisch: 100% Win-Rate (4 Trades, +$59)\n\n"
            "=== ENTRY RULES (alle müssen zutreffen) ===\n"
            "• yes_price ≥ 85¢\n"
            "• days_to_expiry ≤ 3\n"
            "• volume ≥ $10.000\n"
            "• liquidity ≥ $2.000\n\n"
            "=== TRADE ===\n"
            "Seite: YES | Einsatz: $5.00 | Min. Edge: 2%"
        ),
    },
}


def main():
    # Step 1: Retire thin-edge strategies
    for strat_id in RETIRE_IDS:
        row = engine.query_one("SELECT id, name, status FROM strategies WHERE id = ?", (strat_id,))
        if not row:
            print(f"  SKIP {strat_id} — not found")
            continue
        if row["status"] == "retired":
            print(f"  ALREADY RETIRED {strat_id}")
            continue
        engine.execute(
            "UPDATE strategies SET status = 'retired', updated_at = datetime('now') WHERE id = ?",
            (strat_id,),
        )
        print(f"  RETIRED {strat_id} — {row['name']} (edge too thin after fees)")

    # Step 2: Update descriptions
    updated = 0
    for strat_id, updates in STRATEGY_UPDATES.items():
        row = engine.query_one("SELECT id FROM strategies WHERE id = ?", (strat_id,))
        if not row:
            print(f"  SKIP {strat_id} — not found")
            continue

        engine.execute(
            "UPDATE strategies SET name = ?, description = ?, updated_at = datetime('now') WHERE id = ?",
            (updates["name"], updates["description"], strat_id),
        )
        print(f"  UPDATED {strat_id} — {updates['name']}")
        updated += 1

    print(f"\n{updated} strategy descriptions updated.")

    # Show final active list
    print("\n=== ACTIVE STRATEGIES ===")
    rows = engine.query("SELECT id, name FROM strategies WHERE status = 'active' ORDER BY name")
    for r in rows:
        print(f"  {r['id']} | {r['name']}")
    print(f"Total active: {len(rows)}")


if __name__ == "__main__":
    main()
