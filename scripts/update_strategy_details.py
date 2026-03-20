"""
Update all active strategies with clear names, descriptions, and documented parameters.
Each strategy gets a unique name, edge explanation, and properly structured definition.

Run via: docker exec polymarket-bot-bot-1 python3 scripts/update_strategy_details.py
"""

import json
import sys
sys.path.insert(0, "/app")

from db import engine


# Complete strategy definitions for all active strategies.
# Each entry updates: name, description, category, definition (entry_rules + trade_params)
STRATEGIES = {

    # ---------------------------------------------------------------
    # DATA-DRIVEN STRATEGIES (proven edges from 288M blockchain trades)
    # ---------------------------------------------------------------

    "politics_edge": {
        "name": "Politics YES Bias",
        "description": (
            "Politische Märkte sind systematisch unterbewertet. "
            "Die Crowd unterschätzt die Wahrscheinlichkeit politischer Ereignisse. "
            "Edge: +3.1% auf 16.2M historische Trades. "
            "Kauft YES bei 20-75¢ auf Politik-Märkten mit genug Volumen/Liquidität."
        ),
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

    "whale_favorites": {
        "name": "Whale Favorites (50-80¢)",
        "description": (
            "Wenn Whales ($500+) und Medium-Trader ($50-500) Favoriten bei 50-80¢ kaufen, "
            "liegt die Hit-Rate 2.5-3% über Fair Value. Retail zeigt keinen Edge in dieser Range. "
            "Edge: +2.5-3% auf 2M+ Trades. "
            "Kauft YES wenn mindestens 2 Whale-Buys und positiver Whale Net Flow."
        ),
        "category": "",
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

    # ---------------------------------------------------------------
    # EDGE-SOURCE STRATEGIES (external data feeds)
    # ---------------------------------------------------------------

    "strat_fedwatch": {
        "name": "FedWatch Zins-Edge",
        "description": (
            "Vergleicht CME FedWatch Zins-Wahrscheinlichkeiten mit Polymarket-Preisen. "
            "Wenn FedWatch eine höhere Wahrscheinlichkeit zeigt als der Marktpreis, kauft YES. "
            "Nutzt professionelle Zins-Futures-Daten als Edge-Quelle. "
            "Parameter: min_edge=5%, nur Economics-Kategorie."
        ),
        "category": "economics",
        "definition": {
            "entry_rules": [
                {"field": "calculated_edge", "op": "gte", "value": 0.05},
                {"field": "volume", "op": "gte", "value": 5000},
                {"field": "liquidity", "op": "gte", "value": 1000},
            ],
            "category_filter": ["economics"],
            "trade_params": {
                "side": "YES",
                "sizing_method": "fixed_amount",
                "fixed_amount_usd": 3.0,
                "min_edge": 0.05,
            },
        },
    },

    "strat_odds_edge": {
        "name": "Sportwetten-Odds Edge",
        "description": (
            "Vergleicht professionelle Sportwetten-Quoten (Pinnacle, DraftKings) "
            "mit Polymarket-Preisen. Wenn Buchmacher-Quoten einen höheren Preis implizieren, "
            "kauft YES. Nutzt den Informationsvorsprung professioneller Sportsbücher. "
            "Parameter: min_edge=5%, nur Sports-Kategorie."
        ),
        "category": "sports",
        "definition": {
            "entry_rules": [
                {"field": "calculated_edge", "op": "gte", "value": 0.05},
                {"field": "volume", "op": "gte", "value": 5000},
                {"field": "liquidity", "op": "gte", "value": 1000},
            ],
            "category_filter": ["sports"],
            "trade_params": {
                "side": "YES",
                "sizing_method": "fixed_amount",
                "fixed_amount_usd": 3.0,
                "min_edge": 0.05,
            },
        },
    },

    # ---------------------------------------------------------------
    # WEATHER STRATEGIES
    # ---------------------------------------------------------------

    "strat_85bce613": {
        "name": "Weather Forecast Edge",
        "description": (
            "Nutzt Wetter-API-Vorhersagen (NOAA, OpenMeteo) als Edge-Quelle für Wetter-Märkte. "
            "Wenn die Vorhersage ≥10% vom Marktpreis abweicht und Top-Holder konzentriert sind, "
            "kauft YES. Holder-Konzentration >40% deutet auf informierte Trader. "
            "Parameter: min_edge=10%, min_holder_concentration=40%."
        ),
        "category": "weather",
        "definition": {
            "entry_rules": [
                {"field": "calculated_edge", "op": "gte", "value": 0.10},
                {"field": "top_holder_concentration", "op": "gte", "value": 0.4},
                {"field": "volume", "op": "gte", "value": 5000},
            ],
            "category_filter": ["weather", "climate", "science"],
            "trade_params": {
                "side": "YES",
                "sizing_method": "fixed_amount",
                "fixed_amount_usd": 3.0,
                "min_edge": 0.10,
            },
        },
    },

    "strat_weather_no": {
        "name": "Weather NO Sniper",
        "description": (
            "Kauft NO auf Wetter-Märkten wenn die Vorhersage stark gegen YES spricht. "
            "Backtest: +$2.87, 100% Win Rate (kleine Stichprobe). "
            "Kombiniert Wetter-Vorhersage-Edge mit NO-Seite für Märkte wo die "
            "Crowd das Wetter-Event überschätzt. "
            "Parameter: min_edge=10%, Weather-Kategorie."
        ),
        "category": "weather",
        "definition": {
            "entry_rules": [
                {"field": "calculated_edge", "op": "gte", "value": 0.10},
                {"field": "volume", "op": "gte", "value": 3000},
                {"field": "no_price", "op": "lte", "value": 0.50},
            ],
            "category_filter": ["weather", "climate"],
            "trade_params": {
                "side": "NO",
                "sizing_method": "fixed_amount",
                "fixed_amount_usd": 2.0,
                "min_edge": 0.10,
            },
        },
    },

    # ---------------------------------------------------------------
    # SPORTS STRATEGIES
    # ---------------------------------------------------------------

    "strat_live_sports": {
        "name": "Live Sports Odds Tracker",
        "description": (
            "Trackt Live-Sportwetten-Quoten während laufender Events und kauft "
            "wenn der Polymarket-Preis deutlich unter den Buchmacher-Quoten liegt. "
            "Backtest: 50% Win Rate. Nutzt den Zeitvorteil bei Live-Events "
            "wo Polymarket langsamer reagiert als professionelle Bücher. "
            "Parameter: min_edge=5%, nur Sports."
        ),
        "category": "sports",
        "definition": {
            "entry_rules": [
                {"field": "calculated_edge", "op": "gte", "value": 0.05},
                {"field": "volume", "op": "gte", "value": 10000},
                {"field": "liquidity", "op": "gte", "value": 2000},
            ],
            "category_filter": ["sports"],
            "trade_params": {
                "side": "YES",
                "sizing_method": "fixed_amount",
                "fixed_amount_usd": 3.0,
                "min_edge": 0.05,
            },
        },
    },

    "strat_sports_no": {
        "name": "Sports Overdog Fade",
        "description": (
            "Kauft NO auf Sport-Märkten wo der Favorit überbewertet ist. "
            "Backtest: 75% Win Rate bei -$2.95 PnL (Verluste bei den 25% waren größer). "
            "Nutzt den Bias dass Fans ihre Teams überbewerten. "
            "Parameter: min_edge=5%, Sports-Kategorie, NO-Seite."
        ),
        "category": "sports",
        "definition": {
            "entry_rules": [
                {"field": "calculated_edge", "op": "gte", "value": 0.05},
                {"field": "yes_price", "op": "gte", "value": 0.65},
                {"field": "volume", "op": "gte", "value": 5000},
            ],
            "category_filter": ["sports"],
            "trade_params": {
                "side": "NO",
                "sizing_method": "fixed_amount",
                "fixed_amount_usd": 2.0,
                "min_edge": 0.05,
            },
        },
    },

    # ---------------------------------------------------------------
    # ECONOMICS STRATEGIES
    # ---------------------------------------------------------------

    "strat_econ_no": {
        "name": "Economics Contrarian NO",
        "description": (
            "Kauft NO auf Wirtschafts-/Fed-Märkten wenn die Crowd zu optimistisch ist. "
            "Backtest: 75% Win Rate bei -$4.22 PnL. "
            "Die Verluste bei den 25% Misses waren überproportional groß. "
            "Nutzt FedWatch als externe Datenquelle für Edge-Berechnung. "
            "Parameter: min_edge=5%, Economics-Kategorie, NO-Seite."
        ),
        "category": "economics",
        "definition": {
            "entry_rules": [
                {"field": "calculated_edge", "op": "gte", "value": 0.05},
                {"field": "yes_price", "op": "gte", "value": 0.60},
                {"field": "volume", "op": "gte", "value": 5000},
            ],
            "category_filter": ["economics"],
            "trade_params": {
                "side": "NO",
                "sizing_method": "fixed_amount",
                "fixed_amount_usd": 2.0,
                "min_edge": 0.05,
            },
        },
    },

    "strat_politics_no": {
        "name": "Politics Contrarian NO",
        "description": (
            "Kauft NO auf Politik-Märkten wenn die Crowd zu optimistisch ist. "
            "Backtest: 75% Win Rate bei -$3.02 PnL. "
            "Gegenstück zu 'Politics YES Bias' — für Märkte wo YES überbewertet ist. "
            "Achtung: Steht im Widerspruch zum Politics YES Bias Edge (+3.1%). "
            "Nur einsetzen wenn calculated_edge klar für NO spricht. "
            "Parameter: min_edge=5%, Politics-Kategorie, NO-Seite."
        ),
        "category": "politics",
        "definition": {
            "entry_rules": [
                {"field": "calculated_edge", "op": "gte", "value": 0.05},
                {"field": "yes_price", "op": "gte", "value": 0.65},
                {"field": "volume", "op": "gte", "value": 5000},
            ],
            "category_filter": ["politics"],
            "trade_params": {
                "side": "NO",
                "sizing_method": "fixed_amount",
                "fixed_amount_usd": 2.0,
                "min_edge": 0.05,
            },
        },
    },

    # ---------------------------------------------------------------
    # GENERAL STRATEGIES
    # ---------------------------------------------------------------

    "strat_resolution_sniper": {
        "name": "Resolution Sniper",
        "description": (
            "Kauft kurz vor Market-Resolution wenn der Ausgang bereits sehr wahrscheinlich ist. "
            "Backtest: +$59.00, 100% Win Rate (10 Trades). "
            "Nutzt den Zeitvorteil: kurz vor Ablauf ist die Information am klarsten, "
            "aber der Markt bietet noch Spread. "
            "Parameter: days_to_expiry ≤ 3, yes_price ≥ 85¢, Volume ≥ 10K."
        ),
        "category": "resolution",
        "definition": {
            "entry_rules": [
                {"field": "yes_price", "op": "gte", "value": 0.85},
                {"field": "days_to_expiry", "op": "lte", "value": 3},
                {"field": "volume", "op": "gte", "value": 10000},
                {"field": "liquidity", "op": "gte", "value": 2000},
            ],
            "trade_params": {
                "side": "YES",
                "sizing_method": "fixed_amount",
                "fixed_amount_usd": 5.0,
                "min_edge": 0.02,
            },
        },
    },

    "strat_simple_edge": {
        "name": "Simple Edge Allrounder",
        "description": (
            "Basis-Strategie: Kauft YES wenn der berechnete Edge ≥ 3% ist. "
            "Keine Kategorie-Einschränkung — funktioniert über alle Märkte. "
            "251 Live-Trades ausgeführt. Dient als Benchmark für spezialisierte Strategien. "
            "Parameter: min_edge=3%, min_volume=5K, min_liquidity=1K."
        ),
        "category": "general",
        "definition": {
            "entry_rules": [
                {"field": "calculated_edge", "op": "gte", "value": 0.03},
                {"field": "volume", "op": "gte", "value": 5000},
                {"field": "liquidity", "op": "gte", "value": 1000},
            ],
            "trade_params": {
                "side": "YES",
                "sizing_method": "fixed_amount",
                "fixed_amount_usd": 2.0,
                "min_edge": 0.03,
            },
        },
    },

    "strat_low_price_simple": {
        "name": "Low Price Value Hunter",
        "description": (
            "Kauft günstige YES-Positionen (≤ 30¢) mit positivem berechneten Edge. "
            "249 Live-Trades ausgeführt. Hoher potentieller Multiplier bei Gewinn (3-10x), "
            "aber niedrigere Win-Rate. ACHTUNG: 288M-Trade-Analyse zeigt KEINEN Edge "
            "bei reinen Preis-Buckets. Nur profitabel wenn calculated_edge zuverlässig ist. "
            "Parameter: max_price=30¢, min_edge=3%, min_volume=5K."
        ),
        "category": "value",
        "definition": {
            "entry_rules": [
                {"field": "yes_price", "op": "lte", "value": 0.30},
                {"field": "calculated_edge", "op": "gte", "value": 0.03},
                {"field": "volume", "op": "gte", "value": 5000},
                {"field": "liquidity", "op": "gte", "value": 500},
            ],
            "trade_params": {
                "side": "YES",
                "sizing_method": "fixed_amount",
                "fixed_amount_usd": 2.0,
                "min_edge": 0.03,
            },
        },
    },
}


def main():
    updated = 0
    for strat_id, data in STRATEGIES.items():
        existing = engine.query_one(
            "SELECT id FROM strategies WHERE id = ?", (strat_id,)
        )

        if existing:
            # Update existing strategy
            engine.execute(
                """UPDATE strategies
                   SET name = ?, description = ?, category = ?,
                       definition = ?, updated_at = datetime('now')
                   WHERE id = ?""",
                (
                    data["name"],
                    data["description"],
                    data.get("category", ""),
                    json.dumps(data["definition"]),
                    strat_id,
                ),
            )
            print(f"  UPDATED {strat_id} → {data['name']}")
            updated += 1
        else:
            # Create new strategy
            engine.execute(
                """INSERT INTO strategies (id, name, description, definition, status,
                   category, discovered_by, created_at, updated_at)
                   VALUES (?, ?, ?, ?, 'active', ?, ?, datetime('now'), datetime('now'))""",
                (
                    strat_id,
                    data["name"],
                    data["description"],
                    json.dumps(data["definition"]),
                    data.get("category", ""),
                    data.get("discovered_by", "manual"),
                ),
            )
            print(f"  CREATED {strat_id} → {data['name']}")
            updated += 1

    print(f"\nDone: {updated} strategies updated with full details.")

    # Show summary
    rows = engine.query(
        "SELECT id, name, status, category FROM strategies WHERE status = 'active' ORDER BY name"
    )
    print(f"\n{'='*60}")
    print(f"Active Strategies ({len(rows)}):")
    print(f"{'='*60}")
    for r in rows:
        print(f"  [{r['category'] or 'all':12s}] {r['name']}")


if __name__ == "__main__":
    main()
