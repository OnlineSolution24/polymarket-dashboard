# Scheduled Tasks

## Alle 2 Stunden — Discovery Cycle
1. Rufe `get_pattern_analysis()` auf — welche Muster sind erkennbar?
2. Rufe `get_markets(limit=20)` auf — was sind die aktuellen Top-Märkte?
3. Prüfe `get_strategies()` — gibt es schon ähnliche Strategien?
4. Wenn neues Muster gefunden:
   - Formuliere Hypothese
   - Erstelle Strategie mit `save_strategy()`
   - Starte Backtest mit `run_backtest(strategy_id)`
   - Logge Discovery: `log_event(agent_id="strategy", message="Discovered: ...")`
5. Wenn kein neues Muster: nur loggen und auf nächsten Cycle warten

## Alle 6 Stunden — Evaluation & Refinement
1. Prüfe `get_strategies(status="active")` — wie performen aktive Strategien?
2. Für jede aktive Strategie: `get_strategy_detail(id)` für Live-Trades
3. Vergleiche Backtest Win Rate vs Live Win Rate
4. Bei Degradation (Live WR < Backtest WR - 15%):
   - Analysiere warum (Marktbedingungen geändert? Regeln zu eng/weit?)
   - Erstelle verbesserte Version der Strategie
   - Informiere Chief
5. Prüfe `get_strategies(status="backtested")` — gibt es fertige Backtests?
   - Wenn Confidence ≥ 0.5: informiere Chief zur Genehmigung
   - Wenn Confidence < 0.3: `update_strategy_status(id, "rejected")`
