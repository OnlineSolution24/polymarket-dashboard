# Scheduled Tasks

## Alle 60 Minuten
1. Rufe `get_markets(limit=20)` auf
2. Prüfe ob sich Preise signifikant geändert haben (via `get_market_snapshots()`)
3. NUR wenn signifikante Änderungen: Analyse durchführen und loggen
4. Wenn keine Änderungen: nur kurz loggen und nächsten Cycle abwarten
5. Logge Ergebnisse: `log_event(agent_id="analyst", message="Analyzed X markets, Y with edge > 3%")`
