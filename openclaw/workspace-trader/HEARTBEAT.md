# Scheduled Tasks

## Alle 5 Minuten
1. Rufe `get_strategies(status="active")` auf
2. Für jede aktive Strategie: `check_strategy_signals(strategy_id)`
3. NUR wenn ein Signal gefunden wird:
   - `get_circuit_breaker()` prüfen
   - `check_risk(market_id, side, amount)` prüfen
   - Trade ausführen oder simulieren
   - `log_event(agent_id="trader", message="Traded ...")` loggen
4. Wenn kein Signal: KEIN Claude-Call nötig, nächsten Cycle abwarten
