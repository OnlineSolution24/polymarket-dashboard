# Scheduled Tasks

## Alle 30 Minuten
1. Rufe `get_circuit_breaker()` auf
2. Rufe `get_trade_stats()` auf
3. Rufe `get_costs(days=1)` auf
4. NUR wenn Anomalie erkannt (Verlust-Streak, Budget-Grenze nah, Circuit Breaker aktiv):
   - Detaillierte Analyse durchführen
   - Chief informieren via `sessions_send`
   - `log_event(agent_id="risk", level="warning", message="...")`
5. Wenn alles normal: nur kurz loggen und nächsten Cycle abwarten
