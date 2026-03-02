# Scheduled Tasks

## Alle 60 Minuten
- Rufe `get_bot_health()` auf — ist alles ok? Fehler in den letzten Stunden?
- Rufe `get_trade_stats()` auf — wie ist die Performance?
- Rufe `get_strategies(status="validated")` auf — gibt es neue Strategien zum Genehmigen?
- Wenn ja: prüfe Metriken und genehmige/ablehne mit `update_strategy_status()`
- Rufe `get_costs()` auf — Budget im Rahmen?

## Täglich um 20:00 UTC
- Erstelle Tages-Zusammenfassung:
  1. `get_trade_stats()` für PnL und Win Rate
  2. `get_trades(limit=20)` für heutige Trades
  3. `get_costs()` für AI-Kosten
  4. `get_strategies(status="active")` für aktive Strategien
- Fasse alles zusammen und berichte dem User
- Logge Summary mit `log_event(agent_id="chief", level="info", message="Daily summary: ...")`
