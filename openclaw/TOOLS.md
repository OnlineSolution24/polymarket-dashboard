# Bot API Tools

Du hast Zugang zur Polymarket Bot REST API via curl. Nutze diese Befehle um Daten abzurufen und Aktionen auszufuehren.

## Setup
```bash
source /data/mcp/api.sh
```

## API Referenz

### Maerkte
```bash
# Alle Maerkte abrufen
api_get /api/markets "limit=20"

# Markt-Snapshots (Preisverlauf)
api_get /api/snapshots/MARKET_ID "hours=48"
```

### Strategien
```bash
# Alle Strategien
api_get /api/strategies

# Strategien nach Status filtern
api_get /api/strategies "status=active"

# Strategie-Detail
api_get /api/strategies/STRATEGY_ID

# Neue Strategie erstellen
api_post /api/strategies '{"name":"Trend Following","description":"Buy when edge > 5%","definition":{"entry_rules":[{"field":"calculated_edge","op":"gt","value":0.05}],"exit_rules":[{"field":"calculated_edge","op":"lt","value":0.01}],"trade_params":{"sizing_method":"kelly","max_position_pct":5}}}'

# Strategie-Status aendern
api_put /api/strategies/STRATEGY_ID/status '{"status":"active","approved_by":"chief"}'

# Strategie loeschen
api_delete /api/strategies/STRATEGY_ID
```

### Backtest
```bash
# Backtest starten
api_post /api/backtest/STRATEGY_ID

# Backtest-Ergebnisse abrufen
api_get /api/backtest/STRATEGY_ID/results
```

### Analytics
```bash
# Pattern-Analyse (Win Rates nach Kategorie/Preis/Volumen)
api_get /api/analytics/patterns

# Strategie-Signale (welche Maerkte matchen eine Strategie?)
api_get /api/analytics/strategy-signals/STRATEGY_ID
```

### Trading
```bash
# Trade ausfuehren (Live)
api_post /api/trades/execute '{"market_id":"MARKET_ID","side":"yes","amount":5.0}'

# Paper Trade simulieren
api_post /api/trades/simulate '{"market_id":"MARKET_ID","side":"yes","amount":5.0}'

# Risk-Check
api_post /api/trades/check-risk '{"market_id":"MARKET_ID","side":"yes","amount":5.0}'

# Trade-History
api_get /api/trades "limit=20"

# Trade-Statistiken
api_get /api/trades/stats
```

### System
```bash
# Bot-Status
api_get /api/status

# Bot-Health (erweitert)
api_get /api/monitor/health

# Fehler-Logs
api_get /api/monitor/errors "hours=24"

# Circuit Breaker Status
api_get /api/circuit-breaker

# Circuit Breaker zuruecksetzen
api_post /api/circuit-breaker/reset

# API-Kosten
api_get /api/costs "days=7"

# Konfiguration
api_get /api/config

# Event loggen
api_post /api/logs '{"agent_id":"DEIN_AGENT_ID","level":"info","message":"Deine Nachricht"}'
```

## Wichtig
- Fuehre immer `source /data/mcp/api.sh` aus bevor du API-Calls machst
- Nutze `| python3 -m json.tool` um JSON-Output lesbarer zu formatieren
- Bei Fehlern: pruefe ob der Bot laeuft mit `api_get /api/status`
