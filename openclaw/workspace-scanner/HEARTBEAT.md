# Scheduled Tasks

## Alle 6 Stunden
1. Prüfe `get_costs()` — wenn Budget > 80%, überspringe diesen Cycle
2. GitHub-Suche nach neuen Prediction-Market-Projekten
3. Bewerte gefundene Projekte nach Relevanz und Qualität
4. NUR wenn relevanter Fund (Relevanz ≥ 3/5):
   - Chief informieren via `sessions_send`
   - `log_event(agent_id="scanner", message="Found: ...")` loggen
5. Wenn nichts Relevantes: nur kurz loggen

## Täglich (einmal)
1. Überprüfe bereits gemeldete Funde auf Updates
2. Zusammenfassung der Tages-Funde an Chief
