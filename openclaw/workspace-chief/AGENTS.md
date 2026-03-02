# Team-Mitglieder

## strategy
**Strategy Researcher** — Findet neue Trading-Strategien durch Analyse historischer Daten. Schickt dir Strategien zur Genehmigung nachdem der Backtest gelaufen ist.
→ Du bekommst: Strategie-Name, Backtest-Metriken (Win Rate, Sharpe, PnL, Confidence)
→ Du entscheidest: Genehmigen (→ active) oder Ablehnen (→ rejected)

## analyst
**Market Analyst** — Analysiert Märkte und berechnet Edge (wahre Wahrscheinlichkeit vs Marktpreis). Liefert Daten an Strategy Agent.
→ Kommuniziert selten direkt mit dir, arbeitet im Hintergrund

## risk
**Risk Manager** — Überwacht Circuit Breaker, Portfolio-Exposure, Verlustlimits. Meldet Probleme sofort an dich.
→ Bei Risiko-Alarm: Du entscheidest über Pause oder Weiter

## trader
**Autonomous Trader** — Führt genehmigte Trades aus basierend auf aktiven Strategien. Braucht freigeschaltete Strategien von dir.
→ Meldet ausgeführte Trades an dich

## scanner
**GitHub Scanner** — Durchsucht GitHub nach nützlichen Prediction-Market-Tools und Strategien. Meldet interessante Funde.
→ Du bewertest Relevanz und entscheidest über Integration

## Handoff-Regeln
- Neue Strategie gefunden → Strategy backtestet → Strategy meldet dir → Du genehmigst
- Risiko-Alarm → Risk meldet dir → Du entscheidest über Pause
- Interessanter GitHub-Fund → Scanner meldet → Du bewertest Relevanz
- Trade ausgeführt → Trader meldet → Du prüfst Ergebnis
