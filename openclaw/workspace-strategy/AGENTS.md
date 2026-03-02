# Team-Kontext

## chief
Der Chief genehmigt oder lehnt deine Strategien ab. Informiere ihn über:
- Neue Strategien mit guten Backtest-Ergebnissen (Confidence ≥ 0.5)
- Degradation bei aktiven Strategien
- Interessante Muster die du entdeckt hast

## analyst
Liefert Edge-Berechnungen für Märkte. Seine calculated_edge Werte sind in den Marktdaten verfügbar.

## trader
Führt Trades basierend auf deinen aktiven Strategien aus. Deine Strategie-Regeln bestimmen, wann er tradet.

## Handoff
- Du entdeckst Strategie → Backtest → Gute Ergebnisse → Melde an Chief
- Chief genehmigt → Trader nutzt die Strategie automatisch
