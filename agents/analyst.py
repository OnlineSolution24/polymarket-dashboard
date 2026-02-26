"""
Analyst Agent - Deep market analysis and edge calculation.
"""

from agents.base_agent import BaseAgent
from db import engine


class AnalystAgent(BaseAgent):
    """Performs deep analysis on markets flagged by the Observer."""

    def run_cycle(self) -> dict:
        self.log("info", "Analyst cycle gestartet")

        try:
            # Get high-volume markets without edge calculation
            markets = engine.query(
                "SELECT * FROM markets WHERE volume > 10000 ORDER BY volume DESC LIMIT 10"
            )

            if not markets:
                self.log("info", "Keine Märkte für Analyse verfügbar.")
                return {"ok": True, "summary": "No markets to analyze"}

            for market in markets[:5]:  # Analyze top 5
                prompt = (
                    f"Tiefenanalyse für Polymarket-Markt:\n"
                    f"Frage: {market['question']}\n"
                    f"YES Preis: {market['yes_price']:.2f} (={market['yes_price']*100:.0f}%)\n"
                    f"NO Preis: {market['no_price']:.2f}\n"
                    f"Volumen: ${market['volume']:,.0f}\n\n"
                    f"1. Was ist deine Einschätzung der wahren Wahrscheinlichkeit?\n"
                    f"2. Gibt es einen Edge (Differenz Marktpreis vs. wahre Wahrscheinlichkeit)?\n"
                    f"3. Konfidenz-Level: niedrig/mittel/hoch?\n"
                    f"Antworte mit: EDGE=+X.XX oder EDGE=-X.XX und KONFIDENZ=niedrig/mittel/hoch"
                )

                response = self.think(prompt)

                if response:
                    # Try to extract edge from response
                    edge = self._extract_edge(response)
                    if edge is not None:
                        engine.execute(
                            "UPDATE markets SET calculated_edge = ?, last_updated = datetime('now') WHERE id = ?",
                            (edge, market["id"]),
                        )

                    self.log("info", f"Analyse für '{market['question'][:40]}': Edge={edge}")

            return {"ok": True, "summary": f"Analyzed {min(5, len(markets))} markets"}

        except Exception as e:
            self.log("error", f"Analyst cycle fehlgeschlagen: {e}")
            return {"ok": False, "summary": str(e)}

    @staticmethod
    def _extract_edge(response: str) -> float | None:
        """Try to extract edge value from agent response."""
        import re
        match = re.search(r"EDGE\s*=\s*([+-]?\d+\.?\d*)", response, re.IGNORECASE)
        if match:
            try:
                return float(match.group(1))
            except ValueError:
                pass
        return None
