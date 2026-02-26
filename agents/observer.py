"""
Observer Agent - Market scanning and trend detection.
"""

from agents.base_agent import BaseAgent
from db import engine


class ObserverAgent(BaseAgent):
    """Scans Polymarket for trending markets and price movements."""

    def run_cycle(self) -> dict:
        self.log("info", "Observer scan gestartet")

        try:
            # Get current market data
            markets = engine.query("SELECT * FROM markets ORDER BY volume DESC LIMIT 20")

            if not markets:
                self.log("info", "Keine Marktdaten vorhanden. Überspringe Scan.")
                return {"ok": True, "summary": "No market data available"}

            # Build analysis prompt
            market_lines = []
            for m in markets:
                market_lines.append(
                    f"- {m['question'][:60]}: YES={m['yes_price']:.2f} "
                    f"Vol=${m['volume']:,.0f} Liq=${m['liquidity']:,.0f}"
                )

            prompt = (
                f"Analysiere diese Polymarket-Märkte:\n\n"
                f"{'chr(10)'.join(market_lines)}\n\n"
                f"1. Welche Märkte zeigen interessante Preisbewegungen?\n"
                f"2. Gibt es Märkte mit hohem Volumen aber extremen Preisen (>90% oder <10%)?\n"
                f"3. Welche Märkte verdienen tiefere Analyse?\n"
                f"Antworte kurz und präzise."
            )

            response = self.think(prompt)

            if response:
                self.log("info", f"Observer scan abgeschlossen: {response[:200]}...")

                # Save findings to memory
                memory = self.load_memory()
                memory["last_scan"] = {
                    "market_count": len(markets),
                    "findings": response[:500],
                }
                self.save_memory(memory)

            return {"ok": True, "summary": f"Scanned {len(markets)} markets"}

        except Exception as e:
            self.log("error", f"Observer scan fehlgeschlagen: {e}")
            return {"ok": False, "summary": str(e)}
