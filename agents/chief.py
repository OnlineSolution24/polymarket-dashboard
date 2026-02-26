"""
Chief Agent - The orchestrator.
Coordinates sub-agents, generates suggestions, and provides daily summaries.
"""

import json
from datetime import datetime

from agents.base_agent import BaseAgent
from db import engine


class ChiefAgent(BaseAgent):
    """
    The Polymarket Chief Agent.
    Responsibilities:
    - Coordinate sub-agent results
    - Generate improvement suggestions
    - Propose new sub-agents for new topics
    - Create daily performance summaries
    """

    def run_cycle(self) -> dict:
        """Run one orchestration cycle."""
        self.log("info", "Chief cycle gestartet")

        try:
            # 1. Collect sub-agent statuses
            agents = engine.query("SELECT * FROM agents WHERE role != 'chief' AND status = 'active'")
            agent_summary = self._summarize_agents(agents)

            # 2. Check recent market data
            markets = engine.query("SELECT * FROM markets ORDER BY volume DESC LIMIT 10")
            market_summary = self._summarize_markets(markets)

            # 3. Check recent trade performance
            trades = engine.query(
                "SELECT * FROM trades WHERE result IS NOT NULL ORDER BY created_at DESC LIMIT 20"
            )
            trade_summary = self._summarize_trades(trades)

            # 4. Ask OpenClaw for analysis and suggestions
            prompt = (
                f"Analyse als Polymarket-Chief:\n\n"
                f"AGENTS:\n{agent_summary}\n\n"
                f"TOP MÄRKTE:\n{market_summary}\n\n"
                f"LETZTE TRADES:\n{trade_summary}\n\n"
                f"Aufgaben:\n"
                f"1. Gibt es Verbesserungsvorschläge? (z.B. neue Agents, Config-Änderungen, Risk-Anpassungen)\n"
                f"2. Welche Märkte verdienen tiefere Analyse?\n"
                f"3. Gibt es Muster in den Trade-Ergebnissen?\n"
                f"Antworte strukturiert mit klaren Empfehlungen."
            )

            response = self.think(prompt)

            if response:
                # Parse response for actionable suggestions
                self._process_suggestions(response)
                self.log("info", f"Chief cycle abgeschlossen. Response: {response[:200]}...")
            else:
                self.log("warn", "Keine Antwort von OpenClaw erhalten")

            return {"ok": True, "summary": "Chief cycle completed"}

        except Exception as e:
            self.log("error", f"Chief cycle fehlgeschlagen: {e}")
            return {"ok": False, "summary": str(e)}

    def _summarize_agents(self, agents: list[dict]) -> str:
        if not agents:
            return "Keine Sub-Agents aktiv."
        lines = []
        for a in agents:
            budget = a.get("budget_used_today", 0) or 0
            lines.append(f"- {a['name']} ({a['role']}): Status={a['status']}, Budget heute=${budget:.2f}")
        return "\n".join(lines)

    def _summarize_markets(self, markets: list[dict]) -> str:
        if not markets:
            return "Keine Marktdaten."
        lines = []
        for m in markets:
            lines.append(
                f"- {m['question'][:60]}: YES={m['yes_price']:.2f} NO={m['no_price']:.2f} "
                f"Vol=${m['volume']:,.0f}"
            )
        return "\n".join(lines)

    def _summarize_trades(self, trades: list[dict]) -> str:
        if not trades:
            return "Keine abgeschlossenen Trades."
        wins = sum(1 for t in trades if t.get("result") == "win")
        losses = sum(1 for t in trades if t.get("result") == "loss")
        total_pnl = sum(t.get("pnl", 0) or 0 for t in trades)
        return f"Wins: {wins}, Losses: {losses}, Total PnL: ${total_pnl:+.2f}"

    def _process_suggestions(self, response: str) -> None:
        """Extract and create suggestions from Chief's response."""
        # For now, create a single suggestion with the full analysis
        self.create_suggestion(
            type="analysis",
            title="Chief Agent Analyse",
            description=response[:500],
            payload={"full_response": response, "timestamp": datetime.utcnow().isoformat()},
        )
