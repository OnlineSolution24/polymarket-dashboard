"""
Chief Agent - The orchestrator.
Coordinates sub-agents, generates suggestions, and provides daily summaries.
"""

import json
from datetime import datetime

from agents.base_agent import BaseAgent
from config import load_platform_config
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

            # 4. Check markets with calculated edge for trade opportunities
            trading_cfg = load_platform_config().get("trading", {})
            trading_mode = trading_cfg.get("mode", "paper")

            edge_markets = engine.query(
                "SELECT * FROM markets WHERE calculated_edge IS NOT NULL AND calculated_edge > 0.03 "
                "ORDER BY calculated_edge DESC LIMIT 5"
            )
            edge_summary = self._summarize_edge_markets(edge_markets)

            # 5. Ask OpenClaw for analysis and suggestions
            prompt = (
                f"Analyse als Polymarket-Chief (Trading-Modus: {trading_mode}):\n\n"
                f"AGENTS:\n{agent_summary}\n\n"
                f"TOP MÄRKTE:\n{market_summary}\n\n"
                f"MÄRKTE MIT EDGE:\n{edge_summary}\n\n"
                f"LETZTE TRADES:\n{trade_summary}\n\n"
                f"Aufgaben:\n"
                f"1. Gibt es Verbesserungsvorschläge? (z.B. neue Agents, Config-Änderungen, Risk-Anpassungen)\n"
                f"2. Welche Märkte verdienen tiefere Analyse?\n"
                f"3. Gibt es Muster in den Trade-Ergebnissen?\n"
                f"4. Für welche Märkte mit Edge sollten wir Trades platzieren? "
                f"Antworte mit JSON-Array falls ja: "
                f'[{{"market_id": "...", "side": "YES/NO", "amount_usd": X, "reason": "..."}}]\n'
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

    def _summarize_edge_markets(self, markets: list[dict]) -> str:
        if not markets:
            return "Keine Märkte mit berechnetem Edge."
        lines = []
        for m in markets:
            edge = m.get("calculated_edge", 0) or 0
            lines.append(
                f"- {m['question'][:60]}: Edge={edge:.1%} "
                f"YES={m['yes_price']:.2f} NO={m['no_price']:.2f} Vol=${m['volume']:,.0f}"
            )
        return "\n".join(lines)

    def _process_suggestions(self, response: str) -> None:
        """Extract and create suggestions from Chief's response."""
        # 1. Always create analysis suggestion
        self.create_suggestion(
            type="analysis",
            title="Chief Agent Analyse",
            description=response[:500],
            payload={"full_response": response, "timestamp": datetime.utcnow().isoformat()},
        )

        # 2. Try to extract trade recommendations from response
        self._extract_trade_suggestions(response)

    def _extract_trade_suggestions(self, response: str) -> None:
        """Parse OpenClaw response for trade recommendations and create trade suggestions."""
        trading_cfg = load_platform_config().get("trading", {})
        mode = trading_cfg.get("mode", "paper")

        # Try to find JSON array in response
        import re
        json_match = re.search(r'\[[\s\S]*?\{[\s\S]*?"market_id"[\s\S]*?\}[\s\S]*?\]', response)
        if not json_match:
            return

        try:
            trades = json.loads(json_match.group())
        except (json.JSONDecodeError, ValueError):
            return

        for trade in trades:
            if not isinstance(trade, dict):
                continue
            market_id = trade.get("market_id", "")
            if not market_id:
                continue

            # Look up market data
            market = engine.query_one("SELECT * FROM markets WHERE id = ?", (market_id,))
            if not market:
                continue

            side = trade.get("side", "YES").upper()
            amount = float(trade.get("amount_usd", 0))
            if amount <= 0:
                continue

            # In full-auto mode, auto-approve. Otherwise, leave as pending for user.
            status = "auto_approved" if mode == "full-auto" else "pending"

            engine.execute(
                """INSERT INTO suggestions (agent_id, type, title, description, payload, status, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (
                    self.id,
                    "trade",
                    f"Trade: {side} auf '{market['question'][:50]}...'",
                    trade.get("reason", "")[:500],
                    json.dumps({
                        "market_id": market_id,
                        "market_question": market["question"],
                        "side": side,
                        "amount_usd": amount,
                        "edge": market.get("calculated_edge", 0),
                        "yes_price": market.get("yes_price", 0),
                        "no_price": market.get("no_price", 0),
                        "chief_reason": trade.get("reason", ""),
                    }),
                    status,
                    datetime.utcnow().isoformat(),
                ),
            )
            self.log("info", f"Trade suggestion erstellt ({status}): {side} ${amount:.2f} auf {market_id}")
