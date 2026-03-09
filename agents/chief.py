"""
Chief Agent - The orchestrator.
Coordinates sub-agents, generates suggestions, and provides daily summaries.
"""

import json
import re
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

            # 2. Check recent market data (high-level only)
            markets = engine.query("SELECT * FROM markets ORDER BY volume DESC LIMIT 10")
            market_summary = self._summarize_markets(markets)

            # 3. Check recent trade performance
            trades = engine.query(
                "SELECT * FROM trades WHERE result IS NOT NULL ORDER BY created_at DESC LIMIT 20"
            )
            trade_summary = self._summarize_trades(trades)

            # 4. Check overall edge environment (high-level only)
            trading_cfg = load_platform_config().get("trading", {})
            trading_mode = trading_cfg.get("mode", "paper")
            edge_cfg = trading_cfg.get("limits", {})
            min_edge = edge_cfg.get("min_edge", 0.03)

            edge_row = engine.query_one(
                "SELECT COUNT(*) AS cnt FROM markets WHERE calculated_edge IS NOT NULL AND calculated_edge >= ?",
                (min_edge,),
            )
            edge_count = int(edge_row["cnt"]) if edge_row else 0

            # 5. Ask OpenClaw for analysis and suggestions
            prompt = (
                f"Analyse als Polymarket-Chief (Trading-Modus: {trading_mode}):\n\n"
                f"AGENTS:\n{agent_summary}\n\n"
                f"MARKT-UEBERBLICK:\n{market_summary}\n\n"
                f"EDGE-UMFELD: {edge_count} Maerkte >= {min_edge:.1%}\n\n"
                f"LETZTE TRADES:\n{trade_summary}\n\n"
                f"Aufgaben:\n"
                f"1. Nenne allgemeine Verbesserungen (Config, Risiko, Prozesse, Monitoring).\n"
                f"2. Keine markt- oder trade-spezifischen Einzelvorschlaege.\n"
                f"3. Keine Budget-Breakdowns pro Agent.\n"
                f"4. Formuliere klare Rahmenbedingungen, z.B. 'Erhoehe Edge-Threshold auf X%'.\n"
                f"Antworte strukturiert mit klaren Empfehlungen."
            )

            response = self.think(prompt)

            if response:
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
            lines.append(f"- {a['name']} ({a['role']}): Status={a['status']}")
        return "\n".join(lines)

    def _summarize_markets(self, markets: list[dict]) -> str:
        if not markets:
            return "Keine Marktdaten."
        count = len(markets)
        avg_yes = sum((m.get("yes_price", 0) or 0) for m in markets) / count
        avg_no = sum((m.get("no_price", 0) or 0) for m in markets) / count
        total_vol = sum((m.get("volume", 0) or 0) for m in markets)
        avg_edge = sum((m.get("calculated_edge", 0) or 0) for m in markets) / count
        return (
            f"Top-{count} Maerkte aggregiert: "
            f"AVG YES={avg_yes:.2f}, AVG NO={avg_no:.2f}, "
            f"Total Vol=${total_vol:,.0f}, AVG Edge={avg_edge:.1%}"
        )

    def _summarize_trades(self, trades: list[dict]) -> str:
        if not trades:
            return "Keine abgeschlossenen Trades."
        wins = sum(1 for t in trades if t.get("result") == "win")
        losses = sum(1 for t in trades if t.get("result") == "loss")
        total_pnl = sum(t.get("pnl", 0) or 0 for t in trades)
        return f"Wins: {wins}, Losses: {losses}, Total PnL: ${total_pnl:+.2f}"

    def _process_suggestions(self, response: str) -> None:
        """Extract and create suggestions from Chief's response."""
        if self._is_duplicate_analysis(response):
            self.log("info", "Chief suggestion uebersprungen: gleiche Punkte wie letzter Vorschlag")
            return

        self.create_suggestion(
            type="analysis",
            title="Chief Agent Analyse",
            description=response[:500],
            payload={"full_response": response, "timestamp": datetime.utcnow().isoformat()},
        )

    def _is_duplicate_analysis(self, response: str) -> bool:
        """Return True if the new response repeats the same recommendation points as the last one."""
        last = engine.query_one(
            "SELECT payload, description FROM suggestions WHERE agent_id = ? AND type = 'analysis' "
            "ORDER BY created_at DESC LIMIT 1",
            (self.id,),
        )
        if not last:
            return False

        previous_text = ""
        payload = last.get("payload")
        if payload:
            try:
                data = json.loads(payload) if isinstance(payload, str) else payload
                previous_text = data.get("full_response", "") if isinstance(data, dict) else ""
            except (json.JSONDecodeError, TypeError, ValueError):
                previous_text = ""
        if not previous_text:
            previous_text = last.get("description", "") or ""

        new_points = self._extract_points(response)
        old_points = self._extract_points(previous_text)
        if new_points and old_points and new_points == old_points:
            return True

        return self._normalize_text(response) == self._normalize_text(previous_text)

    def _extract_points(self, text: str) -> set[str]:
        """Extract normalized bullet/numbered points."""
        points = set()
        for raw in (text or "").splitlines():
            line = raw.strip()
            if not line:
                continue
            if re.match(r"^([-*]\s+|\d+[.)]\s+)", line):
                points.add(self._normalize_text(line))
        return {p for p in points if p}

    def _normalize_text(self, text: str) -> str:
        cleaned = re.sub(r"\s+", " ", (text or "").strip().lower())
        cleaned = re.sub(r"[^\w\s%]", "", cleaned)
        return cleaned
