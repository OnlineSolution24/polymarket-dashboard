"""
Agent Factory - Creates agent instances from YAML configs.
Supports dynamic creation of new agent types.
"""

import logging
from typing import Optional

from config import AgentConfigYAML
from agents.base_agent import BaseAgent
from agents.chief import ChiefAgent
from agents.observer import ObserverAgent
from agents.analyst import AnalystAgent
from agents.risk_manager import RiskManagerAgent
from agents.trader import TraderAgent
from agents.backtester_agent import BacktesterAgent

logger = logging.getLogger(__name__)

# Role → Agent class mapping
ROLE_MAP: dict[str, type[BaseAgent]] = {
    "chief": ChiefAgent,
    "observer": ObserverAgent,
    "analyst": AnalystAgent,
    "risk_manager": RiskManagerAgent,
    "trader": TraderAgent,
    "backtester": BacktesterAgent,
}


class GenericAgent(BaseAgent):
    """
    Generic agent for custom/dynamic roles.
    Relies entirely on its persona prompt for behavior.
    """

    def run_cycle(self) -> dict:
        self.log("info", f"Generic agent '{self.name}' cycle gestartet")

        try:
            prompt = (
                f"Du bist {self.name}. Führe deine Aufgaben aus basierend auf deiner Persona.\n"
                f"Aktuelle Situation: Routine-Check. Berichte was du beobachtest oder empfiehlst."
            )
            response = self.think(prompt)

            if response:
                self.log("info", f"Response: {response[:200]}")
                return {"ok": True, "summary": response[:200]}

            return {"ok": True, "summary": "No response"}

        except Exception as e:
            self.log("error", f"Cycle fehlgeschlagen: {e}")
            return {"ok": False, "summary": str(e)}


def create_agent(config: AgentConfigYAML, telegram_bridge=None) -> BaseAgent:
    """
    Create an agent instance based on its role.
    Falls back to GenericAgent for unknown roles.
    """
    agent_cls = ROLE_MAP.get(config.role, GenericAgent)
    agent = agent_cls(config=config, telegram_bridge=telegram_bridge)
    logger.info(f"Created agent: {config.name} ({config.role}) -> {agent_cls.__name__}")
    return agent


def register_role(role_name: str, agent_cls: type[BaseAgent]) -> None:
    """Register a new role → agent class mapping (for plugins)."""
    ROLE_MAP[role_name] = agent_cls
    logger.info(f"Registered new agent role: {role_name} -> {agent_cls.__name__}")
