"""
Agent Registry - Runtime management of active agent instances.
"""

import logging
from typing import Optional

from config import load_agent_configs, AgentConfigYAML
from agents.base_agent import BaseAgent
from agents.agent_factory import create_agent

logger = logging.getLogger(__name__)


class AgentRegistry:
    """
    Manages the lifecycle of agent instances.
    Loads agents from YAML configs, starts/stops them, and runs cycles.
    """

    def __init__(self):
        self._agents: dict[str, BaseAgent] = {}

    def load_from_configs(self, telegram_bridge=None) -> None:
        """Load all enabled agents from YAML configs."""
        configs = load_agent_configs()

        for config in configs:
            if config.enabled and config.id not in self._agents:
                try:
                    agent = create_agent(config, telegram_bridge)
                    self._agents[config.id] = agent
                    logger.info(f"Agent loaded: {config.name}")
                except Exception as e:
                    logger.error(f"Failed to load agent {config.id}: {e}")

    def get(self, agent_id: str) -> Optional[BaseAgent]:
        """Get an agent by ID."""
        return self._agents.get(agent_id)

    def list_agents(self) -> list[BaseAgent]:
        """Get all registered agents."""
        return list(self._agents.values())

    def run_agent_cycle(self, agent_id: str) -> dict:
        """Run a single agent's cycle."""
        agent = self._agents.get(agent_id)
        if not agent:
            return {"ok": False, "summary": f"Agent {agent_id} not found"}

        try:
            result = agent.run_cycle()
            return result
        except Exception as e:
            logger.error(f"Agent {agent_id} cycle failed: {e}")
            return {"ok": False, "summary": str(e)}

    def run_all_cycles(self) -> dict[str, dict]:
        """Run all agents' cycles. Returns results per agent."""
        results = {}
        for agent_id, agent in self._agents.items():
            try:
                results[agent_id] = agent.run_cycle()
            except Exception as e:
                results[agent_id] = {"ok": False, "summary": str(e)}
        return results

    def remove(self, agent_id: str) -> bool:
        """Remove an agent from the registry."""
        if agent_id in self._agents:
            del self._agents[agent_id]
            return True
        return False

    @property
    def count(self) -> int:
        return len(self._agents)


# Singleton
_registry: Optional[AgentRegistry] = None


def get_registry() -> AgentRegistry:
    global _registry
    if _registry is None:
        _registry = AgentRegistry()
    return _registry
