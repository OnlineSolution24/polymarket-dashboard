"""
Abstract base class for all agents.
Defines the contract every agent must implement.
"""

import json
import logging
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Optional

from config import AgentConfigYAML
from db import engine
from services.cost_tracker import log_cost, check_budget, estimate_tokens

logger = logging.getLogger(__name__)


class BaseAgent(ABC):
    """
    Base class for all agents (Chief, Observer, Analyst, etc.).
    Each agent can think (via OpenClaw/Telegram), log, and manage its memory.
    """

    def __init__(self, config: AgentConfigYAML, telegram_bridge=None):
        self.config = config
        self.bridge = telegram_bridge
        self.id = config.id
        self.name = config.name
        self.role = config.role

    @abstractmethod
    def run_cycle(self) -> dict:
        """
        Execute one cycle of agent work.
        Returns a result dict with at least {"ok": bool, "summary": str}.
        """
        ...

    def think(self, prompt: str) -> Optional[str]:
        """
        Send a prompt to OpenClaw via Telegram and get a response.
        Checks budget before sending. Logs cost after response.
        """
        # Check budget
        budget_status = check_budget(agent_id=self.id)
        if not budget_status["allowed"]:
            self.log("warn", f"Budget erschöpft: {budget_status['reason']}")
            return None

        # Build full prompt with agent context
        full_prompt = (
            f"[Agent: {self.name} | Rolle: {self.role}]\n"
            f"{self.config.persona}\n\n"
            f"---\n{prompt}"
        )

        # Send via bridge
        if self.bridge and self.bridge.is_connected():
            response = self.bridge.send_and_wait(full_prompt, timeout=120)
        else:
            self.log("warn", "Telegram Bridge nicht verbunden. Verwende Mock-Response.")
            response = f"[Mock] Antwort auf: {prompt[:100]}..."

        # Log cost
        if response:
            tokens_in = estimate_tokens(full_prompt)
            tokens_out = estimate_tokens(response)
            log_cost(
                provider=self.config.model,
                tokens_in=tokens_in,
                tokens_out=tokens_out,
                agent_id=self.id,
                endpoint="telegram_bridge",
            )

        return response

    def log(self, level: str, message: str, metadata: dict = None) -> None:
        """Write a log entry to the database."""
        engine.execute(
            "INSERT INTO agent_logs (agent_id, level, message, metadata, created_at) VALUES (?, ?, ?, ?, ?)",
            (self.id, level, message, json.dumps(metadata) if metadata else None, datetime.utcnow().isoformat()),
        )

    def create_suggestion(
        self,
        type: str,
        title: str,
        description: str = "",
        payload: dict = None,
    ) -> None:
        """Create a suggestion for the user to review."""
        engine.execute(
            """INSERT INTO suggestions (agent_id, type, title, description, payload, status, created_at)
               VALUES (?, ?, ?, ?, ?, 'pending', ?)""",
            (self.id, type, title, description, json.dumps(payload) if payload else None,
             datetime.utcnow().isoformat()),
        )
        self.log("info", f"Suggestion erstellt: {title}")

        # Send alert
        try:
            from config import AppConfig
            from services.telegram_alerts import get_alerts
            alerts = get_alerts(AppConfig.from_env())
            alerts.alert_new_suggestion(title, self.id)
        except Exception:
            pass

    def load_memory(self) -> dict:
        """Load agent's memory from JSON file."""
        from config import DATA_DIR
        memory_path = DATA_DIR / "memories" / f"{self.id}.json"
        if memory_path.exists():
            with open(memory_path, "r", encoding="utf-8") as f:
                return json.load(f)
        return {}

    def save_memory(self, data: dict) -> None:
        """Save agent's memory to JSON file."""
        from config import DATA_DIR
        memory_path = DATA_DIR / "memories" / f"{self.id}.json"
        memory_path.parent.mkdir(parents=True, exist_ok=True)
        with open(memory_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
