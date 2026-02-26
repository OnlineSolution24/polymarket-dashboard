"""
Central configuration loader.
Reads .env for secrets and platform_config.yaml for platform settings.
Agent configs are loaded from agent_configs/*.yaml.
"""

import os
from dataclasses import dataclass, field
from pathlib import Path

import yaml
from dotenv import load_dotenv

# Project root
ROOT_DIR = Path(__file__).parent
DATA_DIR = ROOT_DIR / "data"
AGENT_CONFIGS_DIR = ROOT_DIR / "agent_configs"
PLUGINS_DIR = ROOT_DIR / "plugins"
PLATFORM_CONFIG_PATH = ROOT_DIR / "platform_config.yaml"

# Load .env
load_dotenv(ROOT_DIR / ".env")


@dataclass
class AppConfig:
    """Immutable app configuration from .env."""

    # Polymarket
    polymarket_host: str = ""
    polymarket_private_key: str = ""
    polymarket_chain_id: int = 137
    polymarket_funder: str = ""

    # Telegram / OpenClaw
    telegram_api_id: int = 0
    telegram_api_hash: str = ""
    telegram_bot_token: str = ""
    openclaw_chat_id: int = 0

    # Alerts (your personal Telegram)
    alert_telegram_user_id: int = 0

    # External APIs
    newsapi_key: str = ""
    openrouter_api_key: str = ""

    # App
    db_path: str = "data/dashboard.db"
    app_password: str = "changeme"

    @classmethod
    def from_env(cls) -> "AppConfig":
        return cls(
            polymarket_host=os.getenv("POLYMARKET_HOST", "https://clob.polymarket.com"),
            polymarket_private_key=os.getenv("POLYMARKET_PRIVATE_KEY", ""),
            polymarket_chain_id=int(os.getenv("POLYMARKET_CHAIN_ID", "137")),
            polymarket_funder=os.getenv("POLYMARKET_FUNDER", ""),
            telegram_api_id=int(os.getenv("TELEGRAM_API_ID", "0")),
            telegram_api_hash=os.getenv("TELEGRAM_API_HASH", ""),
            telegram_bot_token=os.getenv("TELEGRAM_BOT_TOKEN", ""),
            openclaw_chat_id=int(os.getenv("OPENCLAW_CHAT_ID", "0")),
            alert_telegram_user_id=int(os.getenv("ALERT_TELEGRAM_USER_ID", "0")),
            newsapi_key=os.getenv("NEWSAPI_KEY", ""),
            openrouter_api_key=os.getenv("OPENROUTER_API_KEY", ""),
            db_path=os.getenv("DB_PATH", "data/dashboard.db"),
            app_password=os.getenv("APP_PASSWORD", "changeme"),
        )


@dataclass
class AgentConfigYAML:
    """Parsed agent configuration from a YAML file."""

    id: str  # filename without .yaml
    name: str = ""
    role: str = "custom"
    persona: str = ""
    skills: list[str] = field(default_factory=list)
    schedule: str = "*/60 * * * *"
    budget_daily_usd: float = 0.50
    model: str = "haiku"
    enabled: bool = True
    priority: int = 5
    depends_on: list[str] = field(default_factory=list)


def load_platform_config() -> dict:
    """Load platform_config.yaml. Returns empty dict on error."""
    if not PLATFORM_CONFIG_PATH.exists():
        return {}
    with open(PLATFORM_CONFIG_PATH, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def save_platform_config(config: dict) -> None:
    """Save platform_config.yaml (for live editing via UI)."""
    with open(PLATFORM_CONFIG_PATH, "w", encoding="utf-8") as f:
        yaml.dump(config, f, default_flow_style=False, allow_unicode=True, sort_keys=False)


def load_agent_configs() -> list[AgentConfigYAML]:
    """Load all agent configs from agent_configs/*.yaml (skip _template)."""
    agents = []
    if not AGENT_CONFIGS_DIR.exists():
        return agents

    for yaml_file in sorted(AGENT_CONFIGS_DIR.glob("*.yaml")):
        if yaml_file.stem.startswith("_"):
            continue
        try:
            with open(yaml_file, "r", encoding="utf-8") as f:
                data = yaml.safe_load(f) or {}
            agent_data = data.get("agent", {})
            agents.append(AgentConfigYAML(
                id=yaml_file.stem,
                name=agent_data.get("name", yaml_file.stem),
                role=agent_data.get("role", "custom"),
                persona=agent_data.get("persona", ""),
                skills=agent_data.get("skills", []),
                schedule=agent_data.get("schedule", "*/60 * * * *"),
                budget_daily_usd=float(agent_data.get("budget_daily_usd", 0.50)),
                model=agent_data.get("model", "haiku"),
                enabled=agent_data.get("enabled", True),
                priority=int(agent_data.get("priority", 5)),
                depends_on=agent_data.get("depends_on", []),
            ))
        except Exception:
            continue  # Skip malformed configs

    return agents


def save_agent_config(agent: AgentConfigYAML) -> None:
    """Save an agent config to YAML (for dynamic creation)."""
    data = {
        "agent": {
            "name": agent.name,
            "role": agent.role,
            "persona": agent.persona,
            "skills": agent.skills,
            "schedule": agent.schedule,
            "budget_daily_usd": agent.budget_daily_usd,
            "model": agent.model,
            "enabled": agent.enabled,
            "priority": agent.priority,
            "depends_on": agent.depends_on,
        }
    }
    filepath = AGENT_CONFIGS_DIR / f"{agent.id}.yaml"
    with open(filepath, "w", encoding="utf-8") as f:
        yaml.dump(data, f, default_flow_style=False, allow_unicode=True, sort_keys=False)


def get_budget_config() -> dict:
    """Get budget settings from platform config."""
    platform = load_platform_config()
    return platform.get("budgets", {
        "monthly_total_usd": 50.0,
        "daily_limit_usd": 5.0,
        "per_agent_daily_usd": 1.0,
        "alert_threshold_percent": 80,
        "auto_pause_on_budget_exceeded": True,
    })


def get_alert_config() -> dict:
    """Get alert settings from platform config."""
    platform = load_platform_config()
    return platform.get("alerts", {"enabled": False})


def get_cache_config() -> dict:
    """Get cache settings from platform config."""
    platform = load_platform_config()
    return platform.get("cache", {"enabled": True, "ttl_minutes": 30, "max_entries": 1000})


def get_model_routing() -> dict:
    """Get model routing config."""
    platform = load_platform_config()
    return platform.get("model_routing", {"default": "claude-sonnet"})
