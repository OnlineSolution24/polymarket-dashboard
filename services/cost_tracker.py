"""
API Cost Tracker with budget enforcement.
Tracks costs per provider and agent, enforces daily/monthly limits.
"""

import logging
from datetime import date, datetime
from typing import Optional

from db import engine
from config import get_budget_config

logger = logging.getLogger(__name__)

# Approximate token costs per provider (USD per 1K tokens)
TOKEN_COSTS = {
    "claude-opus": {"input": 0.015, "output": 0.075},
    "claude-sonnet": {"input": 0.003, "output": 0.015},
    "haiku": {"input": 0.00025, "output": 0.00125},
    "gemini-flash": {"input": 0.000075, "output": 0.0003},
    "openrouter": {"input": 0.003, "output": 0.015},  # varies
    "newsapi": {"input": 0, "output": 0},  # flat rate
}


def estimate_cost(provider: str, tokens_in: int, tokens_out: int) -> float:
    """Estimate cost based on provider and token counts."""
    costs = TOKEN_COSTS.get(provider, TOKEN_COSTS["claude-sonnet"])
    cost = (tokens_in / 1000 * costs["input"]) + (tokens_out / 1000 * costs["output"])
    return round(cost, 6)


def estimate_tokens(text: str) -> int:
    """Rough token estimate (1 token ≈ 4 chars)."""
    return max(1, len(text) // 4)


def log_cost(
    provider: str,
    tokens_in: int = 0,
    tokens_out: int = 0,
    cost_usd: Optional[float] = None,
    agent_id: Optional[str] = None,
    endpoint: Optional[str] = None,
) -> None:
    """Log an API cost entry."""
    if cost_usd is None:
        cost_usd = estimate_cost(provider, tokens_in, tokens_out)

    engine.execute(
        """INSERT INTO api_costs (provider, endpoint, tokens_in, tokens_out, cost_usd, agent_id, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (provider, endpoint, tokens_in, tokens_out, cost_usd, agent_id, datetime.utcnow().isoformat()),
    )

    # Update agent's daily budget usage
    if agent_id:
        engine.execute(
            "UPDATE agents SET budget_used_today = budget_used_today + ? WHERE id = ?",
            (cost_usd, agent_id),
        )


def check_budget(agent_id: Optional[str] = None) -> dict:
    """
    Check if budget limits are exceeded.
    Returns: {"allowed": bool, "reason": str, "daily_used": float, "monthly_used": float}
    """
    budget = get_budget_config()
    today = date.today().isoformat()

    # Daily total
    daily_row = engine.query_one(
        "SELECT COALESCE(SUM(cost_usd), 0) as total FROM api_costs WHERE date(created_at) = ?",
        (today,),
    )
    daily_used = daily_row["total"] if daily_row else 0

    # Monthly total
    monthly_row = engine.query_one(
        "SELECT COALESCE(SUM(cost_usd), 0) as total FROM api_costs "
        "WHERE strftime('%Y-%m', created_at) = strftime('%Y-%m', 'now')"
    )
    monthly_used = monthly_row["total"] if monthly_row else 0

    daily_limit = budget.get("daily_limit_usd", 5.0)
    monthly_limit = budget.get("monthly_total_usd", 50.0)

    # Check daily limit
    if daily_used >= daily_limit:
        return {
            "allowed": False,
            "reason": f"Tagesbudget erschöpft (${daily_used:.2f} / ${daily_limit:.2f})",
            "daily_used": daily_used,
            "monthly_used": monthly_used,
        }

    # Check monthly limit
    if monthly_used >= monthly_limit:
        return {
            "allowed": False,
            "reason": f"Monatsbudget erschöpft (${monthly_used:.2f} / ${monthly_limit:.2f})",
            "daily_used": daily_used,
            "monthly_used": monthly_used,
        }

    # Check per-agent daily limit
    if agent_id:
        per_agent_limit = budget.get("per_agent_daily_usd", 1.0)
        agent_row = engine.query_one(
            "SELECT COALESCE(budget_used_today, 0) as used FROM agents WHERE id = ?",
            (agent_id,),
        )
        agent_used = agent_row["used"] if agent_row else 0
        if agent_used >= per_agent_limit:
            return {
                "allowed": False,
                "reason": f"Agent-Budget erschöpft (${agent_used:.2f} / ${per_agent_limit:.2f})",
                "daily_used": daily_used,
                "monthly_used": monthly_used,
            }

    return {
        "allowed": True,
        "reason": "OK",
        "daily_used": daily_used,
        "monthly_used": monthly_used,
    }


def reset_daily_budgets() -> None:
    """Reset all agents' daily budget counters. Called by scheduler at midnight."""
    today = date.today().isoformat()
    engine.execute(
        "UPDATE agents SET budget_used_today = 0, last_reset_date = ?",
        (today,),
    )
    logger.info("Daily agent budgets reset")


def get_today_costs_by_provider() -> dict:
    """Get today's costs grouped by provider."""
    today = date.today().isoformat()
    rows = engine.query(
        "SELECT provider, SUM(cost_usd) as total FROM api_costs WHERE date(created_at) = ? GROUP BY provider",
        (today,),
    )
    return {r["provider"]: r["total"] for r in rows}


def get_monthly_total() -> float:
    """Get total costs for current month."""
    row = engine.query_one(
        "SELECT COALESCE(SUM(cost_usd), 0) as total FROM api_costs "
        "WHERE strftime('%Y-%m', created_at) = strftime('%Y-%m', 'now')"
    )
    return row["total"] if row else 0
