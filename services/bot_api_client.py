"""
Bot API Client for the Monitoring Dashboard.
Wraps all REST API calls to the Trading Bot.
Used by Dashboard pages instead of direct DB queries.
"""

import logging
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

_DEFAULT_TIMEOUT = 15.0


class BotAPIClient:
    """HTTP client that talks to the Trading Bot REST API."""

    def __init__(self, base_url: str, api_key: str):
        self.base_url = base_url.rstrip("/")
        self._headers = {"Authorization": f"Bearer {api_key}"}

    def _get(self, path: str, params: dict = None) -> dict | list | None:
        try:
            resp = httpx.get(
                f"{self.base_url}{path}",
                headers=self._headers,
                params=params,
                timeout=_DEFAULT_TIMEOUT,
            )
            resp.raise_for_status()
            return resp.json()
        except httpx.HTTPStatusError as e:
            logger.error(f"API error {e.response.status_code}: {path}")
            return None
        except Exception as e:
            logger.error(f"API request failed: {path} → {e}")
            return None

    def _post(self, path: str, json_body: dict = None) -> dict | None:
        try:
            resp = httpx.post(
                f"{self.base_url}{path}",
                headers=self._headers,
                json=json_body,
                timeout=_DEFAULT_TIMEOUT,
            )
            resp.raise_for_status()
            return resp.json()
        except httpx.HTTPStatusError as e:
            logger.error(f"API error {e.response.status_code}: {path}")
            try:
                return e.response.json()
            except Exception:
                return {"detail": f"HTTP {e.response.status_code}"}
        except Exception as e:
            logger.error(f"API request failed: {path} → {e}")
            return {"detail": str(e)}

    def _put(self, path: str, json_body: dict = None) -> dict | None:
        try:
            resp = httpx.put(
                f"{self.base_url}{path}",
                headers=self._headers,
                json=json_body,
                timeout=_DEFAULT_TIMEOUT,
            )
            resp.raise_for_status()
            return resp.json()
        except httpx.HTTPStatusError as e:
            logger.error(f"API error {e.response.status_code}: {path}")
            return None
        except Exception as e:
            logger.error(f"API request failed: {path} → {e}")
            return None

    def _delete(self, path: str) -> dict | None:
        try:
            resp = httpx.delete(
                f"{self.base_url}{path}",
                headers=self._headers,
                timeout=_DEFAULT_TIMEOUT,
            )
            resp.raise_for_status()
            return resp.json()
        except httpx.HTTPStatusError as e:
            logger.error(f"API error {e.response.status_code}: {path}")
            return None
        except Exception as e:
            logger.error(f"API request failed: {path} → {e}")
            return None

    # ------------------------------------------------------------------
    # Read endpoints
    # ------------------------------------------------------------------

    def get_status(self) -> dict | None:
        return self._get("/api/status")

    def get_markets(self, limit: int = 50, category: str = None) -> list:
        params = {"limit": limit}
        if category:
            params["category"] = category
        return self._get("/api/markets", params) or []

    def get_trades(self, limit: int = 50, status: str = None) -> list:
        params = {"limit": limit}
        if status:
            params["status"] = status
        return self._get("/api/trades", params) or []

    def get_trade_stats(self) -> dict:
        return self._get("/api/trades/stats") or {"total": 0, "wins": 0, "losses": 0, "total_pnl": 0}

    def get_open_positions(self) -> list:
        return self._get("/api/trades/positions") or []

    def get_closed_trades(self) -> list:
        return self._get("/api/trades/closed") or []

    def get_performance(self) -> dict:
        return self._get("/api/trades/performance") or {
            "pnl_today": 0, "pnl_7d": 0, "pnl_30d": 0, "pnl_all": 0, "equity_curve": []
        }

    def get_agents(self) -> list:
        return self._get("/api/agents") or []

    def get_logs(self, agent_id: str = None, level: str = None, limit: int = 100) -> list:
        params = {"limit": limit}
        if agent_id:
            params["agent_id"] = agent_id
        if level:
            params["level"] = level
        return self._get("/api/logs", params) or []

    def get_costs(self, days: int = 7) -> dict:
        return self._get("/api/costs", {"days": days}) or {"daily_total": 0, "monthly_total": 0, "entries": []}

    def get_suggestions(self, status: str = None, limit: int = 50) -> list:
        params = {"limit": limit}
        if status:
            params["status"] = status
        return self._get("/api/suggestions", params) or []

    def get_circuit_breaker(self) -> dict:
        return self._get("/api/circuit-breaker") or {"consecutive_losses": 0, "paused_until": None}

    def get_config(self) -> dict:
        return self._get("/api/config") or {}

    def save_config(self, config: dict) -> dict | None:
        return self._post("/api/config", config)

    def save_setting(self, key: str, value) -> dict | None:
        return self._post(f"/api/settings/{key}", {"value": value})

    # ------------------------------------------------------------------
    # Write endpoints
    # ------------------------------------------------------------------

    def respond_suggestion(self, suggestion_id: int, action: str, note: str = None) -> dict | None:
        return self._post(f"/api/suggestions/{suggestion_id}/respond", {"action": action, "note": note})

    def reset_circuit_breaker(self) -> dict | None:
        return self._post("/api/circuit-breaker/reset")

    def pause_bot(self) -> dict | None:
        return self._post("/api/bot/pause")

    def resume_bot(self) -> dict | None:
        return self._post("/api/bot/resume")

    def reload_scheduler(self) -> dict | None:
        return self._post("/api/scheduler/reload")

    # ------------------------------------------------------------------
    # Strategy endpoints
    # ------------------------------------------------------------------

    def get_strategies(self, status: str = None) -> list:
        params = {}
        if status:
            params["status"] = status
        return self._get("/api/strategies", params) or []

    def get_strategy(self, strategy_id: str) -> dict | None:
        return self._get(f"/api/strategies/{strategy_id}")

    def create_strategy(self, data: dict) -> dict | None:
        return self._post("/api/strategies", data)

    def update_strategy(self, strategy_id: str, data: dict) -> dict | None:
        return self._put(f"/api/strategies/{strategy_id}", data)

    def update_strategy_status(self, strategy_id: str, status: str, approved_by: str = "user") -> dict | None:
        return self._put(f"/api/strategies/{strategy_id}/status", {"status": status, "approved_by": approved_by})

    def delete_strategy(self, strategy_id: str) -> dict | None:
        return self._delete(f"/api/strategies/{strategy_id}")

    def manual_cashout(self, trade_id: int) -> dict | None:
        return self._post("/api/trades/cashout", {"trade_id": trade_id})

    def import_position(self, data: dict) -> dict | None:
        return self._post("/api/trades/import", data)

    # ------------------------------------------------------------------
    # Backtest endpoints
    # ------------------------------------------------------------------

    def run_backtest(self, strategy_id: str) -> dict | None:
        return self._post(f"/api/backtest/{strategy_id}")

    def get_backtest_results(self, strategy_id: str) -> dict | None:
        return self._get(f"/api/backtest/{strategy_id}/results")

    # ------------------------------------------------------------------
    # Analytics endpoints
    # ------------------------------------------------------------------

    def get_patterns(self) -> dict | None:
        return self._get("/api/analytics/patterns")

    def get_strategy_signals(self, strategy_id: str) -> list:
        return self._get(f"/api/analytics/strategy-signals/{strategy_id}") or []

    # ------------------------------------------------------------------
    # Snapshot endpoints
    # ------------------------------------------------------------------

    def get_snapshots(self, market_id: str, hours: int = 48) -> list:
        return self._get(f"/api/snapshots/{market_id}", {"hours": hours}) or []

    # ------------------------------------------------------------------
    # Code changes (Self-Modification)
    # ------------------------------------------------------------------

    def get_pending_code_changes(self) -> list:
        data = self._get("/api/code/pending")
        return (data or {}).get("pending", [])

    def get_code_change_history(self, status: str = None, limit: int = 50) -> list:
        params = {"limit": limit}
        if status:
            params["status"] = status
        data = self._get("/api/code/history", params)
        return (data or {}).get("changes", [])

    def get_code_change_details(self, change_id: int) -> dict | None:
        return self._get(f"/api/code/{change_id}")

    def approve_code_change(self, change_id: int, approved: bool, comment: str = None) -> dict | None:
        return self._post(f"/api/code/{change_id}/approve", {"approved": approved, "user_comment": comment})

    def rollback_code_change(self, change_id: int) -> dict | None:
        return self._post(f"/api/code/{change_id}/rollback")

    # ------------------------------------------------------------------
    # Health check
    # ------------------------------------------------------------------

    def is_reachable(self) -> bool:
        try:
            resp = httpx.get(
                f"{self.base_url}/api/docs",
                timeout=5.0,
            )
            return resp.status_code == 200
        except Exception:
            return False


# ---------------------------------------------------------------------------
# Singleton
# ---------------------------------------------------------------------------

_client: Optional[BotAPIClient] = None


def get_bot_client() -> BotAPIClient:
    """Get or create the singleton BotAPIClient from environment."""
    global _client
    if _client is None:
        import os
        base_url = os.getenv("BOT_API_URL", "http://localhost:8000")
        api_key = os.getenv("BOT_API_KEY", "")
        _client = BotAPIClient(base_url, api_key)
    return _client
