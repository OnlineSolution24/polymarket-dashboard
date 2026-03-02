"""
MCP Tool Server for OpenClaw Agents.

Exposes Polymarket bot functionality as MCP tools that OpenClaw agents
can call. Communicates with the Bot REST API over HTTP.

Usage:
    python mcp/tool_server.py

Environment:
    BOT_API_URL  - Bot REST API base URL (default: http://polymarket-bot:8000)
    BOT_API_KEY  - API key for authentication
"""

import json
import logging
import os
import sys
from typing import Any, Optional

import httpx
from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import TextContent, Tool

logger = logging.getLogger("mcp-polymarket")

BOT_API_URL = os.getenv("BOT_API_URL", "http://polymarket-bot:8000")
BOT_API_KEY = os.getenv("BOT_API_KEY", "")
_TIMEOUT = 30.0

server = Server("polymarket-tools")


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

def _headers() -> dict:
    return {"Authorization": f"Bearer {BOT_API_KEY}", "Content-Type": "application/json"}


def _api_get(path: str, params: dict | None = None) -> Any:
    """GET request to bot API."""
    try:
        resp = httpx.get(
            f"{BOT_API_URL}{path}",
            headers=_headers(),
            params={k: v for k, v in (params or {}).items() if v is not None},
            timeout=_TIMEOUT,
        )
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        logger.error(f"API GET {path} failed: {e}")
        return {"error": str(e)}


def _api_post(path: str, body: dict | None = None) -> Any:
    """POST request to bot API."""
    try:
        resp = httpx.post(
            f"{BOT_API_URL}{path}",
            headers=_headers(),
            json=body,
            timeout=_TIMEOUT,
        )
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        logger.error(f"API POST {path} failed: {e}")
        return {"error": str(e)}


def _api_put(path: str, body: dict | None = None) -> Any:
    """PUT request to bot API."""
    try:
        resp = httpx.put(
            f"{BOT_API_URL}{path}",
            headers=_headers(),
            json=body,
            timeout=_TIMEOUT,
        )
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        logger.error(f"API PUT {path} failed: {e}")
        return {"error": str(e)}


def _api_delete(path: str) -> Any:
    """DELETE request to bot API."""
    try:
        resp = httpx.delete(
            f"{BOT_API_URL}{path}",
            headers=_headers(),
            timeout=_TIMEOUT,
        )
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        logger.error(f"API DELETE {path} failed: {e}")
        return {"error": str(e)}


# ---------------------------------------------------------------------------
# Tool definitions
# ---------------------------------------------------------------------------

TOOLS = [
    # --- Market Tools ---
    Tool(
        name="get_markets",
        description="Aktuelle Polymarket-Märkte abrufen. Gibt Liste mit Preisen, Volumen, Liquidität zurück.",
        inputSchema={
            "type": "object",
            "properties": {
                "limit": {"type": "integer", "description": "Max Anzahl Märkte (1-200)", "default": 50},
                "category": {"type": "string", "description": "Filter nach Kategorie (z.B. politics, crypto, sports)"},
            },
        },
    ),
    Tool(
        name="get_market_snapshots",
        description="Historische Preisbewegungen eines Marktes. Gut für Trend-Analyse.",
        inputSchema={
            "type": "object",
            "properties": {
                "market_id": {"type": "string", "description": "Market ID"},
                "hours": {"type": "integer", "description": "Stunden zurück (1-720)", "default": 48},
            },
            "required": ["market_id"],
        },
    ),

    # --- Strategy Tools ---
    Tool(
        name="save_strategy",
        description="Neue Trading-Strategie speichern. Status wird automatisch auf 'pending_backtest' gesetzt.",
        inputSchema={
            "type": "object",
            "properties": {
                "name": {"type": "string", "description": "Name der Strategie"},
                "description": {"type": "string", "description": "Kurzbeschreibung"},
                "hypothesis": {"type": "string", "description": "Warum sollte diese Strategie funktionieren?"},
                "entry_rules": {
                    "type": "array",
                    "description": "Entry-Regeln als Liste von {field, op, value}. "
                                   "Felder: yes_price, no_price, volume, liquidity, sentiment_score, "
                                   "calculated_edge, days_to_expiry. Ops: gt, lt, gte, lte, eq.",
                    "items": {"type": "object"},
                },
                "exit_rules": {"type": "array", "items": {"type": "object"}},
                "trade_params": {
                    "type": "object",
                    "description": "Trade-Parameter: {side: YES|NO, sizing_method: kelly|fixed_pct, "
                                   "sizing_value: float, min_edge: float}",
                },
                "category_filter": {"type": "array", "items": {"type": "string"}},
                "min_liquidity": {"type": "number", "default": 500},
                "discovered_by": {"type": "string", "description": "Agent ID der die Strategie erstellt hat"},
            },
            "required": ["name", "entry_rules", "trade_params"],
        },
    ),
    Tool(
        name="get_strategies",
        description="Alle Strategien mit Metriken abrufen. Filterbar nach Status.",
        inputSchema={
            "type": "object",
            "properties": {
                "status": {
                    "type": "string",
                    "description": "Filter: draft, pending_backtest, backtested, validated, active, retired, rejected",
                },
            },
        },
    ),
    Tool(
        name="get_strategy_detail",
        description="Detail einer Strategie inkl. Backtest-Ergebnisse und verknüpfte Trades.",
        inputSchema={
            "type": "object",
            "properties": {"strategy_id": {"type": "string"}},
            "required": ["strategy_id"],
        },
    ),
    Tool(
        name="update_strategy_status",
        description="Status einer Strategie ändern (z.B. active, retired, rejected).",
        inputSchema={
            "type": "object",
            "properties": {
                "strategy_id": {"type": "string"},
                "status": {"type": "string", "description": "Neuer Status: active, retired, rejected, validated"},
                "approved_by": {"type": "string", "description": "Wer hat genehmigt (z.B. chief, user)", "default": "chief"},
            },
            "required": ["strategy_id", "status"],
        },
    ),
    Tool(
        name="run_backtest",
        description="Backtest einer Strategie starten: Simulator + Walk-Forward + Monte Carlo. "
                    "Ergebnisse werden in der DB gespeichert.",
        inputSchema={
            "type": "object",
            "properties": {"strategy_id": {"type": "string"}},
            "required": ["strategy_id"],
        },
    ),
    Tool(
        name="check_strategy_signals",
        description="Prüfe welche aktuellen Märkte die Regeln einer aktiven Strategie erfüllen. "
                    "Gibt Trade-Signale zurück.",
        inputSchema={
            "type": "object",
            "properties": {"strategy_id": {"type": "string"}},
            "required": ["strategy_id"],
        },
    ),

    # --- Analytics Tools ---
    Tool(
        name="get_pattern_analysis",
        description="Historische Win Rates nach Kategorie, Preis-Bucket, Volumen-Bucket und Seite. "
                    "Essentiell für Strategy Discovery.",
        inputSchema={"type": "object", "properties": {}},
    ),

    # --- Trading Tools ---
    Tool(
        name="place_trade",
        description="Trade ausführen (mit allen Safety Checks). Im Paper-Modus wird simuliert.",
        inputSchema={
            "type": "object",
            "properties": {
                "market_id": {"type": "string"},
                "side": {"type": "string", "enum": ["YES", "NO"]},
                "amount": {"type": "number", "description": "Betrag in USD"},
            },
            "required": ["market_id", "side", "amount"],
        },
    ),
    Tool(
        name="simulate_trade",
        description="Paper Trade simulieren (kein echtes Geld).",
        inputSchema={
            "type": "object",
            "properties": {
                "market_id": {"type": "string"},
                "side": {"type": "string", "enum": ["YES", "NO"]},
                "amount": {"type": "number"},
            },
            "required": ["market_id", "side", "amount"],
        },
    ),
    Tool(
        name="check_risk",
        description="Risk-Validierung ohne Trade. Prüft Circuit Breaker, Daily Loss, Position Size.",
        inputSchema={
            "type": "object",
            "properties": {
                "market_id": {"type": "string"},
                "side": {"type": "string", "enum": ["YES", "NO"]},
                "amount": {"type": "number"},
            },
            "required": ["market_id", "side", "amount"],
        },
    ),
    Tool(
        name="get_trades",
        description="Trade-History abrufen.",
        inputSchema={
            "type": "object",
            "properties": {
                "limit": {"type": "integer", "default": 50},
                "status": {"type": "string"},
            },
        },
    ),
    Tool(
        name="get_trade_stats",
        description="Trading-Performance: Gesamt-Trades, Wins, Losses, PnL.",
        inputSchema={"type": "object", "properties": {}},
    ),
    Tool(
        name="get_circuit_breaker",
        description="Circuit Breaker Status (Verlust-Streak, Pause-Zeitpunkt).",
        inputSchema={"type": "object", "properties": {}},
    ),
    Tool(
        name="reset_circuit_breaker",
        description="Circuit Breaker zurücksetzen.",
        inputSchema={"type": "object", "properties": {}},
    ),

    # --- System Tools ---
    Tool(
        name="get_bot_health",
        description="System-Health Check: DB-Status, Error-Count, letzte Aktivität.",
        inputSchema={"type": "object", "properties": {}},
    ),
    Tool(
        name="get_bot_status",
        description="Bot-Status: Trading-Modus, aktive Agents, Kosten, PnL, Circuit Breaker.",
        inputSchema={"type": "object", "properties": {}},
    ),
    Tool(
        name="get_costs",
        description="API-Kosten Übersicht (täglich, monatlich, nach Provider/Agent).",
        inputSchema={
            "type": "object",
            "properties": {"days": {"type": "integer", "default": 7}},
        },
    ),
    Tool(
        name="get_config",
        description="Platform-Konfiguration lesen (Trading-Modus, Budgets, Scheduler, etc.).",
        inputSchema={"type": "object", "properties": {}},
    ),
    Tool(
        name="log_event",
        description="Event in die Bot-Datenbank loggen (für Audit Trail).",
        inputSchema={
            "type": "object",
            "properties": {
                "agent_id": {"type": "string", "description": "Deine Agent-ID"},
                "level": {"type": "string", "enum": ["debug", "info", "warn", "error"], "default": "info"},
                "message": {"type": "string", "description": "Log-Nachricht"},
            },
            "required": ["agent_id", "message"],
        },
    ),
    Tool(
        name="get_recent_errors",
        description="Letzte Fehler-Logs abrufen.",
        inputSchema={
            "type": "object",
            "properties": {
                "hours": {"type": "integer", "default": 24},
                "limit": {"type": "integer", "default": 50},
            },
        },
    ),
]


# ---------------------------------------------------------------------------
# Tool handler
# ---------------------------------------------------------------------------

@server.list_tools()
async def list_tools() -> list[Tool]:
    return TOOLS


@server.call_tool()
async def call_tool(name: str, arguments: dict) -> list[TextContent]:
    """Route tool calls to the Bot REST API."""
    result = _dispatch(name, arguments)
    return [TextContent(type="text", text=json.dumps(result, ensure_ascii=False, default=str))]


def _dispatch(name: str, args: dict) -> Any:
    """Dispatch tool call to appropriate API endpoint."""

    # --- Market Tools ---
    if name == "get_markets":
        return _api_get("/api/markets", {"limit": args.get("limit", 50), "category": args.get("category")})

    if name == "get_market_snapshots":
        return _api_get(f"/api/snapshots/{args['market_id']}", {"hours": args.get("hours", 48)})

    # --- Strategy Tools ---
    if name == "save_strategy":
        return _api_post("/api/strategies", {
            "name": args.get("name", "Unnamed Strategy"),
            "description": args.get("description", ""),
            "hypothesis": args.get("hypothesis", ""),
            "entry_rules": args.get("entry_rules", []),
            "exit_rules": args.get("exit_rules", []),
            "trade_params": args.get("trade_params", {}),
            "category_filter": args.get("category_filter", []),
            "min_liquidity": args.get("min_liquidity", 500),
            "discovered_by": args.get("discovered_by", "openclaw_agent"),
        })

    if name == "get_strategies":
        params = {}
        if args.get("status"):
            params["status"] = args["status"]
        return _api_get("/api/strategies", params)

    if name == "get_strategy_detail":
        return _api_get(f"/api/strategies/{args['strategy_id']}")

    if name == "update_strategy_status":
        return _api_put(f"/api/strategies/{args['strategy_id']}/status", {
            "status": args["status"],
            "approved_by": args.get("approved_by", "chief"),
        })

    if name == "run_backtest":
        return _api_post(f"/api/backtest/{args['strategy_id']}")

    if name == "check_strategy_signals":
        return _api_get(f"/api/analytics/strategy-signals/{args['strategy_id']}")

    # --- Analytics ---
    if name == "get_pattern_analysis":
        return _api_get("/api/analytics/patterns")

    # --- Trading ---
    if name == "place_trade":
        return _api_post("/api/trades/execute", {
            "market_id": args["market_id"],
            "side": args["side"],
            "amount": args["amount"],
        })

    if name == "simulate_trade":
        return _api_post("/api/trades/simulate", {
            "market_id": args["market_id"],
            "side": args["side"],
            "amount": args["amount"],
        })

    if name == "check_risk":
        return _api_post("/api/trades/check-risk", {
            "market_id": args["market_id"],
            "side": args["side"],
            "amount": args["amount"],
        })

    if name == "get_trades":
        return _api_get("/api/trades", {"limit": args.get("limit", 50), "status": args.get("status")})

    if name == "get_trade_stats":
        return _api_get("/api/trades/stats")

    if name == "get_circuit_breaker":
        return _api_get("/api/circuit-breaker")

    if name == "reset_circuit_breaker":
        return _api_post("/api/circuit-breaker/reset")

    # --- System ---
    if name == "get_bot_health":
        return _api_get("/api/monitor/health")

    if name == "get_bot_status":
        return _api_get("/api/status")

    if name == "get_costs":
        return _api_get("/api/costs", {"days": args.get("days", 7)})

    if name == "get_config":
        return _api_get("/api/config")

    if name == "log_event":
        return _api_post("/api/logs", {
            "agent_id": args["agent_id"],
            "level": args.get("level", "info"),
            "message": args["message"],
        })

    if name == "get_recent_errors":
        return _api_get("/api/monitor/errors", {"hours": args.get("hours", 24), "limit": args.get("limit", 50)})

    return {"error": f"Unknown tool: {name}"}


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def main():
    logging.basicConfig(level=logging.INFO, stream=sys.stderr)
    logger.info(f"Starting MCP server (API: {BOT_API_URL})")
    async with stdio_server() as (read_stream, write_stream):
        await server.run(read_stream, write_stream, server.create_initialization_options())


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
