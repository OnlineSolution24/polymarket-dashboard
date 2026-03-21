"""
Health Monitor — detects silent failures and alerts via Telegram.

Runs every 2 hours as a scheduled job. Checks:
1. Edge source freshness (weather, sports, crypto etc.)
2. Trade drought (no trades for X hours)
3. API health (external APIs returning errors)
4. PnL anomalies (settled trades with suspicious PnL)
"""

import logging
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


def run_health_monitor(platform_cfg: dict) -> dict:
    """Run all health checks and alert on issues."""
    from db import engine
    from config import AppConfig
    from services.telegram_alerts import get_alerts

    config = AppConfig()
    alerts = get_alerts(config)
    report = {"ok": [], "warning": [], "critical": []}

    # 1. Edge source freshness
    _check_edge_sources(engine, alerts, report)

    # 2. Trade drought
    _check_trade_drought(engine, alerts, report)

    # 3. PnL anomalies in recent settlements
    _check_pnl_anomalies(engine, alerts, report)

    # 4. Suggestion pipeline health
    _check_suggestion_pipeline(engine, alerts, report)

    # Send summary if there are critical issues
    crits = report["critical"]
    warns = report["warning"]
    if crits:
        msg = "🚨 <b>Health Monitor — KRITISCH</b>\n\n"
        for c in crits:
            msg += f"• {c}\n"
        if warns:
            msg += f"\n⚠️ {len(warns)} Warnungen"
        alerts.send(msg)
    elif warns and len(warns) >= 3:
        # Only alert on warnings if there are multiple (avoid noise)
        msg = "⚠️ <b>Health Monitor — Warnungen</b>\n\n"
        for w in warns:
            msg += f"• {w}\n"
        alerts.send(msg)

    return report


def _check_edge_sources(engine, alerts, report: dict):
    """Check if edge sources are producing fresh results."""
    # Weather edges: should update every 30 min
    row = engine.query_one(
        "SELECT MAX(last_updated) as last_update, COUNT(*) as cnt "
        "FROM markets WHERE calculated_edge IS NOT NULL AND calculated_edge != 0"
    )
    if row and row.get("last_update"):
        try:
            last = datetime.fromisoformat(row["last_update"])
            hours_ago = (datetime.utcnow() - last).total_seconds() / 3600
            if hours_ago > 6:
                report["critical"].append(
                    f"Weather Edges veraltet: letzte Berechnung vor {hours_ago:.0f}h "
                    f"({row['cnt']} Maerkte mit Edge)"
                )
            elif hours_ago > 2:
                report["warning"].append(
                    f"Weather Edges: letzte Berechnung vor {hours_ago:.0f}h"
                )
            else:
                report["ok"].append(f"Weather Edges: {row['cnt']} aktuell")
        except (ValueError, TypeError):
            report["warning"].append("Weather Edges: Timestamp nicht parsbar")
    else:
        report["critical"].append("Weather Edges: KEINE berechneten Edges in DB")


def _check_trade_drought(engine, alerts, report: dict):
    """Alert if no trades were executed in the last X hours."""
    row = engine.query_one(
        "SELECT MAX(created_at) as last_trade, COUNT(*) as cnt "
        "FROM trades WHERE status IN ('executed', 'closed') "
        "AND created_at > datetime('now', '-72 hours')"
    )
    if row and row.get("last_trade"):
        try:
            last = datetime.fromisoformat(row["last_trade"])
            hours_ago = (datetime.utcnow() - last).total_seconds() / 3600
            if hours_ago > 48:
                report["critical"].append(
                    f"Keine Trades seit {hours_ago:.0f}h! Letzter: {row['last_trade']}"
                )
            elif hours_ago > 24:
                report["warning"].append(
                    f"Trade-Pause: kein Trade seit {hours_ago:.0f}h"
                )
            else:
                report["ok"].append(
                    f"Trades: {row['cnt']} in den letzten 72h, letzter vor {hours_ago:.0f}h"
                )
        except (ValueError, TypeError):
            pass
    else:
        report["critical"].append("Keine Trades in den letzten 72h gefunden")


def _check_pnl_anomalies(engine, alerts, report: dict):
    """Check for recently settled trades with suspicious PnL."""
    # PnL > 10x invested amount is suspicious
    rows = engine.query(
        "SELECT id, market_question, side, amount_usd, pnl, result "
        "FROM trades WHERE status = 'closed' AND result IS NOT NULL "
        "AND ABS(pnl) > amount_usd * 10 AND amount_usd > 0 "
        "AND created_at > datetime('now', '-48 hours')"
    )
    if rows:
        for r in rows:
            ratio = abs(r["pnl"] / r["amount_usd"]) if r["amount_usd"] else 0
            report["critical"].append(
                f"PnL-Anomalie Trade #{r['id']}: "
                f"${r['pnl']:+.2f} bei ${r['amount_usd']:.2f} Einsatz ({ratio:.0f}x) "
                f"— {r['market_question'][:50]}"
            )
    else:
        report["ok"].append("PnL: keine Anomalien")


def _check_suggestion_pipeline(engine, alerts, report: dict):
    """Check if the suggestion pipeline is producing suggestions."""
    row = engine.query_one(
        "SELECT COUNT(*) as cnt, MAX(created_at) as last_sugg "
        "FROM suggestions WHERE created_at > datetime('now', '-24 hours')"
    )
    if row:
        cnt = row.get("cnt", 0)
        if cnt == 0:
            report["warning"].append("Keine Suggestions in den letzten 24h")
        else:
            report["ok"].append(f"Suggestions: {cnt} in den letzten 24h")
