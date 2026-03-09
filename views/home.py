"""
Home Page - KPI Dashboard Overview.
Shows the 5 most important metrics at a glance.
All data loaded from Bot REST API + OpenRouter API.
"""

import os
import streamlit as st
from datetime import datetime

from services.bot_api_client import get_bot_client
from services.openrouter_costs import get_openrouter_costs
from components.status_cards import kpi_row, status_badge

OPENROUTER_API_KEY = os.getenv(
    "OPENROUTER_API_KEY",
    "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",
)


def render():
    st.header("Dashboard Overview")

    client = get_bot_client()
    status = client.get_status()

    if not status:
        st.error("Bot API nicht erreichbar. Prüfe die Verbindung.")
        return

    # --- Fetch real OpenRouter costs ---
    or_costs = get_openrouter_costs(OPENROUTER_API_KEY) or {}
    today_cost = or_costs.get("usage_daily", status.get("cost_today_usd", 0))
    limit_remaining = or_costs.get("limit_remaining")

    # --- KPI Row 1: Key Numbers ---
    active_agents = status.get("active_agents", 0)
    open_positions = status.get("open_positions", 0)
    today_pnl = status.get("pnl_today", 0)
    pending_suggestions = status.get("pending_suggestions", 0)

    kpi_items = [
        {"label": "Aktive Agents", "value": active_agents},
        {"label": "Offene Positionen", "value": open_positions},
        {"label": "PnL Heute", "value": f"${today_pnl:+.2f}", "delta_color": "normal" if today_pnl >= 0 else "inverse"},
        {"label": "AI-Kosten Heute (OR)", "value": f"${today_cost:.4f}"},
        {"label": "Pending Suggestions", "value": pending_suggestions},
    ]
    if limit_remaining is not None:
        kpi_items.append({"label": "Budget Rest", "value": f"${limit_remaining:.2f}"})

    kpi_row(kpi_items)

    st.divider()

    # --- Circuit Breaker Status ---
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Circuit Breaker")
        cb = status.get("circuit_breaker", {})
        paused_until = cb.get("paused_until")
        losses = cb.get("consecutive_losses", 0)

        is_paused = False
        if paused_until:
            try:
                if datetime.fromisoformat(paused_until) > datetime.utcnow():
                    is_paused = True
            except Exception:
                pass

        if is_paused:
            status_badge("Trading", "paused")
            st.warning(f"Pausiert bis: {paused_until}")
        else:
            status_badge("Trading", "active")

        st.caption(f"Verluste in Folge: {losses}/3")

    with col2:
        st.subheader("Budget Status (OpenRouter)")
        monthly_cost = or_costs.get("usage_monthly", status.get("cost_month_usd", 0))
        weekly_cost = or_costs.get("usage_weekly", 0)
        total_usage = or_costs.get("usage", 0)
        limit = or_costs.get("limit")

        if limit and limit > 0:
            monthly_pct = min(total_usage / limit, 1.0)
            st.progress(monthly_pct, text=f"Gesamt: ${total_usage:.2f} / ${limit:.2f}")
        else:
            st.caption(f"Gesamt-Verbrauch: ${total_usage:.2f} (kein Limit)")

        st.caption(f"Heute: ${today_cost:.4f} | Woche: ${weekly_cost:.4f} | Monat: ${monthly_cost:.4f}")

    st.divider()

    # --- Recent Activity ---
    st.subheader("Letzte Aktivität")
    recent_logs = client.get_logs(limit=10)
    if recent_logs:
        for log in recent_logs:
            level_icon = {"info": "ℹ️", "warn": "⚠️", "error": "❌", "debug": "🔍"}.get(log.get("level", ""), "📝")
            ts = (log.get("created_at") or "")[:16]
            st.caption(f"{level_icon} `{ts}` **{log.get('agent_id', 'system')}**: {log.get('message', '')}")
    else:
        st.info("Noch keine Agent-Aktivitäten.")
