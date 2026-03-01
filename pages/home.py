"""
Home Page - KPI Dashboard Overview.
Shows the 5 most important metrics at a glance.
All data loaded from Bot REST API.
"""

import streamlit as st
from datetime import datetime

from services.bot_api_client import get_bot_client
from components.status_cards import kpi_row, status_badge


def render():
    st.header("Dashboard Overview")

    client = get_bot_client()
    status = client.get_status()

    if not status:
        st.error("Bot API nicht erreichbar. Prüfe die Verbindung.")
        return

    # --- KPI Row 1: Key Numbers ---
    active_agents = status.get("active_agents", 0)
    open_positions = status.get("open_positions", 0)
    today_pnl = status.get("pnl_today", 0)
    today_cost = status.get("cost_today_usd", 0)
    pending_suggestions = status.get("pending_suggestions", 0)

    kpi_row([
        {"label": "Aktive Agents", "value": active_agents},
        {"label": "Offene Positionen", "value": open_positions},
        {"label": "PnL Heute", "value": f"${today_pnl:+.2f}", "delta_color": "normal" if today_pnl >= 0 else "inverse"},
        {"label": "AI-Kosten Heute", "value": f"${today_cost:.2f}"},
        {"label": "Pending Suggestions", "value": pending_suggestions},
    ])

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
        st.subheader("Budget Status")
        config = client.get_config()
        budget = config.get("budgets", {})
        daily_limit = budget.get("daily_limit_usd", 5.0)
        monthly_limit = budget.get("monthly_total_usd", 50.0)

        monthly_cost = status.get("cost_month_usd", 0)

        daily_pct = (today_cost / daily_limit * 100) if daily_limit > 0 else 0
        monthly_pct = (monthly_cost / monthly_limit * 100) if monthly_limit > 0 else 0

        st.progress(min(daily_pct / 100, 1.0), text=f"Tagesbudget: ${today_cost:.2f} / ${daily_limit:.2f}")
        st.progress(min(monthly_pct / 100, 1.0), text=f"Monatsbudget: ${monthly_cost:.2f} / ${monthly_limit:.2f}")

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
