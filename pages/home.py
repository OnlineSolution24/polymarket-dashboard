"""
Home Page - KPI Dashboard Overview.
Shows the 5 most important metrics at a glance.
"""

import streamlit as st
from datetime import datetime, date

from db import engine
from components.status_cards import kpi_row, status_badge


def render():
    st.header("Dashboard Overview")

    # --- KPI Row 1: Key Numbers ---
    today = date.today().isoformat()

    # Active agents
    agents_row = engine.query_one("SELECT COUNT(*) as cnt FROM agents WHERE status = 'active'")
    active_agents = agents_row["cnt"] if agents_row else 0

    # Open positions (trades with status 'executed' and result 'open')
    open_row = engine.query_one("SELECT COUNT(*) as cnt FROM trades WHERE status = 'executed' AND (result = 'open' OR result IS NULL)")
    open_positions = open_row["cnt"] if open_row else 0

    # Today's PnL
    pnl_row = engine.query_one(
        "SELECT COALESCE(SUM(pnl), 0) as total_pnl FROM trades WHERE date(executed_at) = ?", (today,)
    )
    today_pnl = pnl_row["total_pnl"] if pnl_row else 0

    # Today's AI costs
    cost_row = engine.query_one(
        "SELECT COALESCE(SUM(cost_usd), 0) as total_cost FROM api_costs WHERE date(created_at) = ?", (today,)
    )
    today_cost = cost_row["total_cost"] if cost_row else 0

    # Pending suggestions
    sugg_row = engine.query_one("SELECT COUNT(*) as cnt FROM suggestions WHERE status = 'pending'")
    pending_suggestions = sugg_row["cnt"] if sugg_row else 0

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
        cb = engine.query_one("SELECT * FROM circuit_breaker WHERE id = 1")
        if cb and cb.get("paused_until"):
            paused_until = cb["paused_until"]
            if paused_until and datetime.fromisoformat(paused_until) > datetime.utcnow():
                status_badge("Trading", "paused")
                st.warning(f"Pausiert bis: {paused_until}")
            else:
                status_badge("Trading", "active")
        else:
            status_badge("Trading", "active")

        losses = cb["consecutive_losses"] if cb else 0
        st.caption(f"Verluste in Folge: {losses}/3")

    with col2:
        st.subheader("Budget Status")
        from config import get_budget_config
        budget = get_budget_config()
        daily_limit = budget.get("daily_limit_usd", 5.0)
        monthly_limit = budget.get("monthly_total_usd", 50.0)

        # Monthly costs
        monthly_row = engine.query_one(
            "SELECT COALESCE(SUM(cost_usd), 0) as total FROM api_costs WHERE strftime('%Y-%m', created_at) = strftime('%Y-%m', 'now')"
        )
        monthly_cost = monthly_row["total"] if monthly_row else 0

        daily_pct = (today_cost / daily_limit * 100) if daily_limit > 0 else 0
        monthly_pct = (monthly_cost / monthly_limit * 100) if monthly_limit > 0 else 0

        st.progress(min(daily_pct / 100, 1.0), text=f"Tagesbudget: ${today_cost:.2f} / ${daily_limit:.2f}")
        st.progress(min(monthly_pct / 100, 1.0), text=f"Monatsbudget: ${monthly_cost:.2f} / ${monthly_limit:.2f}")

    st.divider()

    # --- Recent Activity ---
    st.subheader("Letzte Aktivität")
    recent_logs = engine.query(
        "SELECT agent_id, level, message, created_at FROM agent_logs ORDER BY created_at DESC LIMIT 10"
    )
    if recent_logs:
        for log in recent_logs:
            level_icon = {"info": "ℹ️", "warn": "⚠️", "error": "❌", "debug": "🔍"}.get(log["level"], "📝")
            ts = log["created_at"][:16] if log["created_at"] else ""
            st.caption(f"{level_icon} `{ts}` **{log['agent_id']}**: {log['message']}")
    else:
        st.info("Noch keine Agent-Aktivitäten. Starte die Agents im Agent Manager.")
