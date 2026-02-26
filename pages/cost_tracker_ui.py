"""
Tab 6: Cost Tracker
Real-time API cost tracking with budget warnings and provider breakdown.
"""

import streamlit as st
from datetime import date, datetime

from db import engine
from config import get_budget_config
from components.charts import cost_pie_chart
from components.status_cards import kpi_row


def render():
    st.header("Cost Tracker")

    budget = get_budget_config()
    today = date.today().isoformat()

    # --- Today's Costs ---
    today_costs = engine.query(
        "SELECT provider, SUM(cost_usd) as total, SUM(tokens_in) as tokens_in, SUM(tokens_out) as tokens_out "
        "FROM api_costs WHERE date(created_at) = ? GROUP BY provider",
        (today,),
    )
    today_total = sum(c["total"] for c in today_costs) if today_costs else 0

    # Monthly costs
    month_costs = engine.query(
        "SELECT provider, SUM(cost_usd) as total "
        "FROM api_costs WHERE strftime('%Y-%m', created_at) = strftime('%Y-%m', 'now') GROUP BY provider"
    )
    month_total = sum(c["total"] for c in month_costs) if month_costs else 0

    # --- KPI Row ---
    daily_limit = budget.get("daily_limit_usd", 5.0)
    monthly_limit = budget.get("monthly_total_usd", 50.0)
    daily_remaining = max(0, daily_limit - today_total)
    monthly_remaining = max(0, monthly_limit - month_total)

    kpi_row([
        {"label": "Kosten Heute", "value": f"${today_total:.2f}"},
        {"label": "Tagesbudget Rest", "value": f"${daily_remaining:.2f}",
         "delta_color": "normal" if daily_remaining > daily_limit * 0.2 else "inverse"},
        {"label": "Kosten Monat", "value": f"${month_total:.2f}"},
        {"label": "Monatsbudget Rest", "value": f"${monthly_remaining:.2f}",
         "delta_color": "normal" if monthly_remaining > monthly_limit * 0.2 else "inverse"},
    ])

    # --- Budget Bars ---
    st.divider()
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Tagesbudget")
        pct = min(today_total / daily_limit, 1.0) if daily_limit > 0 else 0
        st.progress(pct, text=f"${today_total:.2f} / ${daily_limit:.2f} ({pct*100:.0f}%)")
        if pct >= 0.8:
            st.warning("Tagesbudget fast erschöpft!")

    with col2:
        st.subheader("Monatsbudget")
        pct = min(month_total / monthly_limit, 1.0) if monthly_limit > 0 else 0
        st.progress(pct, text=f"${month_total:.2f} / ${monthly_limit:.2f} ({pct*100:.0f}%)")
        if pct >= 0.8:
            st.warning("Monatsbudget fast erschöpft!")

    st.divider()

    # --- Provider Breakdown ---
    st.subheader("Kosten nach Provider")

    tab_today, tab_month = st.tabs(["Heute", "Monat"])

    with tab_today:
        if today_costs:
            costs_dict = {c["provider"]: round(c["total"], 4) for c in today_costs}
            st.plotly_chart(cost_pie_chart(costs_dict), use_container_width=True)

            for cost in today_costs:
                st.markdown(
                    f"**{cost['provider']}**: ${cost['total']:.4f} "
                    f"({cost['tokens_in'] or 0} tokens in, {cost['tokens_out'] or 0} tokens out)"
                )
        else:
            st.info("Heute noch keine API-Kosten angefallen.")

    with tab_month:
        if month_costs:
            costs_dict = {c["provider"]: round(c["total"], 4) for c in month_costs}
            st.plotly_chart(cost_pie_chart(costs_dict), use_container_width=True)
        else:
            st.info("Diesen Monat noch keine API-Kosten angefallen.")

    st.divider()

    # --- Agent Cost Breakdown ---
    st.subheader("Kosten pro Agent (Heute)")
    agent_costs = engine.query(
        "SELECT agent_id, SUM(cost_usd) as total FROM api_costs "
        "WHERE date(created_at) = ? AND agent_id IS NOT NULL GROUP BY agent_id ORDER BY total DESC",
        (today,),
    )
    if agent_costs:
        per_agent_limit = budget.get("per_agent_daily_usd", 1.0)
        for ac in agent_costs:
            pct = min(ac["total"] / per_agent_limit, 1.0) if per_agent_limit > 0 else 0
            st.progress(pct, text=f"{ac['agent_id']}: ${ac['total']:.4f} / ${per_agent_limit:.2f}")
    else:
        st.info("Keine Agent-spezifischen Kosten heute.")

    st.divider()

    # --- Recent Cost Entries ---
    st.subheader("Letzte API-Aufrufe")
    recent = engine.query(
        "SELECT * FROM api_costs ORDER BY created_at DESC LIMIT 20"
    )
    if recent:
        for entry in recent:
            ts = entry["created_at"][:16] if entry["created_at"] else ""
            agent = entry.get("agent_id", "system") or "system"
            st.caption(
                f"`{ts}` **{entry['provider']}** — ${entry['cost_usd']:.4f} "
                f"(Agent: {agent}, Endpoint: {entry.get('endpoint', 'N/A')})"
            )
