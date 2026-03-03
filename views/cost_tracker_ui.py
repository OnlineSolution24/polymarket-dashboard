"""
Cost Tracker — API cost monitoring via Bot REST API.
Shows daily/monthly costs, provider breakdown, and agent costs.
"""

import streamlit as st

from services.bot_api_client import get_bot_client
from components.charts import cost_pie_chart
from components.status_cards import kpi_row


def render():
    st.header("Cost Tracker")

    client = get_bot_client()
    costs = client.get_costs(days=30)
    config = client.get_config()
    budget = config.get("budgets", {})

    today_total = costs.get("daily_total", 0)
    month_total = costs.get("monthly_total", 0)

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
        today_by_provider = costs.get("today_by_provider", [])
        if today_by_provider:
            costs_dict = {c["provider"]: round(c["total"], 4) for c in today_by_provider}
            st.plotly_chart(cost_pie_chart(costs_dict), use_container_width=True)

            for cost in today_by_provider:
                st.markdown(
                    f"**{cost['provider']}**: ${cost['total']:.4f} "
                    f"({cost.get('tokens_in') or 0} tokens in, {cost.get('tokens_out') or 0} tokens out)"
                )
        else:
            st.info("Heute noch keine API-Kosten angefallen.")

    with tab_month:
        month_by_provider = costs.get("month_by_provider", [])
        if month_by_provider:
            costs_dict = {c["provider"]: round(c["total"], 4) for c in month_by_provider}
            st.plotly_chart(cost_pie_chart(costs_dict), use_container_width=True)
        else:
            st.info("Diesen Monat noch keine API-Kosten angefallen.")

    st.divider()

    # --- Agent Cost Breakdown ---
    st.subheader("Kosten pro Agent (Heute)")
    today_by_agent = costs.get("today_by_agent", [])
    if today_by_agent:
        per_agent_limit = budget.get("per_agent_daily_usd", 1.0)
        for ac in today_by_agent:
            pct = min(ac["total"] / per_agent_limit, 1.0) if per_agent_limit > 0 else 0
            st.progress(pct, text=f"{ac['agent_id']}: ${ac['total']:.4f} / ${per_agent_limit:.2f}")
    else:
        st.info("Keine Agent-spezifischen Kosten heute.")

    st.divider()

    # --- Recent Cost Entries ---
    st.subheader("Letzte API-Aufrufe")
    entries = costs.get("entries", [])
    if entries:
        for entry in entries[:20]:
            ts = (entry.get("created_at") or "")[:16]
            agent = entry.get("agent_id", "system") or "system"
            st.caption(
                f"`{ts}` **{entry.get('provider', '?')}** — ${entry.get('cost_usd', 0):.4f} "
                f"(Agent: {agent}, Endpoint: {entry.get('endpoint', 'N/A')})"
            )
