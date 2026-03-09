"""
Cost Tracker — Real OpenRouter API cost monitoring.
Shows daily/weekly/monthly costs, remaining budget, and usage charts.
"""

import os
import streamlit as st
import plotly.graph_objects as go

from services.bot_api_client import get_bot_client
from services.openrouter_costs import get_openrouter_costs
from components.charts import CHART_LAYOUT, COLORS, COLOR_SEQUENCE, _empty_chart
from components.status_cards import kpi_row

OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY", "")


def render():
    st.header("Cost Tracker")

    # --- Fetch real OpenRouter data ---
    or_data = get_openrouter_costs(OPENROUTER_API_KEY) or {}

    usage_daily = or_data.get("usage_daily", 0)
    usage_weekly = or_data.get("usage_weekly", 0)
    usage_monthly = or_data.get("usage_monthly", 0)
    usage_total = or_data.get("usage", 0)
    limit = or_data.get("limit")
    limit_remaining = or_data.get("limit_remaining")

    # --- KPI Row ---
    kpi_items = [
        {"label": "Heute", "value": f"${usage_daily:.2f}"},
        {"label": "Diese Woche", "value": f"${usage_weekly:.2f}"},
        {"label": "Dieser Monat", "value": f"${usage_monthly:.2f}"},
        {"label": "Gesamt", "value": f"${usage_total:.2f}"},
    ]
    if limit_remaining is not None:
        kpi_items.append({"label": "Budget Rest", "value": f"${limit_remaining:.2f}"})

    kpi_row(kpi_items)

    st.divider()

    # --- Budget Progress ---
    if limit and limit > 0:
        st.subheader("Budget-Auslastung")
        col1, col2 = st.columns(2)

        with col1:
            pct = min(usage_total / limit, 1.0)
            color = "normal" if pct < 0.8 else "off"
            st.progress(pct, text=f"Gesamt: ${usage_total:.2f} / ${limit:.2f} ({pct*100:.1f}%)")
            if pct >= 0.9:
                st.error("Budget fast erschoepft!")
            elif pct >= 0.7:
                st.warning("Budget ueber 70% verbraucht")

        with col2:
            st.metric("Verbleibendes Budget", f"${limit_remaining:.2f}" if limit_remaining else "Unbegrenzt")
            if limit_remaining and limit_remaining < 5:
                st.error("Weniger als $5 verbleibend!")

    st.divider()

    # --- Budget Limits Editor ---
    _render_budget_editor(usage_daily)

    st.divider()

    # --- Cost Breakdown Chart ---
    st.subheader("Kosten-Uebersicht")

    _render_cost_bars(usage_daily, usage_weekly, usage_monthly, usage_total)

    st.divider()

    # --- Bot internal cost data (provider + agent breakdown) ---
    st.subheader("Detaillierte Aufschluesselung (Bot-intern)")
    _render_bot_costs()


def _render_budget_editor(usage_daily: float):
    """Allow user to view and edit daily/monthly budget limits."""
    st.subheader("Budget-Limits anpassen")

    client = get_bot_client()
    config = client.get_config()
    budget = config.get("budgets", {})

    current_daily = budget.get("daily_limit_usd", 5.0)
    current_monthly = budget.get("monthly_total_usd", 50.0)
    current_per_agent = budget.get("per_agent_daily_usd", 1.0)

    col1, col2, col3 = st.columns(3)

    with col1:
        daily_pct = min(usage_daily / current_daily, 1.0) if current_daily > 0 else 0
        st.progress(daily_pct, text=f"Heute: ${usage_daily:.2f} / ${current_daily:.2f}")

    with col2:
        st.metric("Tages-Limit", f"${current_daily:.2f}")

    with col3:
        st.metric("Monats-Limit", f"${current_monthly:.2f}")

    with st.expander("Limits bearbeiten"):
        new_daily = st.number_input(
            "Tages-Limit ($)", min_value=0.5, max_value=100.0,
            value=float(current_daily), step=0.5,
        )
        new_monthly = st.number_input(
            "Monats-Limit ($)", min_value=5.0, max_value=1000.0,
            value=float(current_monthly), step=5.0,
        )
        new_per_agent = st.number_input(
            "Pro-Agent Tages-Limit ($)", min_value=0.1, max_value=20.0,
            value=float(current_per_agent), step=0.1,
        )

        changed = (
            new_daily != current_daily
            or new_monthly != current_monthly
            or new_per_agent != current_per_agent
        )

        if changed and st.button("Limits speichern", type="primary"):
            config["budgets"] = {
                **budget,
                "daily_limit_usd": new_daily,
                "monthly_total_usd": new_monthly,
                "per_agent_daily_usd": new_per_agent,
            }
            result = client.save_config(config)
            if result and result.get("ok"):
                st.success(f"Gespeichert: Tages ${new_daily:.2f} / Monat ${new_monthly:.2f} / Agent ${new_per_agent:.2f}")
                st.rerun()
            else:
                st.error("Fehler beim Speichern.")


def _render_cost_bars(daily: float, weekly: float, monthly: float, total: float):
    """Bar chart comparing daily/weekly/monthly/total costs."""
    periods = ["Heute", "Woche", "Monat", "Gesamt"]
    values = [daily, weekly, monthly, total]
    colors = [COLORS["green"], COLORS["blue"], COLORS["orange"], COLORS["purple"]]

    fig = go.Figure(go.Bar(
        x=periods,
        y=values,
        marker_color=colors,
        text=[f"${v:.2f}" if v < 1 else f"${v:.2f}" for v in values],
        textposition="outside",
    ))
    layout = {**CHART_LAYOUT, "height": 350}
    fig.update_layout(
        **layout,
        title="OpenRouter Kosten",
        yaxis_title="USD",
        showlegend=False,
    )
    st.plotly_chart(fig, use_container_width=True)


def _render_bot_costs():
    """Show provider + agent breakdown from bot internal tracking."""
    client = get_bot_client()
    costs = client.get_costs(days=30)
    config = client.get_config()
    budget = config.get("budgets", {})

    tab_provider, tab_agent, tab_recent = st.tabs(["Nach Provider", "Nach Agent", "Letzte Aufrufe"])

    with tab_provider:
        today_by_provider = costs.get("today_by_provider", [])
        if today_by_provider:
            from components.charts import cost_pie_chart
            costs_dict = {c["provider"]: round(c["total"], 4) for c in today_by_provider}
            st.plotly_chart(cost_pie_chart(costs_dict), use_container_width=True)
            for cost in today_by_provider:
                st.markdown(
                    f"**{cost['provider']}**: ${cost['total']:.2f} "
                    f"({cost.get('tokens_in') or 0} in, {cost.get('tokens_out') or 0} out)"
                )
        else:
            st.info("Heute noch keine Provider-Daten vom Bot.")

    with tab_agent:
        today_by_agent = costs.get("today_by_agent", [])
        if today_by_agent:
            per_agent_limit = budget.get("per_agent_daily_usd", 1.0)
            for ac in today_by_agent:
                pct = min(ac["total"] / per_agent_limit, 1.0) if per_agent_limit > 0 else 0
                st.progress(pct, text=f"{ac['agent_id']}: ${ac['total']:.2f} / ${per_agent_limit:.2f}")
        else:
            st.info("Keine Agent-spezifischen Kosten heute.")

    with tab_recent:
        entries = costs.get("entries", [])
        if entries:
            for entry in entries[:20]:
                ts = (entry.get("created_at") or "")[:16]
                agent = entry.get("agent_id", "system") or "system"
                st.caption(
                    f"`{ts}` **{entry.get('provider', '?')}** — ${entry.get('cost_usd', 0):.2f} "
                    f"(Agent: {agent})"
                )
        else:
            st.info("Keine Eintraege vorhanden.")
