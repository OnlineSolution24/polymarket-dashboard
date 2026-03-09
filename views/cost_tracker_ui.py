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
    """Show provider + agent + hourly breakdown from bot internal tracking."""
    client = get_bot_client()
    costs = client.get_costs(days=30)
    config = client.get_config()
    budget = config.get("budgets", {})

    tab_hourly, tab_provider, tab_agent, tab_recent = st.tabs(
        ["Stuendlich", "Nach Modell", "Nach Agent", "Letzte Aufrufe"]
    )

    with tab_hourly:
        hourly = costs.get("hourly", [])
        if hourly:
            _render_hourly_chart(hourly)
        else:
            st.info("Keine stuendlichen Daten in den letzten 24h.")

    with tab_provider:
        today_by_provider = costs.get("today_by_provider", [])
        if today_by_provider:
            from components.charts import cost_pie_chart
            costs_dict = {c["provider"]: round(c["total"], 4) for c in today_by_provider}
            st.plotly_chart(cost_pie_chart(costs_dict), use_container_width=True)
            for cost in today_by_provider:
                tokens_total = (cost.get("tokens_in") or 0) + (cost.get("tokens_out") or 0)
                st.markdown(
                    f"**{cost['provider']}**: ${cost['total']:.4f} "
                    f"({cost.get('tokens_in') or 0} in, {cost.get('tokens_out') or 0} out, "
                    f"{tokens_total} total)"
                )
        else:
            st.info("Heute noch keine Modell-Daten vom Bot.")

    with tab_agent:
        today_by_agent = costs.get("today_by_agent", [])
        if today_by_agent:
            per_agent_limit = budget.get("per_agent_daily_usd", 1.0)
            for ac in today_by_agent:
                pct = min(ac["total"] / per_agent_limit, 1.0) if per_agent_limit > 0 else 0
                st.progress(pct, text=f"{ac['agent_id']}: ${ac['total']:.4f} / ${per_agent_limit:.2f}")
        else:
            st.info("Keine Agent-spezifischen Kosten heute.")

    with tab_recent:
        entries = costs.get("entries", [])
        if entries:
            for entry in entries[:30]:
                ts = (entry.get("created_at") or "")[:16]
                agent = entry.get("agent_id", "system") or "system"
                model = entry.get("provider", "?")
                cost = entry.get("cost_usd", 0)
                tokens_in = entry.get("tokens_in", 0)
                tokens_out = entry.get("tokens_out", 0)
                st.caption(
                    f"`{ts}` **{model}** — ${cost:.4f} "
                    f"({tokens_in}+{tokens_out} tok) — {agent}"
                )
        else:
            st.info("Keine Eintraege vorhanden.")


def _render_hourly_chart(hourly: list):
    """Stacked bar chart of costs per hour, colored by model."""
    from collections import defaultdict

    # Group by hour
    hours_data = defaultdict(lambda: defaultdict(float))
    all_models = set()
    for row in hourly:
        hour = row.get("hour", "?")
        model = row.get("provider", "?")
        hours_data[hour][model] += row.get("total", 0)
        all_models.add(model)

    if not hours_data:
        st.info("Keine Daten.")
        return

    hours_sorted = sorted(hours_data.keys())
    model_colors = {
        "claude-sonnet": COLORS.get("blue", "#448AFF"),
        "gemini-flash": COLORS.get("green", "#00D4AA"),
        "haiku": COLORS.get("orange", "#FFB74D"),
    }

    fig = go.Figure()
    for model in sorted(all_models):
        values = [hours_data[h].get(model, 0) for h in hours_sorted]
        color = model_colors.get(model, COLORS.get("purple", "#AB47BC"))
        fig.add_trace(go.Bar(
            x=[h[-5:] for h in hours_sorted],  # Show only HH:00
            y=values,
            name=model,
            marker_color=color,
            text=[f"${v:.4f}" if v > 0 else "" for v in values],
            textposition="outside",
        ))

    layout = {**CHART_LAYOUT, "height": 400}
    fig.update_layout(
        **layout,
        title="Kosten pro Stunde (letzte 24h)",
        yaxis_title="USD",
        barmode="stack",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="center", x=0.5),
    )
    st.plotly_chart(fig, use_container_width=True)

    # Summary table
    total_24h = sum(row.get("total", 0) for row in hourly)
    total_calls = sum(row.get("calls", 0) for row in hourly)
    col1, col2, col3 = st.columns(3)
    col1.metric("Kosten 24h", f"${total_24h:.4f}")
    col2.metric("API-Aufrufe 24h", total_calls)
    col3.metric("Durchschnitt/Aufruf", f"${total_24h / max(total_calls, 1):.4f}")
