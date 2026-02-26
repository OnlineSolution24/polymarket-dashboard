"""
Reusable Plotly chart builders for the dashboard.
"""

import plotly.graph_objects as go
import plotly.express as px
import pandas as pd


CHART_LAYOUT = dict(
    template="plotly_dark",
    margin=dict(l=40, r=20, t=40, b=40),
    height=400,
    font=dict(size=12, color="#C8D0DC", family="Inter, sans-serif"),
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(26,31,46,0.6)",
    title_font=dict(color="#E8ECF1", size=16),
    legend=dict(bgcolor="rgba(0,0,0,0)", font=dict(color="#8892A4")),
    xaxis=dict(gridcolor="rgba(0,212,170,0.06)", zerolinecolor="rgba(0,212,170,0.1)"),
    yaxis=dict(gridcolor="rgba(0,212,170,0.06)", zerolinecolor="rgba(0,212,170,0.1)"),
)

# Color palette
COLORS = {
    "green": "#00D4AA",
    "red": "#FF5252",
    "blue": "#448AFF",
    "orange": "#FFB74D",
    "purple": "#B388FF",
    "cyan": "#18FFFF",
    "teal": "#00BFA5",
    "pink": "#FF80AB",
}

COLOR_SEQUENCE = ["#00D4AA", "#448AFF", "#FFB74D", "#B388FF", "#FF5252", "#18FFFF", "#00BFA5", "#FF80AB"]


def price_bar_chart(markets: list[dict], max_items: int = 20) -> go.Figure:
    """Horizontal bar chart of YES/NO prices for markets."""
    df = pd.DataFrame(markets[:max_items])
    if df.empty:
        return _empty_chart("Keine Marktdaten verfügbar")

    # Truncate long questions
    df["short_q"] = df["question"].str[:60] + "..."

    fig = go.Figure()
    fig.add_trace(go.Bar(
        y=df["short_q"], x=df["yes_price"], name="YES",
        orientation="h", marker_color=COLORS["green"],
    ))
    fig.add_trace(go.Bar(
        y=df["short_q"], x=df["no_price"], name="NO",
        orientation="h", marker_color=COLORS["red"],
    ))
    fig.update_layout(**CHART_LAYOUT, barmode="group", title="YES/NO Preise", yaxis=dict(autorange="reversed"))
    return fig


def volume_chart(markets: list[dict], max_items: int = 15) -> go.Figure:
    """Bar chart of market volumes."""
    df = pd.DataFrame(markets[:max_items])
    if df.empty:
        return _empty_chart("Keine Volumendaten verfügbar")

    df["short_q"] = df["question"].str[:50] + "..."
    df = df.sort_values("volume", ascending=True)

    fig = go.Figure(go.Bar(
        y=df["short_q"], x=df["volume"], orientation="h",
        marker_color=COLORS["blue"],
    ))
    fig.update_layout(**CHART_LAYOUT, title="Markt-Volumen (USD)")
    return fig


def cost_timeline(costs: list[dict]) -> go.Figure:
    """Line chart of daily API costs."""
    if not costs:
        return _empty_chart("Keine Kostendaten verfügbar")

    df = pd.DataFrame(costs)
    fig = px.line(df, x="date", y="total_cost", color="provider", title="API-Kosten (täglich)",
                  color_discrete_sequence=COLOR_SEQUENCE)
    fig.update_layout(**CHART_LAYOUT)
    return fig


def cost_pie_chart(costs_by_provider: dict) -> go.Figure:
    """Pie chart of costs by provider."""
    if not costs_by_provider:
        return _empty_chart("Keine Kostendaten verfügbar")

    fig = go.Figure(go.Pie(
        labels=list(costs_by_provider.keys()),
        values=list(costs_by_provider.values()),
        hole=0.45,
        marker=dict(colors=COLOR_SEQUENCE),
    ))
    fig.update_layout(**CHART_LAYOUT, title="Kosten nach Provider")
    return fig


def pnl_chart(trades: list[dict]) -> go.Figure:
    """Cumulative PnL line chart."""
    if not trades:
        return _empty_chart("Keine Trade-Daten verfügbar")

    df = pd.DataFrame(trades)
    df["cumulative_pnl"] = df["pnl"].cumsum()
    fig = px.line(df, x="executed_at", y="cumulative_pnl", title="Kumulierter PnL",
                  color_discrete_sequence=[COLORS["green"]])
    fig.update_layout(**CHART_LAYOUT)
    return fig


def _empty_chart(message: str) -> go.Figure:
    """Return an empty chart with a message."""
    fig = go.Figure()
    fig.add_annotation(text=message, xref="paper", yref="paper", x=0.5, y=0.5, showarrow=False, font=dict(size=16, color="#5A6478"))
    fig.update_layout(**CHART_LAYOUT)
    return fig
