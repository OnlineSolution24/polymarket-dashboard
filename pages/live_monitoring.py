"""
Live Monitoring Dashboard.
Trending markets, prices, volume, sentiment scores, edge, and agent logs.
All data loaded from Bot REST API.
"""

import streamlit as st

from services.bot_api_client import get_bot_client
from components.charts import price_bar_chart, volume_chart
from components.tables import market_table
from components.log_viewer import render_log_viewer


def render():
    st.header("Live Monitoring")

    client = get_bot_client()

    # --- Markets Data ---
    markets = client.get_markets(limit=50)

    if not markets:
        st.info("Keine Marktdaten vorhanden. Der Bot aktualisiert Märkte automatisch.")
        return

    # --- KPI Metrics ---
    total_markets = len(markets)
    avg_volume = sum(m.get("volume", 0) for m in markets) / total_markets if total_markets else 0
    high_edge = [m for m in markets if (m.get("calculated_edge") or 0) > 0.05]
    has_sentiment = [m for m in markets if m.get("sentiment_score") is not None]

    kc = st.columns(4)
    with kc[0]:
        st.metric("Märkte", total_markets)
    with kc[1]:
        st.metric("Ø Volumen", f"${avg_volume:,.0f}")
    with kc[2]:
        st.metric("High-Edge", len(high_edge))
    with kc[3]:
        st.metric("Mit Sentiment", len(has_sentiment))

    # --- Tabs ---
    tab_chart, tab_table, tab_sentiment, tab_edge = st.tabs(["Charts", "Tabelle", "Sentiment", "Edge-Analyse"])

    with tab_chart:
        st.plotly_chart(price_bar_chart(markets), use_container_width=True)
        st.plotly_chart(volume_chart(markets), use_container_width=True)

    with tab_table:
        market_table(markets)

    with tab_sentiment:
        _render_sentiment_tab(markets)

    with tab_edge:
        _render_edge_tab(markets)

    st.divider()

    # --- Live Agent Logs ---
    st.subheader("Agent Logs (Live)")

    agents = client.get_agents()
    agent_ids = [a.get("id", "") for a in agents if a.get("id")]
    log_filter = st.selectbox("Agent filtern", ["Alle"] + agent_ids)
    agent_filter = None if log_filter == "Alle" else log_filter

    logs = client.get_logs(agent_id=agent_filter, limit=50)
    render_log_viewer(logs=logs)


def _render_sentiment_tab(markets: list[dict]):
    """Render sentiment analysis view."""
    st.subheader("Sentiment Scores")

    sentiment_markets = [m for m in markets if m.get("sentiment_score") is not None]
    if not sentiment_markets:
        st.info("Noch keine Sentiment-Daten. Der Bot berechnet Sentiments automatisch.")
        return

    import plotly.graph_objects as go

    sorted_m = sorted(sentiment_markets, key=lambda x: x.get("sentiment_score") or 0, reverse=True)
    questions = [m["question"][:50] + "..." for m in sorted_m[:20]]
    scores = [m["sentiment_score"] for m in sorted_m[:20]]
    colors = ["#2ca02c" if s > 0 else "#d62728" for s in scores]

    fig = go.Figure(go.Bar(
        y=questions, x=scores, orientation="h", marker_color=colors,
    ))
    fig.update_layout(
        template="plotly_dark", title="Sentiment Scores (-1.0 bis +1.0)",
        margin=dict(l=40, r=20, t=40, b=40), height=500,
        yaxis=dict(autorange="reversed"),
    )
    st.plotly_chart(fig, use_container_width=True)

    # Positive / Negative breakdown
    pos = [m for m in sentiment_markets if (m.get("sentiment_score") or 0) > 0.1]
    neg = [m for m in sentiment_markets if (m.get("sentiment_score") or 0) < -0.1]
    neutral = [m for m in sentiment_markets if abs(m.get("sentiment_score") or 0) <= 0.1]

    c1, c2, c3 = st.columns(3)
    with c1:
        st.metric("Positiv", len(pos))
    with c2:
        st.metric("Neutral", len(neutral))
    with c3:
        st.metric("Negativ", len(neg))


def _render_edge_tab(markets: list[dict]):
    """Render edge analysis view."""
    st.subheader("Edge-Analyse")

    edge_markets = [m for m in markets if m.get("calculated_edge") is not None]
    if not edge_markets:
        st.info("Noch keine Edge-Berechnungen. Der Analyst Agent berechnet Edges automatisch.")
        return

    import plotly.graph_objects as go

    sorted_m = sorted(edge_markets, key=lambda x: abs(x.get("calculated_edge") or 0), reverse=True)[:15]
    questions = [m["question"][:50] + "..." for m in sorted_m]
    edges = [m["calculated_edge"] for m in sorted_m]
    colors = ["#2ca02c" if e > 0 else "#d62728" for e in edges]

    fig = go.Figure(go.Bar(
        y=questions, x=edges, orientation="h", marker_color=colors,
    ))
    fig.update_layout(
        template="plotly_dark", title="Berechneter Edge (positiv = Kaufgelegenheit)",
        margin=dict(l=40, r=20, t=40, b=40), height=450,
        yaxis=dict(autorange="reversed"),
    )
    st.plotly_chart(fig, use_container_width=True)

    # High-edge opportunities
    high_pos = [m for m in edge_markets if (m.get("calculated_edge") or 0) > 0.05]
    if high_pos:
        st.success(f"{len(high_pos)} Märkte mit positivem Edge > 5%:")
        for m in sorted(high_pos, key=lambda x: x["calculated_edge"], reverse=True)[:5]:
            st.markdown(
                f"- **{m['question'][:60]}** — Edge: {m['calculated_edge']:+.2f} "
                f"(YES: {m['yes_price']:.0%}, Vol: ${m['volume']:,.0f})"
            )
