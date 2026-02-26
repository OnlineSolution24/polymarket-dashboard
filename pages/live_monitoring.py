"""
Tab 3: Live Monitoring Dashboard
Trending markets, prices, volume, sentiment scores, edge, and agent logs.
"""

import streamlit as st
from datetime import datetime

from db import engine
from components.charts import price_bar_chart, volume_chart
from components.tables import market_table
from components.log_viewer import render_log_viewer


def render():
    st.header("Live Monitoring")

    # --- Refresh Controls ---
    col1, col2, col3 = st.columns([1, 1, 1])
    with col1:
        if st.button("Märkte aktualisieren", type="primary"):
            _refresh_markets()
    with col2:
        if st.button("Sentiment aktualisieren"):
            _refresh_sentiment()
    with col3:
        last_update = engine.query_one("SELECT MAX(last_updated) as ts FROM markets")
        ts = last_update["ts"][:16] if last_update and last_update["ts"] else "Nie"
        st.caption(f"Letztes Update: {ts}")

    st.divider()

    # --- Markets Data ---
    markets = engine.query("SELECT * FROM markets ORDER BY volume DESC LIMIT 50")

    if not markets:
        st.info("Keine Marktdaten vorhanden. Drücke 'Märkte aktualisieren'.")
        return

    # --- KPI Metrics ---
    total_markets = len(markets)
    avg_volume = sum(m["volume"] for m in markets) / total_markets if total_markets else 0
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

    log_filter = st.selectbox("Agent filtern", ["Alle"] + _get_agent_ids())
    agent_filter = None if log_filter == "Alle" else log_filter
    render_log_viewer(agent_id=agent_filter, limit=50)


def _render_sentiment_tab(markets: list[dict]):
    """Render sentiment analysis view."""
    st.subheader("Sentiment Scores")

    sentiment_markets = [m for m in markets if m.get("sentiment_score") is not None]
    if not sentiment_markets:
        st.info("Noch keine Sentiment-Daten. Drücke 'Sentiment aktualisieren'.")
        return

    import plotly.graph_objects as go

    sorted_m = sorted(sentiment_markets, key=lambda x: x["sentiment_score"] or 0, reverse=True)
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

    sorted_m = sorted(edge_markets, key=lambda x: abs(x["calculated_edge"] or 0), reverse=True)[:15]
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


def _refresh_markets():
    """Refresh market data from Polymarket API."""
    try:
        from services.polymarket_client import PolymarketService
        from config import AppConfig

        config = AppConfig.from_env()
        service = PolymarketService(config)
        markets = service.fetch_markets()

        for market in markets:
            engine.execute(
                """INSERT OR REPLACE INTO markets
                   (id, question, slug, yes_price, no_price, volume, liquidity, end_date, category, last_updated)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (market["id"], market["question"], market.get("slug", ""),
                 market.get("yes_price", 0), market.get("no_price", 0),
                 market.get("volume", 0), market.get("liquidity", 0),
                 market.get("end_date"), market.get("category", ""),
                 datetime.utcnow().isoformat()),
            )

        st.success(f"{len(markets)} Märkte aktualisiert!")
    except Exception as e:
        st.error(f"Fehler: {e}")


def _refresh_sentiment():
    """Update sentiment scores for all tracked markets."""
    try:
        from services.news_sentiment import NewsSentimentService
        from config import AppConfig

        config = AppConfig.from_env()
        service = NewsSentimentService(config)

        markets = engine.query("SELECT id, question FROM markets ORDER BY volume DESC LIMIT 20")
        updated = 0

        progress = st.progress(0, text="Sentiment-Analyse läuft...")
        for i, market in enumerate(markets):
            # Extract key terms from question
            query = market["question"][:80]
            result = service.get_sentiment(query, days_back=3)

            if result["article_count"] > 0:
                engine.execute(
                    "UPDATE markets SET sentiment_score = ?, last_updated = ? WHERE id = ?",
                    (result["score"], datetime.utcnow().isoformat(), market["id"]),
                )
                updated += 1

            progress.progress((i + 1) / len(markets), text=f"Analysiere {i+1}/{len(markets)}...")

        progress.empty()
        st.success(f"Sentiment für {updated} Märkte aktualisiert!")

    except Exception as e:
        st.error(f"Sentiment-Fehler: {e}")


def _get_agent_ids() -> list[str]:
    """Get list of agent IDs for log filtering."""
    rows = engine.query("SELECT DISTINCT agent_id FROM agent_logs ORDER BY agent_id")
    return [r["agent_id"] for r in rows if r.get("agent_id")]
