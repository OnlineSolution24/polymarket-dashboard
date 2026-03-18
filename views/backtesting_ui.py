"""
Backtesting Module — Runs backtests on trade data from the Bot API.
Monte-Carlo, Walk-Forward, Drawdown analysis, and Historical Blockchain Backtests.
"""

import json
import streamlit as st
import numpy as np
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from pathlib import Path

from services.bot_api_client import get_bot_client

CHART_LAYOUT = dict(template="plotly_dark", margin=dict(l=40, r=20, t=40, b=40), font=dict(size=12))
RESULTS_FILE = Path("data/backtest_results/weather_backtest.json")


def render():
    st.header("Backtesting")

    tab_bot, tab_weather = st.tabs(["Bot Trades", "Historical Weather (384M Trades)"])

    with tab_bot:
        _render_bot_backtest()

    with tab_weather:
        _render_weather_backtest()


def _render_weather_backtest():
    """Render weather strategy backtest results from the 384M trade history."""

    # Check if results exist
    if not RESULTS_FILE.exists():
        st.info("Noch keine Weather-Backtest-Ergebnisse vorhanden.")
        if st.button("Weather Backtest jetzt starten", type="primary"):
            with st.spinner("Analysiere 8.900+ Weather-Markte aus 384M Trade-Datenbank..."):
                try:
                    from backtesting.weather_backtest import run_weather_backtest
                    result = run_weather_backtest()
                    st.success(f"Backtest abgeschlossen! {result.total_weather_markets} Markte analysiert.")
                    st.rerun()
                except Exception as e:
                    st.error(f"Fehler: {e}")
        return

    # Load results
    try:
        data = json.loads(RESULTS_FILE.read_text())
    except Exception as e:
        st.error(f"Fehler beim Laden: {e}")
        return

    # Header stats
    st.markdown(f"*Letzte Analyse: {data.get('timestamp', 'unbekannt')[:19]}*")

    kc = st.columns(4)
    with kc[0]:
        st.metric("Weather-Markte", f"{data.get('total_weather_markets', 0):,}")
    with kc[1]:
        st.metric("Mit Trade-Daten", f"{data.get('markets_with_trades', 0):,}")
    with kc[2]:
        st.metric("Trades analysiert", f"{data.get('total_trades_analyzed', 0):,}")
    with kc[3]:
        total_pnl = sum(s.get("total_pnl", 0) for s in data.get("strategies", []))
        color = "#00c853" if total_pnl > 0 else "#ff1744"
        st.metric("Gesamt PnL (sim.)", f"${total_pnl:+,.2f}")

    st.divider()

    # Strategy comparison
    st.subheader("Strategie-Vergleich")
    strategies = data.get("strategies", [])

    if strategies:
        # Strategy table
        strat_rows = []
        for s in strategies:
            wr = s.get("win_rate", 0)
            pnl = s.get("total_pnl", 0)
            strat_rows.append({
                "Strategie": s["name"],
                "Trades": s.get("total_trades", 0),
                "Win Rate": f"{wr:.1%}",
                "PnL": f"${pnl:+.2f}",
                "Avg Return": f"${s.get('avg_return', 0):+.2f}",
                "Max Win": f"${s.get('max_win', 0):.2f}",
                "Max Loss": f"${s.get('max_loss', 0):.2f}",
                "Sharpe": f"{s.get('sharpe', 0):.2f}",
            })

        st.dataframe(pd.DataFrame(strat_rows), use_container_width=True, hide_index=True)

        # Bar chart: PnL per strategy
        fig = go.Figure()
        names = [s["name"] for s in strategies]
        pnls = [s.get("total_pnl", 0) for s in strategies]
        colors = ["#00c853" if p > 0 else "#ff1744" for p in pnls]
        fig.add_trace(go.Bar(x=names, y=pnls, marker_color=colors))
        fig.update_layout(**CHART_LAYOUT, height=350, title="PnL pro Strategie (simuliert)")
        st.plotly_chart(fig, use_container_width=True)

        # Win rate comparison
        fig_wr = go.Figure()
        win_rates = [s.get("win_rate", 0) * 100 for s in strategies]
        fig_wr.add_trace(go.Bar(x=names, y=win_rates, marker_color="#1f77b4"))
        fig_wr.add_hline(y=50, line_dash="dash", line_color="gray", annotation_text="Break-even")
        fig_wr.update_layout(**CHART_LAYOUT, height=300, title="Win Rate (%)", yaxis_range=[0, 100])
        st.plotly_chart(fig_wr, use_container_width=True)

    st.divider()

    # Category breakdown
    st.subheader("Kategorien")
    cat_stats = data.get("category_stats", {})
    if cat_stats:
        cat_rows = []
        for cat, stats in sorted(cat_stats.items(), key=lambda x: x[1].get("total_volume", 0), reverse=True):
            cat_rows.append({
                "Kategorie": cat.replace("_", " ").title(),
                "Markte": stats.get("total_markets", 0),
                "Geschlossen": stats.get("closed_markets", 0),
                "Volume": f"${stats.get('total_volume', 0):,.0f}",
                "Avg Volume": f"${stats.get('avg_volume', 0):,.0f}",
            })
        st.dataframe(pd.DataFrame(cat_rows), use_container_width=True, hide_index=True)

    st.divider()

    # Trading patterns
    patterns = data.get("trading_patterns", [])
    if patterns:
        st.subheader(f"Trading Patterns (Top {len(patterns)} Markte)")

        # Price trend distribution
        trends = [p.get("trend", "FLAT") for p in patterns]
        up = trends.count("UP")
        down = trends.count("DOWN")
        flat = trends.count("FLAT")

        tc = st.columns(3)
        with tc[0]:
            st.metric("Trend UP", up)
        with tc[1]:
            st.metric("Trend DOWN", down)
        with tc[2]:
            st.metric("Trend FLAT", flat)

        # Pattern details
        with st.expander("Market-Details", expanded=False):
            for p in patterns[:20]:
                resolved = "YES" if p.get("resolved_yes") else "NO"
                trend_icon = {"UP": "+", "DOWN": "-", "FLAT": "="}[p.get("trend", "FLAT")]
                st.markdown(
                    f"**{p.get('question', '?')[:80]}**  \n"
                    f"Vol: ${p.get('volume', 0):,.0f} | "
                    f"Trend: {trend_icon} | "
                    f"Resolved: {resolved} | "
                    f"Prices: {p.get('q1_price', 0):.2f} -> {p.get('mid_price', 0):.2f} -> {p.get('final_price', 0):.2f}"
                )

    st.divider()

    # Key findings
    findings = data.get("key_findings", [])
    if findings:
        st.subheader("Erkenntnisse")
        for f in findings:
            st.markdown(f"- {f}")

    # Re-run button
    st.divider()
    if st.button("Backtest neu starten", type="secondary"):
        with st.spinner("Analysiere Weather-Markte..."):
            try:
                from backtesting.weather_backtest import run_weather_backtest
                run_weather_backtest()
                st.success("Fertig!")
                st.rerun()
            except Exception as e:
                st.error(f"Fehler: {e}")


def _render_bot_backtest():
    """Original bot trades backtest."""
    client = get_bot_client()

    # --- Load trade data from API ---
    trades = client.get_trades(limit=500)
    completed = [t for t in trades if t.get("result") is not None and t.get("pnl") is not None]

    st.markdown(f"**Abgeschlossene Trades vom Bot:** {len(completed)}")

    if len(completed) < 5:
        st.info("Zu wenig abgeschlossene Trades fur ein Backtesting. Mindestens 5 notig.")
        st.caption("Der Bot muss erst Trades ausfuhren und abschliessen, bevor ein Backtesting moglich ist.")

        # Offer synthetic data option
        st.divider()
        st.subheader("Synthetische Daten")
        n_synthetic = st.slider("Anzahl synthetischer Trades", 50, 500, 200, step=50)
        if st.button("Backtest mit synthetischen Daten", type="primary"):
            trades_df = _generate_synthetic_trades(n_synthetic)
            _run_full_backtest(trades_df, 1000.0, 0.05, 1000)
        return

    # --- Parameters ---
    with st.expander("Parameter", expanded=True):
        p1, p2, p3 = st.columns(3)
        with p1:
            initial_capital = st.number_input("Startkapital ($)", value=1000.0, step=100.0)
        with p2:
            max_position = st.slider("Max Position (%)", 1, 20, 5) / 100
        with p3:
            n_simulations = st.slider("Monte Carlo Runs", 100, 5000, 1000, step=100)

    if st.button("Backtest starten", type="primary"):
        trades_df = pd.DataFrame(completed)
        trades_df["pnl"] = trades_df["pnl"].astype(float)
        _run_full_backtest(trades_df, initial_capital, max_position, n_simulations)


def _generate_synthetic_trades(n_trades: int) -> pd.DataFrame:
    """Generate synthetic trade data for backtesting."""
    np.random.seed(42)
    win_rate = 0.55
    wins = np.random.random(n_trades) < win_rate
    pnls = []
    for w in wins:
        if w:
            pnls.append(np.random.uniform(2, 50))
        else:
            pnls.append(-np.random.uniform(2, 40))
    return pd.DataFrame({
        "pnl": pnls,
        "result": ["win" if w else "loss" for w in wins],
        "amount_usd": np.random.uniform(5, 50, n_trades),
    })


def _run_full_backtest(trades_df: pd.DataFrame, capital: float, max_pos: float, n_mc: int):
    """Run backtest analysis on trade data."""
    progress = st.progress(0, text="Backtest läuft...")

    pnls = trades_df["pnl"].values.astype(float)
    n_trades = len(pnls)
    wins = sum(1 for p in pnls if p > 0)
    losses = n_trades - wins
    win_rate = wins / n_trades if n_trades else 0
    total_pnl = float(pnls.sum())

    # Equity curve
    equity = [capital]
    for p in pnls:
        equity.append(equity[-1] + p)
    equity = np.array(equity)

    # Drawdown
    peak = np.maximum.accumulate(equity)
    dd = (equity - peak) / peak
    max_dd = float(abs(dd.min())) if len(dd) > 0 else 0

    # Sharpe ratio
    if len(pnls) > 1 and np.std(pnls) > 0:
        sharpe = float(np.mean(pnls) / np.std(pnls) * np.sqrt(252))
    else:
        sharpe = 0

    progress.progress(25, text="Monte Carlo...")

    # --- Results ---
    st.subheader("Backtest Ergebnis")
    kc = st.columns(5)
    with kc[0]:
        st.metric("Trades", n_trades)
    with kc[1]:
        st.metric("Win Rate", f"{win_rate:.1%}")
    with kc[2]:
        st.metric("PnL", f"${total_pnl:+.2f}")
    with kc[3]:
        st.metric("Sharpe", f"{sharpe:.2f}")
    with kc[4]:
        st.metric("Max DD", f"{max_dd:.1%}")

    # Equity + Drawdown chart
    fig = make_subplots(rows=2, cols=1, shared_xaxes=True, row_heights=[0.7, 0.3],
                        subplot_titles=["Equity Curve", "Drawdown"])
    fig.add_trace(go.Scatter(y=equity.tolist(), mode="lines", name="Equity",
                             line=dict(color="#1f77b4", width=2)), row=1, col=1)
    fig.add_trace(go.Scatter(y=dd.tolist(), mode="lines", name="Drawdown",
                             fill="tozeroy", line=dict(color="#d62728")), row=2, col=1)
    fig.update_layout(**CHART_LAYOUT, height=500, showlegend=False)
    st.plotly_chart(fig, use_container_width=True)

    # PnL distribution
    fig_dist = go.Figure()
    fig_dist.add_trace(go.Histogram(x=pnls[pnls > 0], name="Wins", marker_color="#2ca02c", nbinsx=30))
    fig_dist.add_trace(go.Histogram(x=pnls[pnls < 0], name="Losses", marker_color="#d62728", nbinsx=30))
    fig_dist.update_layout(**CHART_LAYOUT, height=300, title="PnL-Verteilung", barmode="overlay")
    fig_dist.update_traces(opacity=0.7)
    st.plotly_chart(fig_dist, use_container_width=True)

    st.divider()

    # --- Monte Carlo ---
    st.subheader("Monte Carlo Simulation")
    progress.progress(50, text="Monte Carlo Simulation...")

    mc_curves = []
    mc_finals = []
    for _ in range(n_mc):
        shuffled = np.random.permutation(pnls)
        eq = [capital]
        for p in shuffled:
            eq.append(eq[-1] + p)
        mc_curves.append(eq)
        mc_finals.append(eq[-1])

    mc_finals = np.array(mc_finals)
    median_pnl = float(np.median(mc_finals) - capital)
    pnl_5th = float(np.percentile(mc_finals, 5) - capital)
    pnl_95th = float(np.percentile(mc_finals, 95) - capital)
    prob_profit = float(np.mean(mc_finals > capital))

    mc_cols = st.columns(4)
    with mc_cols[0]:
        st.metric("Median PnL", f"${median_pnl:+.2f}")
    with mc_cols[1]:
        st.metric("5% Worst", f"${pnl_5th:+.2f}")
    with mc_cols[2]:
        st.metric("95% Best", f"${pnl_95th:+.2f}")
    with mc_cols[3]:
        st.metric("Profitabel", f"{prob_profit:.0%}")

    fig_mc = go.Figure()
    for curve in mc_curves[:30]:
        fig_mc.add_trace(go.Scatter(y=curve, mode="lines",
                                    line=dict(width=0.5, color="rgba(100,150,255,0.3)"), showlegend=False))
    median_curve = np.median(mc_curves, axis=0)
    fig_mc.add_trace(go.Scatter(y=median_curve.tolist(), mode="lines", name="Median",
                                line=dict(width=3, color="#ff7f0e")))
    fig_mc.update_layout(**CHART_LAYOUT, height=400, title=f"Monte Carlo ({n_mc} Runs)")
    st.plotly_chart(fig_mc, use_container_width=True)

    # Final capital histogram
    fig_hist = go.Figure(go.Histogram(x=mc_finals.tolist(), nbinsx=50, marker_color="#1f77b4"))
    fig_hist.add_vline(x=capital, line_dash="dash", line_color="red", annotation_text="Start")
    fig_hist.update_layout(**CHART_LAYOUT, height=300, title="Verteilung Endkapital")
    st.plotly_chart(fig_hist, use_container_width=True)

    progress.progress(100, text="Fertig!")
    progress.empty()
