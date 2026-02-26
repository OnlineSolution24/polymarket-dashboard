"""
Tab 4: Backtesting Module
Full backtesting with Monte-Carlo, Walk-Forward, and Drawdown analysis.
Interactive Plotly charts for all results.
"""

import streamlit as st
import numpy as np
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from db import engine
from backtesting.data_loader import load_trade_history, generate_synthetic_trades
from backtesting.simulator import run_backtest
from backtesting.monte_carlo import run_monte_carlo
from backtesting.drawdown import analyze_drawdowns
from backtesting.walk_forward import run_walk_forward

CHART_LAYOUT = dict(template="plotly_dark", margin=dict(l=40, r=20, t=40, b=40), font=dict(size=12))


def render():
    st.header("Backtesting")

    # --- Data Source ---
    trade_count = engine.query_one("SELECT COUNT(*) as cnt FROM trades WHERE result IS NOT NULL AND pnl IS NOT NULL")
    real_count = trade_count["cnt"] if trade_count else 0

    st.markdown(f"**Abgeschlossene Trades in DB:** {real_count}")

    data_source = st.radio(
        "Datenquelle",
        ["Echte Trades", "Synthetische Daten", "Echte + Synthetische"],
        horizontal=True,
        index=1 if real_count < 20 else 0,
    )

    # --- Parameters ---
    with st.expander("Parameter", expanded=True):
        p1, p2, p3, p4 = st.columns(4)
        with p1:
            initial_capital = st.number_input("Startkapital ($)", value=1000.0, step=100.0)
        with p2:
            max_position = st.slider("Max Position (%)", 1, 20, 5) / 100
        with p3:
            n_simulations = st.slider("Monte Carlo Runs", 100, 5000, 1000, step=100)
        with p4:
            n_synthetic = st.slider("Synthetische Trades", 50, 500, 200, step=50)

    # --- Load Data ---
    if data_source == "Echte Trades":
        trades_df = load_trade_history()
        if trades_df.empty:
            st.warning("Keine abgeschlossenen Trades vorhanden. Wähle 'Synthetische Daten'.")
            return
    elif data_source == "Synthetische Daten":
        trades_df = generate_synthetic_trades(n_trades=n_synthetic)
    else:
        real = load_trade_history()
        synth = generate_synthetic_trades(n_trades=max(0, n_synthetic - len(real)))
        trades_df = pd.concat([real, synth], ignore_index=True) if not real.empty else synth

    if st.button("Backtest starten", type="primary"):
        _run_full_backtest(trades_df, initial_capital, max_position, n_simulations)


def _run_full_backtest(trades_df: pd.DataFrame, capital: float, max_pos: float, n_mc: int):
    """Run full backtest suite."""
    progress = st.progress(0, text="Backtest läuft...")

    # 1. Basic Backtest
    result = run_backtest(trades_df, initial_capital=capital, max_position_pct=max_pos)
    progress.progress(25, text="Monte Carlo...")

    st.subheader("Backtest Ergebnis")
    kc = st.columns(6)
    with kc[0]:
        st.metric("Trades", result.total_trades)
    with kc[1]:
        st.metric("Win Rate", f"{result.win_rate:.1%}")
    with kc[2]:
        st.metric("PnL", f"${result.total_pnl:+.2f}")
    with kc[3]:
        st.metric("Sharpe", f"{result.sharpe_ratio:.2f}")
    with kc[4]:
        st.metric("Profit Factor", f"{result.profit_factor:.2f}")
    with kc[5]:
        st.metric("Max DD", f"{result.max_drawdown_pct:.1%}")

    # Equity + Drawdown chart
    fig = make_subplots(rows=2, cols=1, shared_xaxes=True, row_heights=[0.7, 0.3],
                        subplot_titles=["Equity Curve", "Drawdown"])
    fig.add_trace(go.Scatter(y=result.equity_curve, mode="lines", name="Equity",
                             line=dict(color="#1f77b4", width=2)), row=1, col=1)
    fig.add_trace(go.Scatter(y=result.drawdown_curve, mode="lines", name="Drawdown",
                             fill="tozeroy", line=dict(color="#d62728")), row=2, col=1)
    fig.update_layout(**CHART_LAYOUT, height=500, showlegend=False)
    st.plotly_chart(fig, use_container_width=True)

    # PnL distribution
    if not result.trades.empty and "adjusted_pnl" in result.trades.columns:
        pnls = result.trades["adjusted_pnl"]
        fig_dist = go.Figure()
        fig_dist.add_trace(go.Histogram(x=pnls[pnls > 0], name="Wins", marker_color="#2ca02c", nbinsx=30))
        fig_dist.add_trace(go.Histogram(x=pnls[pnls < 0], name="Losses", marker_color="#d62728", nbinsx=30))
        fig_dist.update_layout(**CHART_LAYOUT, height=300, title="PnL-Verteilung", barmode="overlay")
        fig_dist.update_traces(opacity=0.7)
        st.plotly_chart(fig_dist, use_container_width=True)

    st.divider()

    # 2. Monte Carlo
    st.subheader("Monte Carlo Simulation")
    pnls_array = result.trades["adjusted_pnl"].values if (not result.trades.empty and "adjusted_pnl" in result.trades.columns) else trades_df["pnl"].fillna(0).values
    mc = run_monte_carlo(pnls_array, n_simulations=n_mc, initial_capital=capital)
    progress.progress(50, text="Walk-Forward...")

    mc_cols = st.columns(5)
    with mc_cols[0]:
        st.metric("Median PnL", f"${mc.median_pnl:+.2f}")
    with mc_cols[1]:
        st.metric("5% Worst", f"${mc.pnl_5th:+.2f}")
    with mc_cols[2]:
        st.metric("95% Best", f"${mc.pnl_95th:+.2f}")
    with mc_cols[3]:
        st.metric("Profitabel", f"{mc.prob_profitable:.0%}")
    with mc_cols[4]:
        st.metric("Worst DD", f"${mc.worst_max_dd:.2f}")

    # MC curves
    fig_mc = go.Figure()
    for curve in mc.equity_curves[:30]:
        fig_mc.add_trace(go.Scatter(y=curve, mode="lines",
                                    line=dict(width=0.5, color="rgba(100,150,255,0.3)"), showlegend=False))
    if mc.equity_curves:
        median_curve = np.median(mc.equity_curves, axis=0)
        fig_mc.add_trace(go.Scatter(y=median_curve.tolist(), mode="lines", name="Median",
                                    line=dict(width=3, color="#ff7f0e")))
    fig_mc.update_layout(**CHART_LAYOUT, height=400, title=f"Monte Carlo ({mc.n_simulations} Runs)")
    st.plotly_chart(fig_mc, use_container_width=True)

    # Final capital histogram
    fig_hist = go.Figure(go.Histogram(x=mc.final_capitals, nbinsx=50, marker_color="#1f77b4"))
    fig_hist.add_vline(x=capital, line_dash="dash", line_color="red", annotation_text="Start")
    fig_hist.update_layout(**CHART_LAYOUT, height=300, title="Verteilung Endkapital")
    st.plotly_chart(fig_hist, use_container_width=True)

    st.divider()

    # 3. Walk-Forward
    st.subheader("Walk-Forward Analyse")
    wf = run_walk_forward(trades_df, n_windows=5)
    progress.progress(75, text="Drawdown-Analyse...")

    if wf.windows:
        wc = st.columns(4)
        with wc[0]:
            st.metric("Windows", wf.n_windows)
        with wc[1]:
            st.metric("Ø Test WR", f"{wf.avg_test_win_rate:.1%}")
        with wc[2]:
            st.metric("Konsistenz", f"{wf.consistency_score:.0%}")
        with wc[3]:
            st.metric("Degradation", f"{wf.degradation:+.1%}")

        labels = [f"W{w.window_id+1}" for w in wf.windows]
        fig_wf = go.Figure()
        fig_wf.add_trace(go.Bar(x=labels, y=[w.train_win_rate for w in wf.windows], name="Train", marker_color="#1f77b4"))
        fig_wf.add_trace(go.Bar(x=labels, y=[w.test_win_rate for w in wf.windows], name="Test", marker_color="#ff7f0e"))
        fig_wf.update_layout(**CHART_LAYOUT, height=300, title="Train vs Test Win Rate", barmode="group")
        st.plotly_chart(fig_wf, use_container_width=True)

        fig_pnl = go.Figure(go.Bar(
            x=labels, y=[w.test_pnl for w in wf.windows],
            marker_color=["#2ca02c" if w.test_pnl > 0 else "#d62728" for w in wf.windows],
        ))
        fig_pnl.update_layout(**CHART_LAYOUT, height=250, title="Test PnL pro Window")
        st.plotly_chart(fig_pnl, use_container_width=True)
    else:
        st.info("Zu wenig Daten für Walk-Forward (min. 20 Trades).")

    st.divider()

    # 4. Drawdown
    st.subheader("Drawdown-Analyse")
    dd = analyze_drawdowns(result.equity_curve)
    progress.progress(100, text="Fertig!")
    progress.empty()

    dc = st.columns(4)
    with dc[0]:
        st.metric("Max DD", f"{dd.max_drawdown_pct:.1%}")
    with dc[1]:
        st.metric("Ø DD", f"{dd.avg_drawdown_pct:.1%}")
    with dc[2]:
        st.metric("Zeit in DD", f"{dd.time_in_drawdown_pct:.0%}")
    with dc[3]:
        st.metric("Längster DD", f"{dd.longest_drawdown_trades} Trades")

    if dd.top_drawdowns:
        st.markdown("**Top Drawdown-Perioden:**")
        for i, p in enumerate(dd.top_drawdowns[:5]):
            rec = f"Recovery: {p.recovery_trades} Trades" if p.recovery_trades else "Aktiv"
            st.caption(f"{i+1}. {p.drawdown_pct:.1%} (${p.drawdown_abs:.2f}) — {p.duration_trades} Trades — {rec}")
