"""
Backtesting Module — Strategy Backtester with parameter tuning & optimizer.
Replays strategies against 408K historical markets from 384M trade database.
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
RESULTS_FILE = Path("data/backtest_results/strategy_backtest.json")
OPT_RESULTS_FILE = Path("data/backtest_results/strategy_backtest_optimized.json")
OPT_LOG_FILE = Path("data/backtest_results/optimization_log.json")

ALL_CATEGORIES = ["Weather", "Crypto", "Sports", "Politics", "Economics", "Other"]

SIZING_MODES = {
    "fixed": "Fester Betrag — immer gleicher $-Betrag pro Trade",
    "percent_equity": "% vom Kapital — Einsatz wachst/schrumpft mit Equity (Compounding)",
    "kelly": "Kelly Criterion — optimale Wettgrosse basierend auf Edge & Odds",
}


def render():
    st.header("Backtesting")

    tab_strat, tab_opt, tab_bot = st.tabs([
        "Strategy Backtest",
        "Parameter Optimizer",
        "Bot Trades (Monte Carlo)",
    ])

    with tab_strat:
        _render_strategy_backtest()

    with tab_opt:
        _render_optimizer()

    with tab_bot:
        _render_bot_backtest()


# =====================================================================
# TAB 1: Strategy Backtest
# =====================================================================

def _render_strategy_backtest():
    """Main backtest tab with parameter controls and results."""

    st.subheader("Parameter einstellen")
    st.caption("Stelle deine Strategie-Parameter ein und starte den Backtest gegen 408K historische Markte (Sep 2022 – Jun 2025).")

    # --- Position Sizing Section ---
    st.markdown("##### Position Sizing")

    sizing_mode = st.radio(
        "Sizing Modus",
        options=list(SIZING_MODES.keys()),
        format_func=lambda x: SIZING_MODES[x],
        index=0,
        key="bt_sizing_mode",
        horizontal=True,
    )

    sc1, sc2, sc3 = st.columns(3)
    with sc1:
        capital = st.number_input("Startkapital ($)", value=1400.0, step=100.0, key="bt_capital")
    with sc2:
        if sizing_mode == "fixed":
            max_amount = st.number_input("Betrag pro Trade ($)", value=7.0, step=1.0, key="bt_amt",
                                         help="Fester $-Betrag pro Trade")
        else:
            max_amount = st.number_input("Max Betrag ($)", value=50.0, step=5.0, key="bt_amt",
                                         help="Obere Grenze pro Trade (0 = kein Limit)")
    with sc3:
        if sizing_mode == "fixed":
            max_pos_pct = st.slider("Max Position (% vom Kapital)", 1, 15, 1, key="bt_pos",
                                    help="Begrenzt den Trade auf X% des Startkapitals")
        elif sizing_mode == "percent_equity":
            max_pos_pct = st.slider("Einsatz (% vom Equity)", 1, 15, 3, key="bt_pos",
                                    help="Jeder Trade = X% deines aktuellen Kapitals")
        else:  # kelly
            max_pos_pct = st.slider("Max Kelly (%)", 1, 25, 10, key="bt_pos",
                                    help="Kelly wird auf max X% gedeckelt")

    # Sizing mode explanation
    if sizing_mode == "percent_equity":
        st.info("**Compounding aktiv:** Gewinnst du, werden deine Einsatze grosser. Verlierst du, werden sie kleiner. "
                "Beschleunigt Wachstum, aber auch Drawdowns.")
    elif sizing_mode == "kelly":
        st.info("**Kelly Criterion:** Berechnet die mathematisch optimale Wettgrosse aus Edge und Quoten. "
                "Maximiert langfristiges Wachstum, kann aber volatile sein.")

    # --- Entry Filters ---
    st.markdown("##### Entry Filters")
    c1, c2, c3 = st.columns(3)
    with c1:
        min_edge = st.slider("Min Edge (%)", 1, 30, 15, key="bt_edge") / 100
    with c2:
        min_volume = st.select_slider("Min Volume ($)", options=[1000, 3000, 5000, 10000, 25000, 50000], value=10000, key="bt_vol")
    with c3:
        categories = st.multiselect("Kategorien (leer = alle)", ALL_CATEGORIES, default=[], key="bt_cats")

    c4, c5, c6 = st.columns(3)
    with c4:
        min_price = st.slider("Min Price", 0.01, 0.30, 0.05, step=0.01, key="bt_minp")
    with c5:
        max_price = st.slider("Max Price", 0.50, 0.99, 0.85, step=0.01, key="bt_maxp")
    with c6:
        stop_loss = st.slider("Stop Loss (%)", 5, 50, 25, key="bt_sl")

    # --- Risk Management ---
    st.markdown("##### Risk Management")
    r1, r2 = st.columns(2)
    with r1:
        max_losses = st.slider("Circuit Breaker (Verluste in Folge)", 1, 10, 3, key="bt_cb")
    with r2:
        pause_trades = st.slider("Pause nach Circuit Breaker (Trades)", 0, 50, 10, key="bt_pause",
                                  help="Wie viele Trades werden nach dem Circuit Breaker uebersprungen")

    # Run button
    col_run, col_status = st.columns([1, 3])
    with col_run:
        run_bt = st.button("Backtest starten", type="primary", key="bt_run")

    if run_bt:
        with st.spinner("Backtest lauft... (408K Markte werden analysiert)"):
            try:
                from backtesting.strategy_backtester import BacktestConfig, run_backtest
                config = BacktestConfig(
                    capital_usd=capital,
                    max_position_pct=max_pos_pct,
                    max_amount_usd=max_amount,
                    sizing_mode=sizing_mode,
                    min_edge=min_edge,
                    min_volume=min_volume,
                    min_price=min_price,
                    max_price=max_price,
                    stop_loss_pct=stop_loss,
                    max_consecutive_losses=max_losses,
                    pause_after_losses=pause_trades,
                    categories=categories,
                    strategy_name="manual",
                )
                result = run_backtest(config)
                st.success(f"Backtest fertig! {result.total_trades} Trades in {result.duration_seconds:.1f}s")
                st.rerun()
            except Exception as e:
                st.error(f"Fehler: {e}")
                import traceback
                st.code(traceback.format_exc())
        return

    # Show results if available
    if RESULTS_FILE.exists():
        _show_backtest_results(RESULTS_FILE)
    else:
        st.info("Noch kein Backtest durchgefuhrt. Stelle Parameter ein und klicke 'Backtest starten'.")


def _show_backtest_results(results_file: Path):
    """Display comprehensive backtest results."""
    try:
        data = json.loads(results_file.read_text())
    except Exception as e:
        st.error(f"Fehler beim Laden: {e}")
        return

    summary = data.get("summary", {})
    config = data.get("config", {})

    st.divider()

    # Config summary
    sizing = config.get("sizing_mode", "fixed")
    sizing_label = {"fixed": "Fest", "percent_equity": "% Equity", "kelly": "Kelly"}.get(sizing, sizing)
    st.caption(
        f"Kapital: ${config.get('capital_usd', 0):,.0f} | "
        f"Sizing: **{sizing_label}** | "
        f"Max Position: {config.get('max_position_pct', 0)}% | "
        f"Max Amount: ${config.get('max_amount_usd', 0)} | "
        f"Min Edge: {config.get('min_edge', 0):.0%} | "
        f"Min Volume: ${config.get('min_volume', 0):,.0f}"
    )

    # KPI Row
    st.subheader("Ergebnis")
    kc = st.columns(6)
    with kc[0]:
        st.metric("Trades", summary.get("total_trades", 0))
    with kc[1]:
        wr = summary.get("win_rate", 0)
        st.metric("Win Rate", f"{wr:.1f}%")
    with kc[2]:
        pnl = summary.get("total_pnl", 0)
        st.metric("PnL", f"${pnl:+,.2f}")
    with kc[3]:
        st.metric("Sharpe", f"{summary.get('sharpe_ratio', 0):.2f}")
    with kc[4]:
        st.metric("Max Drawdown", f"{summary.get('max_drawdown_pct', 0):.1f}%")
    with kc[5]:
        pf = summary.get("profit_factor", 0)
        pf_str = f"{pf:.2f}" if pf != float("inf") else "Inf"
        st.metric("Profit Factor", pf_str)

    # Second KPI row
    kc2 = st.columns(4)
    with kc2[0]:
        st.metric("Avg PnL/Trade", f"${summary.get('avg_pnl', 0):+.2f}")
    with kc2[1]:
        st.metric("Max Win", f"${summary.get('max_win', 0):+.2f}")
    with kc2[2]:
        st.metric("Max Loss", f"${summary.get('max_loss', 0):.2f}")
    with kc2[3]:
        # ROI
        cap = config.get("capital_usd", 1400)
        roi = (pnl / cap * 100) if cap > 0 else 0
        st.metric("ROI", f"{roi:+.1f}%")

    st.divider()

    # Equity Curve + Drawdown
    equity = data.get("equity_curve", [])
    dd_curve = data.get("drawdown_curve", [])

    if equity:
        fig = make_subplots(
            rows=2, cols=1, shared_xaxes=True, row_heights=[0.7, 0.3],
            subplot_titles=["Equity Curve", "Drawdown (%)"],
            vertical_spacing=0.08,
        )
        fig.add_trace(go.Scatter(
            y=equity, mode="lines", name="Equity",
            line=dict(color="#1f77b4", width=2),
        ), row=1, col=1)
        # Add start capital line
        fig.add_hline(y=config.get("capital_usd", 1400), line_dash="dash",
                      line_color="gray", row=1, col=1)

        if dd_curve:
            fig.add_trace(go.Scatter(
                y=[-d for d in dd_curve], mode="lines", name="Drawdown",
                fill="tozeroy", line=dict(color="#d62728", width=1),
            ), row=2, col=1)

        fig.update_layout(**CHART_LAYOUT, height=500, showlegend=False)
        fig.update_yaxes(title_text="$", row=1, col=1)
        fig.update_yaxes(title_text="%", row=2, col=1)
        st.plotly_chart(fig, use_container_width=True)

    # Category Breakdown
    cat_stats = data.get("category_stats", {})
    if cat_stats:
        st.subheader("Performance pro Kategorie")
        cat_rows = []
        for cat, s in sorted(cat_stats.items(), key=lambda x: x[1].get("pnl", 0), reverse=True):
            cat_rows.append({
                "Kategorie": cat,
                "Trades": s.get("trades", 0),
                "Win Rate": f"{s.get('win_rate', 0):.1f}%",
                "PnL": f"${s.get('pnl', 0):+.2f}",
            })
        st.dataframe(pd.DataFrame(cat_rows), use_container_width=True, hide_index=True)

        # Category PnL bar chart
        fig_cat = go.Figure()
        cats = [r["Kategorie"] for r in cat_rows]
        cat_pnls = [cat_stats[c].get("pnl", 0) for c in cats]
        colors = ["#00c853" if p > 0 else "#ff1744" for p in cat_pnls]
        fig_cat.add_trace(go.Bar(x=cats, y=cat_pnls, marker_color=colors))
        fig_cat.update_layout(**CHART_LAYOUT, height=300, title="PnL pro Kategorie")
        st.plotly_chart(fig_cat, use_container_width=True)

    # PnL Distribution
    trades_data = data.get("trades", [])
    if trades_data:
        st.subheader("Trade-Verteilung")
        trade_pnls = [t.get("pnl", 0) for t in trades_data]
        win_pnls = [p for p in trade_pnls if p > 0]
        loss_pnls = [p for p in trade_pnls if p <= 0]

        fig_dist = go.Figure()
        if win_pnls:
            fig_dist.add_trace(go.Histogram(x=win_pnls, name="Wins", marker_color="#2ca02c", nbinsx=30))
        if loss_pnls:
            fig_dist.add_trace(go.Histogram(x=loss_pnls, name="Losses", marker_color="#d62728", nbinsx=30))
        fig_dist.update_layout(**CHART_LAYOUT, height=300, title="PnL-Verteilung", barmode="overlay")
        fig_dist.update_traces(opacity=0.7)
        st.plotly_chart(fig_dist, use_container_width=True)

        # Trade table
        with st.expander(f"Alle Trades ({len(trades_data)})", expanded=False):
            df_trades = pd.DataFrame(trades_data)
            if not df_trades.empty:
                display_cols = ["question", "category", "side", "entry_price", "amount_usd", "pnl", "pnl_pct", "result", "edge"]
                available = [c for c in display_cols if c in df_trades.columns]
                st.dataframe(df_trades[available], use_container_width=True, hide_index=True)


# =====================================================================
# TAB 2: Parameter Optimizer
# =====================================================================

def _render_optimizer():
    """Auto-optimizer that finds the best parameters."""
    st.subheader("Parameter Optimizer")
    st.caption("Testet automatisch verschiedene Parameter-Kombinationen und findet die profitabelsten Einstellungen.")

    c1, c2, c3 = st.columns(3)
    with c1:
        opt_metric = st.selectbox("Optimieren fuer", [
            "sharpe_ratio", "total_pnl", "win_rate", "profit_factor",
        ], index=0, key="opt_metric")
    with c2:
        n_iter = st.slider("Iterationen", 20, 200, 50, step=10, key="opt_iter")
    with c3:
        opt_sizing = st.selectbox("Sizing Modus", list(SIZING_MODES.keys()),
                                   format_func=lambda x: x.replace("_", " ").title(),
                                   index=0, key="opt_sizing")

    c4, c5 = st.columns(2)
    with c4:
        categories = st.multiselect("Kategorien (leer = alle)", ALL_CATEGORIES, default=[], key="opt_cats")
    with c5:
        capital = st.number_input("Startkapital ($)", value=1400.0, step=100.0, key="opt_capital")

    if st.button("Optimierung starten", type="primary", key="opt_run"):
        with st.spinner(f"Optimiere {opt_metric} uber {n_iter} Iterationen..."):
            try:
                from backtesting.strategy_backtester import BacktestConfig, run_optimization
                base = BacktestConfig(capital_usd=capital, categories=categories, sizing_mode=opt_sizing)
                best_config, log, best_result = run_optimization(base, opt_metric, n_iter)
                st.success(
                    f"Beste Parameter gefunden! "
                    f"Edge={best_config.min_edge:.0%}, "
                    f"Amount=${best_config.max_amount_usd}, "
                    f"Position={best_config.max_position_pct}% "
                    f"-> {opt_metric}={getattr(best_result, opt_metric, 0)}"
                )
                st.rerun()
            except Exception as e:
                st.error(f"Fehler: {e}")
                import traceback
                st.code(traceback.format_exc())
        return

    # Show optimization results
    if OPT_LOG_FILE.exists():
        _show_optimization_results()

    if OPT_RESULTS_FILE.exists():
        st.divider()
        st.subheader("Beste Parameter - Backtest Ergebnis")
        _show_backtest_results(OPT_RESULTS_FILE)


def _show_optimization_results():
    """Show optimization log with parameter comparison."""
    try:
        log = json.loads(OPT_LOG_FILE.read_text())
    except Exception:
        return

    if not log:
        return

    st.subheader("Optimierungs-Ergebnisse")

    # Best parameters
    best = log[0]  # sorted by score
    st.markdown("**Beste Parameter:**")
    bc = st.columns(4)
    with bc[0]:
        st.metric("Min Edge", f"{best.get('min_edge', 0):.0%}")
    with bc[1]:
        st.metric("Max Amount", f"${best.get('max_amount_usd', 0)}")
    with bc[2]:
        st.metric("Max Position", f"{best.get('max_position_pct', 0)}%")
    with bc[3]:
        st.metric("Min Volume", f"${best.get('min_volume', 0):,.0f}")

    # Results table
    df_log = pd.DataFrame(log[:20])  # top 20
    display_cols = ["min_edge", "max_amount_usd", "max_position_pct", "min_volume",
                    "min_price", "max_price", "total_trades", "win_rate", "total_pnl",
                    "sharpe_ratio", "max_drawdown_pct", "profit_factor", "score"]
    available = [c for c in display_cols if c in df_log.columns]
    st.dataframe(df_log[available], use_container_width=True, hide_index=True)

    # Scatter: edge vs PnL
    if len(log) > 5:
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=[r.get("min_edge", 0) for r in log],
            y=[r.get("total_pnl", 0) for r in log],
            mode="markers",
            marker=dict(
                size=8,
                color=[r.get("sharpe_ratio", 0) for r in log],
                colorscale="RdYlGn",
                showscale=True,
                colorbar=dict(title="Sharpe"),
            ),
            text=[f"WR={r.get('win_rate', 0)}% Trades={r.get('total_trades', 0)}" for r in log],
        ))
        fig.update_layout(**CHART_LAYOUT, height=400, title="Edge vs PnL (Farbe = Sharpe)",
                          xaxis_title="Min Edge", yaxis_title="Total PnL ($)")
        st.plotly_chart(fig, use_container_width=True)


# =====================================================================
# TAB 3: Bot Trades (Monte Carlo) - original
# =====================================================================

def _render_bot_backtest():
    """Original bot trades backtest with Monte Carlo."""
    client = get_bot_client()
    trades = client.get_trades(limit=500)
    completed = [t for t in trades if t.get("result") is not None and t.get("pnl") is not None]

    st.markdown(f"**Abgeschlossene Trades vom Bot:** {len(completed)}")

    if len(completed) < 5:
        st.info("Zu wenig abgeschlossene Trades. Mindestens 5 notig.")
        st.divider()
        st.subheader("Synthetische Daten")
        n_synthetic = st.slider("Anzahl synthetischer Trades", 50, 500, 200, step=50)
        if st.button("Backtest mit synthetischen Daten", type="primary"):
            trades_df = _generate_synthetic_trades(n_synthetic)
            _run_full_backtest(trades_df, 1000.0, 0.05, 1000)
        return

    with st.expander("Parameter", expanded=True):
        p1, p2, p3 = st.columns(3)
        with p1:
            initial_capital = st.number_input("Startkapital ($)", value=1000.0, step=100.0)
        with p2:
            max_position = st.slider("Max Position (%)", 1, 20, 5) / 100
        with p3:
            n_simulations = st.slider("Monte Carlo Runs", 100, 5000, 1000, step=100)

    if st.button("Backtest starten", type="primary", key="mc_run"):
        trades_df = pd.DataFrame(completed)
        trades_df["pnl"] = trades_df["pnl"].astype(float)
        _run_full_backtest(trades_df, initial_capital, max_position, n_simulations)


def _generate_synthetic_trades(n_trades: int) -> pd.DataFrame:
    np.random.seed(42)
    win_rate = 0.55
    wins = np.random.random(n_trades) < win_rate
    pnls = []
    for w in wins:
        pnls.append(np.random.uniform(2, 50) if w else -np.random.uniform(2, 40))
    return pd.DataFrame({
        "pnl": pnls,
        "result": ["win" if w else "loss" for w in wins],
        "amount_usd": np.random.uniform(5, 50, n_trades),
    })


def _run_full_backtest(trades_df: pd.DataFrame, capital: float, max_pos: float, n_mc: int):
    progress = st.progress(0, text="Backtest lauft...")

    pnls = trades_df["pnl"].values.astype(float)
    n_trades = len(pnls)
    wins = sum(1 for p in pnls if p > 0)
    win_rate = wins / n_trades if n_trades else 0
    total_pnl = float(pnls.sum())

    equity = [capital]
    for p in pnls:
        equity.append(equity[-1] + p)
    equity = np.array(equity)

    peak = np.maximum.accumulate(equity)
    dd = (equity - peak) / peak
    max_dd = float(abs(dd.min())) if len(dd) > 0 else 0

    if len(pnls) > 1 and np.std(pnls) > 0:
        sharpe = float(np.mean(pnls) / np.std(pnls) * np.sqrt(252))
    else:
        sharpe = 0

    progress.progress(25, text="Monte Carlo...")

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

    fig = make_subplots(rows=2, cols=1, shared_xaxes=True, row_heights=[0.7, 0.3],
                        subplot_titles=["Equity Curve", "Drawdown"])
    fig.add_trace(go.Scatter(y=equity.tolist(), mode="lines", name="Equity",
                             line=dict(color="#1f77b4", width=2)), row=1, col=1)
    fig.add_trace(go.Scatter(y=dd.tolist(), mode="lines", name="Drawdown",
                             fill="tozeroy", line=dict(color="#d62728")), row=2, col=1)
    fig.update_layout(**CHART_LAYOUT, height=500, showlegend=False)
    st.plotly_chart(fig, use_container_width=True)

    fig_dist = go.Figure()
    fig_dist.add_trace(go.Histogram(x=pnls[pnls > 0], name="Wins", marker_color="#2ca02c", nbinsx=30))
    fig_dist.add_trace(go.Histogram(x=pnls[pnls < 0], name="Losses", marker_color="#d62728", nbinsx=30))
    fig_dist.update_layout(**CHART_LAYOUT, height=300, title="PnL-Verteilung", barmode="overlay")
    fig_dist.update_traces(opacity=0.7)
    st.plotly_chart(fig_dist, use_container_width=True)

    st.divider()

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

    fig_hist = go.Figure(go.Histogram(x=mc_finals.tolist(), nbinsx=50, marker_color="#1f77b4"))
    fig_hist.add_vline(x=capital, line_dash="dash", line_color="red", annotation_text="Start")
    fig_hist.update_layout(**CHART_LAYOUT, height=300, title="Verteilung Endkapital")
    st.plotly_chart(fig_hist, use_container_width=True)

    progress.progress(100, text="Fertig!")
    progress.empty()
