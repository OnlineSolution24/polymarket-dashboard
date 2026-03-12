"""
Portfolio & Performance — Live positions from Polymarket API,
closed markets with per-market W/L, and portfolio snapshots.
Bot controls (pause/resume, circuit breaker, risk settings) at the bottom.
"""

import streamlit as st
import pandas as pd
from datetime import datetime, timedelta

from services.bot_api_client import get_bot_client


def render():
    st.header("Dashboard")

    client = get_bot_client()
    perf = client.get_performance()

    total_deposited = perf.get("total_deposited", 0)
    positions_value = perf.get("positions_value", 0)
    positions_cost = perf.get("positions_cost", 0)
    unrealized_pnl = perf.get("unrealized_pnl", 0)
    realized_pnl = perf.get("realized_pnl", 0)
    wins = perf.get("wins", 0)
    losses = perf.get("losses", 0)
    open_markets = perf.get("open_market_count", 0)
    equity_curve = perf.get("equity_curve", [])
    total_closed = len(perf.get("closed_markets", []))

    # Calculated values
    cash_available = max(total_deposited - positions_cost + realized_pnl, 0)
    portfolio_total = positions_value + cash_available
    total_pnl = unrealized_pnl + realized_pnl
    total_pnl_pct = (total_pnl / total_deposited * 100) if total_deposited > 0 else 0
    wr = (wins / total_closed * 100) if total_closed > 0 else 0

    # Today's PNL from equity curve
    today_pnl = _calc_today_pnl(equity_curve, unrealized_pnl, realized_pnl)

    # ══════════════════════════════════════════════════════════════════
    # 1. PORTFOLIO OVERVIEW — 4 Cards in one row
    # ══════════════════════════════════════════════════════════════════
    c1, c2, c3, c4 = st.columns(4)

    _dep_str = f" (${total_deposited:,.0f} eingezahlt)" if total_deposited else ""
    _today_color = "green" if today_pnl >= 0 else "red"
    _sign = "+" if today_pnl >= 0 else ""
    _today_pct = (today_pnl / portfolio_total * 100) if portfolio_total > 0 else 0
    _pnl_color = "green" if total_pnl >= 0 else "red"
    _u_color = "green" if unrealized_pnl >= 0 else "red"
    _r_color = "green" if realized_pnl >= 0 else "red"

    with c1:
        with st.container(border=True):
            st.caption(f"Portfolio{_dep_str}")
            st.markdown(f"### ${portfolio_total:,.2f}")
            st.markdown(f"Verfügbar: **${cash_available:,.2f}**")
            st.markdown(f":{_today_color}[{_sign}${today_pnl:.2f} ({_sign}{_today_pct:.1f}%) heute]")

    with c2:
        with st.container(border=True):
            st.caption("Gewinn/Verlust")
            st.markdown(f"### :{_pnl_color}[${total_pnl:+,.2f}]")
            st.markdown(f":{_pnl_color}[{total_pnl_pct:+.1f}%] Gesamt")
            st.markdown(f"W/L: **{wins}/{losses}** | WR: **{wr:.0f}%**")

    with c3:
        with st.container(border=True):
            st.caption(f"Offene Positionen ({open_markets})")
            st.markdown(f"### ${positions_value:,.2f}")
            st.markdown(f"Einsatz: **${positions_cost:,.2f}**")
            st.markdown(f"Unrealisiert: :{_u_color}[${unrealized_pnl:+,.2f}]")

    with c4:
        with st.container(border=True):
            st.caption("Realisiert")
            _r_color = "green" if realized_pnl >= 0 else "red"
            st.markdown(f"### :{_r_color}[${realized_pnl:+,.2f}]")
            _m_label = "Markt" if total_closed == 1 else "Märkte"
            st.markdown(f"{total_closed} {_m_label} abgeschlossen")
            st.markdown(f"W/L: **{wins}/{losses}**")

    # Equity curve (compact, below cards)
    if equity_curve:
        period = st.segmented_control(
            "eq", ["1D", "1W", "1M", "All"],
            default="All", key="eq_period",
            label_visibility="collapsed",
        ) or "All"
        df_eq = _filter_equity_curve(equity_curve, period)
        if not df_eq.empty:
            _chart_color = "#00c853" if total_pnl >= 0 else "#ff1744"
            _build_equity_chart(df_eq, _chart_color)

    st.divider()

    # ══════════════════════════════════════════════════════════════════
    # 2. OFFENE POSITIONEN (Live from Polymarket API)
    # ══════════════════════════════════════════════════════════════════
    st.subheader("Offene Positionen (Live)")

    live_positions = perf.get("live_positions", [])
    if live_positions:
        # Auto-import: register any untracked on-chain positions in DB
        imported_count = 0
        for pos in live_positions:
            if not pos.get("trade_id") and pos.get("market_id"):
                result = client.import_position({
                    "market_id": pos["market_id"],
                    "title": pos.get("title", ""),
                    "outcome": pos.get("outcome", "YES"),
                    "avg_price": pos.get("avg_price", 0),
                    "cost": pos.get("cost", 0),
                    "shares": pos.get("shares", 0),
                })
                if result and result.get("ok"):
                    pos["trade_id"] = result["trade_id"]
                    imported_count += 1
        if imported_count:
            st.info(f"{imported_count} Position(en) automatisch importiert und werden jetzt vom Bot verwaltet.")

        # Compact table: Markt | Einstieg→Aktuell | Shares | Wert | PnL | Sell
        _W = [3.5, 1.2, 0.6, 0.6, 0.8, 0.4]
        hdr = st.columns(_W)
        for col, lbl in zip(hdr, ["Markt", "Einstieg → Aktuell", "Shares", "Wert", "PnL", ""]):
            col.markdown(f"<span style='color:#5A6478;font-size:0.75rem'>{lbl}</span>", unsafe_allow_html=True)

        for i, pos in enumerate(live_positions):
            row = st.columns(_W)
            pnl, pnl_pct = pos["pnl"], pos["pnl_pct"]
            pc = "#00c853" if pnl > 0 else "#ff1744" if pnl < 0 else "#888"

            row[0].markdown(f"<span style='font-size:0.82rem'>{pos['title'][:42]}</span>", unsafe_allow_html=True)
            row[1].markdown(f"<span style='font-size:0.82rem'>${pos['avg_price']:.4f} → ${pos['cur_price']:.4f}</span>", unsafe_allow_html=True)
            row[2].markdown(f"<span style='font-size:0.82rem'>{pos['shares']:,.0f}</span>", unsafe_allow_html=True)
            row[3].markdown(f"<span style='font-size:0.82rem'>${pos['value']:.2f}</span>", unsafe_allow_html=True)
            row[4].markdown(f"<span style='font-size:0.82rem;color:{pc}'>${pnl:+.2f} ({pnl_pct:+.1f}%)</span>", unsafe_allow_html=True)

            trade_id = pos.get("trade_id")
            if trade_id:
                if row[5].button("✕", key=f"sell_{trade_id}_{i}", help="Position verkaufen"):
                    with st.spinner("Verkaufe..."):
                        res = client.manual_cashout(trade_id)
                    if res and res.get("ok"):
                        st.success(f"Verkauft! Profit: ${res.get('profit_usd', 0):+.2f}")
                        st.rerun()
                    else:
                        st.error(f"Fehler: {res.get('error', '?') if res else '?'}")

        st.caption(
            f"**Gesamt:** Einsatz ${positions_cost:.2f} | "
            f"Wert ${positions_value:.2f} | "
            f"PnL ${unrealized_pnl:+.2f}"
        )
    else:
        st.caption("Keine offenen Positionen.")

    st.divider()

    # ══════════════════════════════════════════════════════════════════
    # 3. ABGESCHLOSSENE MÄRKTE
    # ══════════════════════════════════════════════════════════════════
    st.subheader("Abgeschlossene Märkte")

    closed_markets = perf.get("closed_markets", [])

    if closed_markets:
        df_closed = pd.DataFrame(closed_markets)
        display_cols = {}
        if "name" in df_closed.columns:
            display_cols["name"] = "Markt"
        if "result" in df_closed.columns:
            display_cols["result"] = "Ergebnis"
        if "pnl" in df_closed.columns:
            display_cols["pnl"] = "PnL ($)"
        if "trade_count" in df_closed.columns:
            display_cols["trade_count"] = "Trades"
        df_closed = df_closed.rename(columns=display_cols)
        if "Ergebnis" in df_closed.columns:
            df_closed["Ergebnis"] = df_closed["Ergebnis"].str.upper()
        show_cols = [c for c in ["Markt", "Ergebnis", "PnL ($)", "Trades"] if c in df_closed.columns]
        st.dataframe(df_closed[show_cols], use_container_width=True, hide_index=True)
    else:
        st.caption("Noch keine abgeschlossenen Märkte.")

    _open_label = "Markt" if open_markets == 1 else "Märkte"
    st.caption(f"{open_markets} {_open_label} noch offen")

    st.divider()

    # ══════════════════════════════════════════════════════════════════
    # 4. LETZTE AKTIVITÄTEN
    # ══════════════════════════════════════════════════════════════════
    st.subheader("Letzte Aktivität")
    recent_logs = client.get_logs(limit=10)
    if recent_logs:
        for log in recent_logs:
            level_icon = {"info": "ℹ️", "warn": "⚠️", "error": "❌", "debug": "🔍"}.get(log.get("level", ""), "📝")
            ts = (log.get("created_at") or "")[:16]
            st.caption(f"{level_icon} `{ts}` **{log.get('agent_id', 'system')}**: {log.get('message', '')}")
    else:
        st.caption("Noch keine Agent-Aktivitäten.")

    st.divider()

    # ══════════════════════════════════════════════════════════════════
    # 5. EINZAHLUNG BEARBEITEN
    # ══════════════════════════════════════════════════════════════════
    with st.expander("Einzahlung anpassen", expanded=False):
        new_deposited = st.number_input(
            "Gesamt eingezahlt ($)",
            min_value=0.0, max_value=100000.0,
            value=float(total_deposited), step=50.0,
            key="total_deposited",
        )
        if new_deposited != total_deposited and st.button("Einzahlung speichern"):
            result = client.save_setting("total_deposited", new_deposited)
            if result and result.get("ok"):
                st.success(f"Einzahlung auf ${new_deposited:,.2f} aktualisiert!")
                st.rerun()

    # ══════════════════════════════════════════════════════════════════
    # 5. BOT CONTROLS (collapsed)
    # ══════════════════════════════════════════════════════════════════
    with st.expander("Bot-Steuerung & Risk-Einstellungen", expanded=False):
        _render_bot_controls(client)

        config = client.get_config()
        _render_risk_controls(client, config)


# ======================================================================
# Bot Controls
# ======================================================================

def _render_bot_controls(client):
    """Circuit breaker + pause/resume."""
    cb = client.get_circuit_breaker()
    config = client.get_config()
    cb_config = config.get("circuit_breaker", {})
    max_losses = cb_config.get("max_consecutive_losses", 3)

    col1, col2 = st.columns(2)
    with col1:
        consecutive_losses = cb.get("consecutive_losses", 0)
        paused_until = cb.get("paused_until")
        is_cb_paused = False
        if paused_until:
            try:
                if datetime.fromisoformat(paused_until) > datetime.utcnow():
                    is_cb_paused = True
            except Exception:
                pass

        if is_cb_paused:
            st.error(f"CIRCUIT BREAKER AKTIV — Pausiert bis {paused_until[:16]}")
        else:
            st.success("Trading erlaubt")
        st.caption(f"Verluste in Folge: {consecutive_losses}/{max_losses}")

    with col2:
        if is_cb_paused and st.button("Circuit Breaker zurücksetzen"):
            result = client.reset_circuit_breaker()
            if result and result.get("ok"):
                st.success("Circuit Breaker zurückgesetzt!")
                st.rerun()

    status = client.get_status()
    bot_paused = status.get("bot_paused", False) if status else False

    col_p, col_r = st.columns(2)
    with col_p:
        if st.button("Bot PAUSIEREN", type="primary", disabled=bot_paused):
            result = client.pause_bot()
            if result and result.get("ok"):
                st.warning("Bot wurde pausiert!")
                st.rerun()
    with col_r:
        if st.button("Bot FORTSETZEN", disabled=not bot_paused):
            result = client.resume_bot()
            if result and result.get("ok"):
                st.success("Bot läuft wieder!")
                st.rerun()


# ======================================================================
# Risk Controls
# ======================================================================

def _render_risk_controls(client, config: dict):
    """Risk per trade controls."""
    st.subheader("Risk pro Trade")

    trading_cfg = config.get("trading", {})
    limits = trading_cfg.get("limits", {})
    capital = trading_cfg.get("capital_usd", 100.0)
    current_pct = limits.get("max_position_pct", 5)
    current_max_usd = capital * current_pct / 100
    min_edge = limits.get("min_edge", 0.03)
    max_daily_loss = limits.get("max_daily_loss_usd", 50.0)

    col_cap, col_pct, col_usd, col_edge = st.columns(4)
    with col_cap:
        st.metric("Kapital", f"${capital:.0f}")
    with col_pct:
        st.metric("Max Position", f"{current_pct}%")
    with col_usd:
        st.metric("Max pro Trade", f"${current_max_usd:.2f}")
    with col_edge:
        st.metric("Min Edge", f"{min_edge:.0%}")

    with st.expander("Anpassen", expanded=False):
        col_left, col_right = st.columns(2)

        with col_left:
            new_capital = st.number_input(
                "Trading-Kapital ($)",
                min_value=10.0, max_value=10000.0,
                value=float(capital), step=10.0,
                key="risk_capital",
            )
            new_pct = st.slider(
                "Max Position (%)",
                min_value=1, max_value=25, value=int(current_pct),
                key="risk_pct",
            )
            new_max_usd = new_capital * new_pct / 100
            st.caption(f"= max **${new_max_usd:.2f}** pro Trade")

        with col_right:
            new_min_edge = st.slider(
                "Min Edge (%)",
                min_value=1, max_value=20, value=int(min_edge * 100),
                key="risk_edge",
            )
            new_max_daily_loss = st.number_input(
                "Max Tagesverlust ($)",
                min_value=5.0, max_value=500.0,
                value=float(max_daily_loss), step=5.0,
                key="risk_daily_loss",
            )
            new_mode = st.selectbox(
                "Trading-Modus",
                ["paper", "semi-auto", "full-auto"],
                index=["paper", "semi-auto", "full-auto"].index(
                    trading_cfg.get("mode", "paper")
                ),
                key="risk_mode",
            )

        changed = (
            new_capital != capital
            or new_pct != current_pct
            or new_min_edge != int(min_edge * 100)
            or new_max_daily_loss != max_daily_loss
            or new_mode != trading_cfg.get("mode", "paper")
        )

        if changed and st.button("Speichern", type="primary", use_container_width=True):
            config["trading"]["capital_usd"] = new_capital
            config["trading"]["limits"]["max_position_pct"] = new_pct
            config["trading"]["limits"]["min_edge"] = new_min_edge / 100
            config["trading"]["limits"]["max_daily_loss_usd"] = new_max_daily_loss
            config["trading"]["mode"] = new_mode

            result = client.save_config(config)
            if result and result.get("ok"):
                st.success(f"Gespeichert! Modus: {new_mode}")
                st.rerun()
            else:
                st.error("Fehler beim Speichern.")


# ======================================================================
# Helper functions
# ======================================================================

def _calc_today_pnl(equity_curve: list, current_unrealized: float, current_realized: float) -> float:
    """Calculate today's PNL change from equity curve snapshots."""
    if not equity_curve:
        return 0.0
    today_str = datetime.utcnow().strftime("%Y-%m-%d")
    current_total = current_unrealized + current_realized
    # Find the earliest snapshot from today (or most recent before today)
    prev_total = 0.0
    for snap in equity_curve:
        snap_date = str(snap.get("snapshot_at", ""))[:10]
        snap_pnl = (snap.get("unrealized_pnl", 0) or 0) + (snap.get("realized_pnl", 0) or 0)
        if snap_date < today_str:
            prev_total = snap_pnl
        else:
            break
    return current_total - prev_total


def _filter_equity_curve(equity_curve: list, period: str) -> pd.DataFrame:
    """Filter equity curve data by time period and return DataFrame for charting."""
    if not equity_curve:
        return pd.DataFrame()

    now = datetime.utcnow()
    if period == "1D":
        cutoff = now - timedelta(days=1)
    elif period == "1W":
        cutoff = now - timedelta(weeks=1)
    elif period == "1M":
        cutoff = now - timedelta(days=30)
    else:
        cutoff = None

    rows = []
    for snap in equity_curve:
        snap_at = snap.get("snapshot_at", "")
        try:
            dt = datetime.fromisoformat(str(snap_at))
        except (ValueError, TypeError):
            continue
        if cutoff and dt < cutoff:
            continue
        total_pnl = (snap.get("unrealized_pnl", 0) or 0) + (snap.get("realized_pnl", 0) or 0)
        rows.append({"date": dt, "pnl": total_pnl})

    if not rows:
        return pd.DataFrame()

    return pd.DataFrame(rows)


def _build_equity_chart(df: pd.DataFrame, color: str):
    """Render a clean area chart without axis backgrounds."""
    import plotly.graph_objects as go

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=df["date"], y=df["pnl"],
        fill="tozeroy",
        line=dict(color=color, width=2),
        fillcolor=f"rgba({int(color[1:3], 16)}, {int(color[3:5], 16)}, {int(color[5:7], 16)}, 0.15)",
        hovertemplate="%{y:$.2f}<extra></extra>",
    ))
    fig.update_layout(
        height=90,
        margin=dict(l=0, r=0, t=0, b=0),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        xaxis=dict(visible=False),
        yaxis=dict(visible=False),
        showlegend=False,
        hovermode="x unified",
    )
    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
