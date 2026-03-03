"""
Execution Control — Read-only trade monitoring + emergency controls.
Trades are executed by the autonomous bot. Dashboard shows history
and provides emergency pause/resume and circuit breaker reset.
All data loaded from Bot REST API.
"""

import streamlit as st
from datetime import datetime

from services.bot_api_client import get_bot_client
from components.tables import trades_table
from components.charts import pnl_chart


def render():
    st.header("Execution Control")

    client = get_bot_client()

    # --- Circuit Breaker Status ---
    cb = client.get_circuit_breaker()
    config = client.get_config()
    cb_config = config.get("circuit_breaker", {})
    max_losses = cb_config.get("max_consecutive_losses", 3)

    col1, col2 = st.columns(2)
    with col1:
        consecutive_losses = cb.get("consecutive_losses", 0)
        paused_until = cb.get("paused_until")
        is_paused = False
        if paused_until:
            try:
                if datetime.fromisoformat(paused_until) > datetime.utcnow():
                    is_paused = True
            except Exception:
                pass

        if is_paused:
            st.error(f"CIRCUIT BREAKER AKTIV — Pausiert bis {paused_until[:16]}")
        else:
            st.success("Trading erlaubt")
        st.caption(f"Verluste in Folge: {consecutive_losses}/{max_losses}")

    with col2:
        if is_paused and st.button("Circuit Breaker zurücksetzen"):
            result = client.reset_circuit_breaker()
            if result and result.get("ok"):
                st.success("Circuit Breaker zurückgesetzt!")
                st.rerun()
            else:
                st.error("Fehler beim Zurücksetzen.")

    st.divider()

    # --- Emergency Bot Controls ---
    st.subheader("Bot-Steuerung")
    status = client.get_status()
    bot_paused = status.get("bot_paused", False) if status else False

    col_pause, col_resume = st.columns(2)
    with col_pause:
        if st.button("Bot PAUSIEREN", type="primary", disabled=bot_paused):
            result = client.pause_bot()
            if result and result.get("ok"):
                st.warning("Bot wurde pausiert!")
                st.rerun()
    with col_resume:
        if st.button("Bot FORTSETZEN", disabled=not bot_paused):
            result = client.resume_bot()
            if result and result.get("ok"):
                st.success("Bot läuft wieder!")
                st.rerun()

    st.divider()

    # --- Open Trades ---
    st.subheader("Offene Trades")
    open_trades = client.get_trades(limit=50, status="executed")
    open_trades = [t for t in open_trades if t.get("result") in (None, "open")]

    if open_trades:
        for trade in open_trades:
            with st.expander(f"{trade.get('side', '?')} ${trade.get('amount_usd', 0):.2f} — {trade.get('market_question') or trade.get('market_id', '?')}"):
                st.caption(f"Erstellt: {(trade.get('created_at') or '')[:16]} | Status: {trade.get('status', '?')}")
    else:
        st.caption("Keine offenen Trades.")

    st.divider()

    # --- Trade Statistics ---
    st.subheader("Trade Verlauf")
    stats = client.get_trade_stats()
    if stats and stats.get("total", 0) > 0:
        sc = st.columns(4)
        with sc[0]:
            st.metric("Trades", stats["total"])
        with sc[1]:
            wr = stats.get("wins", 0) / stats["total"] * 100
            st.metric("Win Rate", f"{wr:.0f}%")
        with sc[2]:
            st.metric("W / L", f"{stats.get('wins', 0)} / {stats.get('losses', 0)}")
        with sc[3]:
            st.metric("PnL", f"${stats.get('total_pnl', 0):+.2f}")

    trades = client.get_trades(limit=50)
    if trades:
        done = [t for t in trades if t.get("pnl") is not None]
        if done:
            st.plotly_chart(pnl_chart(done), use_container_width=True)
        trades_table(trades)
    else:
        st.info("Noch keine Trades.")
