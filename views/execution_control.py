"""
Portfolio & Performance — Live positions, closed trades, and real PnL
based on deposits vs current portfolio value.
Bot controls (pause/resume, circuit breaker, risk settings) at the bottom.
"""

import streamlit as st
import pandas as pd
from datetime import datetime

from services.bot_api_client import get_bot_client


def render():
    st.header("Portfolio & Performance")

    client = get_bot_client()
    perf = client.get_performance()

    total_deposited = perf.get("total_deposited", 0)
    positions_value = perf.get("open_positions_value", 0)
    positions_cost = perf.get("open_positions_cost", 0)
    unrealized_pnl = perf.get("unrealized_pnl", 0)

    # ══════════════════════════════════════════════════════════════════
    # 1. PORTFOLIO OVERVIEW
    # ══════════════════════════════════════════════════════════════════
    cols = st.columns(4)
    with cols[0]:
        st.metric("Eingezahlt", f"${total_deposited:,.2f}")
    with cols[1]:
        st.metric("Positionen Wert", f"${positions_value:,.2f}")
    with cols[2]:
        st.metric("Investiert (Einsatz)", f"${positions_cost:,.2f}")
    with cols[3]:
        delta_color = "normal" if unrealized_pnl >= 0 else "inverse"
        st.metric("Unrealisierter PnL",
                   f"${unrealized_pnl:+.2f}",
                   delta=f"{unrealized_pnl:+.2f}",
                   delta_color=delta_color)

    # Hint about Polymarket PnL
    if total_deposited > 0:
        st.caption(
            f"Dein realer Gesamt-PnL ergibt sich aus: "
            f"**Portfolio-Wert (Polymarket)** − **Eingezahlt** (${total_deposited:,.2f}). "
            f"Prüfe deinen aktuellen Portfolio-Wert auf Polymarket."
        )

    st.divider()

    # ══════════════════════════════════════════════════════════════════
    # 2. OFFENE POSITIONEN
    # ══════════════════════════════════════════════════════════════════
    st.subheader("Offene Positionen")
    positions = client.get_open_positions()

    if positions:
        df = pd.DataFrame(positions)
        df_display = df[["market_question", "side", "entry_price", "current_price",
                         "shares", "cost_basis", "current_value", "unrealized_pnl", "pnl_pct"]].copy()
        df_display.columns = ["Markt", "Seite", "Einstieg", "Aktuell",
                              "Shares", "Einsatz", "Wert", "PnL $", "PnL %"]

        df_display["Einstieg"] = df_display["Einstieg"].apply(lambda x: f"{x:.3f}")
        df_display["Aktuell"] = df_display["Aktuell"].apply(lambda x: f"{x:.3f}")
        df_display["Einsatz"] = df_display["Einsatz"].apply(lambda x: f"${x:.2f}")
        df_display["Wert"] = df_display["Wert"].apply(lambda x: f"${x:.2f}")
        df_display["PnL $"] = df_display["PnL $"].apply(lambda x: f"${x:+.2f}")
        df_display["PnL %"] = df_display["PnL %"].apply(lambda x: f"{x:+.1f}%")
        df_display["Markt"] = df_display["Markt"].str[:55]

        st.dataframe(df_display, use_container_width=True, hide_index=True)
    else:
        st.caption("Keine offenen Positionen.")

    st.divider()

    # ══════════════════════════════════════════════════════════════════
    # 3. ABGESCHLOSSENE TRADES
    # ══════════════════════════════════════════════════════════════════
    st.subheader("Abgeschlossene Trades")
    closed = client.get_closed_trades()

    if closed:
        total_markets = perf.get("total_markets", 0)
        wins = perf.get("wins", 0)
        losses = perf.get("losses", 0)

        sc = st.columns(3)
        with sc[0]:
            st.metric("Märkte", total_markets)
        with sc[1]:
            wr = (wins / total_markets * 100) if total_markets > 0 else 0
            st.metric("Win Rate", f"{wr:.0f}%")
        with sc[2]:
            st.metric("W / L", f"{wins} / {losses}")

        df_closed = pd.DataFrame(closed)
        cols_to_show = ["market_question", "side", "entry_price", "result", "executed_at"]
        if "realized_pnl" in df_closed.columns:
            cols_to_show.insert(4, "realized_pnl")

        df_show = df_closed[[c for c in cols_to_show if c in df_closed.columns]].copy()
        rename = {"market_question": "Markt", "side": "Seite", "entry_price": "Einstieg",
                  "result": "Ergebnis", "realized_pnl": "PnL $", "executed_at": "Datum"}
        df_show.columns = [rename.get(c, c) for c in df_show.columns]

        if "Einstieg" in df_show.columns:
            df_show["Einstieg"] = df_show["Einstieg"].apply(
                lambda x: f"{x:.3f}" if x else "-")
        if "PnL $" in df_show.columns:
            df_show["PnL $"] = df_show["PnL $"].apply(
                lambda x: f"${x:+.2f}" if x else "-")
        if "Datum" in df_show.columns:
            df_show["Datum"] = df_show["Datum"].apply(
                lambda x: x[:16] if x else "-")
        if "Markt" in df_show.columns:
            df_show["Markt"] = df_show["Markt"].str[:55]
        if "Ergebnis" in df_show.columns:
            df_show["Ergebnis"] = df_show["Ergebnis"].str.upper()

        st.dataframe(df_show, use_container_width=True, hide_index=True)
    else:
        st.caption("Noch keine abgeschlossenen Trades.")

    st.divider()

    # ══════════════════════════════════════════════════════════════════
    # 4. EINZAHLUNG BEARBEITEN
    # ══════════════════════════════════════════════════════════════════
    with st.expander("Einzahlung anpassen", expanded=False):
        new_deposited = st.number_input(
            "Gesamt eingezahlt ($)",
            min_value=0.0, max_value=100000.0,
            value=float(total_deposited), step=50.0,
            key="total_deposited",
        )
        if new_deposited != total_deposited and st.button("Einzahlung speichern"):
            config = client.get_config()
            config.setdefault("trading", {})["total_deposited"] = new_deposited
            result = client.save_config(config)
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
