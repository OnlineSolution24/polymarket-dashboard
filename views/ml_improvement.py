"""
ML Self-Improvement — Placeholder page.
ML training runs on the Bot VPS. Dashboard shows summary info.
"""

import streamlit as st

from services.bot_api_client import get_bot_client


def render():
    st.header("ML Self-Improvement")

    st.info(
        "ML-Training und Modellverwaltung laufen direkt auf dem Bot-VPS. "
        "Das Dashboard zeigt eine Zusammenfassung der Trading-Performance."
    )

    client = get_bot_client()

    # --- Trade data summary ---
    stats = client.get_trade_stats()
    if stats and stats.get("total", 0) > 0:
        st.subheader("Trading-Performance")
        sc = st.columns(4)
        with sc[0]:
            st.metric("Abgeschlossene Trades", stats["total"])
        with sc[1]:
            wr = stats.get("wins", 0) / stats["total"] * 100 if stats["total"] else 0
            st.metric("Win Rate", f"{wr:.1f}%")
        with sc[2]:
            st.metric("Wins / Losses", f"{stats.get('wins', 0)} / {stats.get('losses', 0)}")
        with sc[3]:
            st.metric("Gesamt-PnL", f"${stats.get('total_pnl', 0):+.2f}")

        ready = stats["total"] >= 20
        st.progress(min(stats["total"] / 100, 1.0), text=f"{stats['total']}/100 Trades (ideal für ML)")

        if ready:
            st.success("Genug Daten für ML-Training vorhanden. Training läuft automatisch auf dem Bot.")
        else:
            st.warning(f"Mindestens 20 abgeschlossene Trades nötig für ML-Training ({stats['total']}/20).")
    else:
        st.warning("Noch keine abgeschlossenen Trades. ML-Training wird verfügbar sobald der Bot Trades ausführt.")

    st.divider()
    st.caption("ML-Modelle werden auf dem Bot-VPS trainiert und automatisch für Prognosen verwendet.")
