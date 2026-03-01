"""
System Config — Read-only view of bot platform configuration via REST API.
Config editing happens directly on the bot VPS.
"""

import streamlit as st
import yaml

from services.bot_api_client import get_bot_client


def render():
    st.header("System Konfiguration")

    st.markdown("""
    Zeigt die aktuelle Plattform-Konfiguration des Bots.
    Änderungen werden direkt auf dem Bot-VPS in `platform_config.yaml` vorgenommen.
    """)

    client = get_bot_client()

    # --- Platform Config (read-only) ---
    st.subheader("Bot Konfiguration")

    config = client.get_config()
    if config:
        config_text = yaml.dump(config, default_flow_style=False, allow_unicode=True, sort_keys=False)
        st.code(config_text, language="yaml")
    else:
        st.error("Konfiguration konnte nicht geladen werden. Bot nicht erreichbar?")

    st.divider()

    # --- Trading Config Highlights ---
    if config:
        trading = config.get("trading", {})
        if trading:
            st.subheader("Trading-Einstellungen")
            tc = st.columns(4)
            with tc[0]:
                st.metric("Modus", trading.get("mode", "?"))
            with tc[1]:
                st.metric("Kapital", f"${trading.get('capital_usd', 0):.2f}")
            with tc[2]:
                limits = trading.get("limits", {})
                st.metric("Max Position", f"{limits.get('max_position_pct', 0)}%")
            with tc[3]:
                st.metric("Min Edge", f"{limits.get('min_edge', 0):.1%}")

        cb = config.get("circuit_breaker", {})
        if cb:
            st.subheader("Circuit Breaker")
            cc = st.columns(3)
            with cc[0]:
                st.metric("Max Verluste", cb.get("max_consecutive_losses", "?"))
            with cc[1]:
                st.metric("Pause (Stunden)", cb.get("pause_hours", "?"))
            with cc[2]:
                st.metric("Trigger", cb.get("trigger", "consecutive_losses"))

        budgets = config.get("budgets", {})
        if budgets:
            st.subheader("Budgets")
            bc = st.columns(3)
            with bc[0]:
                st.metric("Tages-Limit", f"${budgets.get('daily_limit_usd', 0):.2f}")
            with bc[1]:
                st.metric("Monats-Limit", f"${budgets.get('monthly_total_usd', 0):.2f}")
            with bc[2]:
                st.metric("Pro Agent/Tag", f"${budgets.get('per_agent_daily_usd', 0):.2f}")

    st.divider()
    st.caption("Konfiguration bearbeiten: `nano /opt/polymarket-bot/platform_config.yaml` auf dem Bot-VPS")
