"""
System Config — Live editor for bot platform configuration via REST API.
Changes are saved directly to the bot and take effect on next scheduler cycle.
"""

import streamlit as st
import yaml

from services.bot_api_client import get_bot_client


def render():
    st.header("System Konfiguration")

    client = get_bot_client()
    config = client.get_config()

    if not config:
        st.error("Bot nicht erreichbar. Konfiguration kann nicht geladen werden.")
        return

    # --- Quick Toggles ---
    st.subheader("Schnelleinstellungen")

    trading = config.get("trading", {})
    scheduler = config.get("scheduler", {})
    alerts_cfg = config.get("alerts", {})

    col1, col2, col3 = st.columns(3)

    with col1:
        st.markdown("**Trading**")
        mode_options = ["paper", "semi-auto", "full-auto"]
        current_mode = trading.get("mode", "paper")
        new_mode = st.selectbox(
            "Trading-Modus",
            mode_options,
            index=mode_options.index(current_mode) if current_mode in mode_options else 0,
        )

    with col2:
        st.markdown("**Scheduler Jobs**")
        agent_check = scheduler.get("agent_health_check", {})
        agent_check_enabled = st.checkbox(
            "Agent-Zyklen aktiv",
            value=agent_check.get("enabled", True),
        )
        agent_interval = st.number_input(
            "Agent-Intervall (Min)",
            min_value=5, max_value=120,
            value=agent_check.get("interval_minutes", 15),
            step=5,
        )

    with col3:
        st.markdown("**Alerts**")
        alerts_enabled = st.checkbox("Telegram Alerts", value=alerts_cfg.get("enabled", True))
        daily_summary = st.checkbox("Tages-Zusammenfassung", value=alerts_cfg.get("on_daily_summary", True))

    # More scheduler toggles
    st.divider()
    st.subheader("Scheduler Jobs")

    sched_col1, sched_col2, sched_col3 = st.columns(3)

    with sched_col1:
        market_cfg = scheduler.get("market_refresh", {})
        market_enabled = st.checkbox("Markt-Refresh", value=market_cfg.get("enabled", True))
        market_interval = st.number_input(
            "Markt-Intervall (Min)", min_value=5, max_value=120,
            value=market_cfg.get("interval_minutes", 30), step=5,
            key="market_interval",
        )

    with sched_col2:
        sentiment_cfg = scheduler.get("sentiment_update", {})
        sentiment_enabled = st.checkbox("Sentiment-Update", value=sentiment_cfg.get("enabled", True))
        sentiment_interval = st.number_input(
            "Sentiment-Intervall (Min)", min_value=15, max_value=240,
            value=sentiment_cfg.get("interval_minutes", 60), step=15,
            key="sentiment_interval",
        )

    with sched_col3:
        trader_cfg = scheduler.get("trader_cycle", {})
        trader_enabled = st.checkbox("Trader-Zyklus", value=trader_cfg.get("enabled", True))
        trader_interval = st.number_input(
            "Trader-Intervall (Min)", min_value=1, max_value=60,
            value=trader_cfg.get("interval_minutes", 5), step=1,
            key="trader_interval",
        )

    # --- Save Quick Settings ---
    if st.button("Einstellungen speichern", type="primary"):
        # Apply changes to config
        config.setdefault("trading", {})["mode"] = new_mode

        config.setdefault("scheduler", {}).setdefault("agent_health_check", {})["enabled"] = agent_check_enabled
        config["scheduler"]["agent_health_check"]["interval_minutes"] = agent_interval

        config.setdefault("scheduler", {}).setdefault("market_refresh", {})["enabled"] = market_enabled
        config["scheduler"]["market_refresh"]["interval_minutes"] = market_interval

        config.setdefault("scheduler", {}).setdefault("sentiment_update", {})["enabled"] = sentiment_enabled
        config["scheduler"]["sentiment_update"]["interval_minutes"] = sentiment_interval

        config.setdefault("scheduler", {}).setdefault("trader_cycle", {})["enabled"] = trader_enabled
        config["scheduler"]["trader_cycle"]["interval_minutes"] = trader_interval

        config.setdefault("alerts", {})["enabled"] = alerts_enabled
        config["alerts"]["on_daily_summary"] = daily_summary

        result = client.save_config(config)
        if result and result.get("ok"):
            st.success("Konfiguration gespeichert! Änderungen wirken beim nächsten Bot-Neustart.")
        else:
            st.error("Fehler beim Speichern.")

    st.divider()

    # --- Trading Limits ---
    st.subheader("Trading-Limits")
    limits = trading.get("limits", {})

    lc1, lc2, lc3, lc4 = st.columns(4)
    with lc1:
        st.metric("Kapital", f"${trading.get('capital_usd', 0):.2f}")
    with lc2:
        st.metric("Max Position", f"{limits.get('max_position_pct', 0)}%")
    with lc3:
        st.metric("Min Edge", f"{limits.get('min_edge', 0):.1%}")
    with lc4:
        st.metric("Max Tagesverlust", f"${limits.get('max_daily_loss_usd', 0):.2f}")

    st.divider()

    # --- Advanced: Full YAML Editor ---
    with st.expander("Erweitert: YAML Editor"):
        st.warning("Achtung: Fehlerhafte YAML-Syntax kann den Bot beeinträchtigen!")
        config_text = yaml.dump(config, default_flow_style=False, allow_unicode=True, sort_keys=False)

        edited = st.text_area(
            "platform_config.yaml",
            value=config_text,
            height=500,
            key="yaml_editor",
        )

        if st.button("YAML speichern", key="save_yaml"):
            try:
                parsed = yaml.safe_load(edited)
                if not isinstance(parsed, dict):
                    st.error("Ungültiges YAML: Root muss ein Dictionary sein.")
                else:
                    result = client.save_config(parsed)
                    if result and result.get("ok"):
                        st.success("YAML gespeichert!")
                    else:
                        st.error("Fehler beim Speichern.")
            except yaml.YAMLError as e:
                st.error(f"YAML Fehler: {e}")

    st.divider()
    st.caption("Änderungen werden beim nächsten Scheduler-Zyklus wirksam. Für sofortige Wirkung: Bot neu starten.")
