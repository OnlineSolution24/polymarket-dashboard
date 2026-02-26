"""
Tab 7: Suggestion System
Chief agent suggestions with Yes/No/Test user responses.
Approved suggestions trigger actions (new agents, config changes, alerts).
"""

import json
import streamlit as st
from datetime import datetime

from db import engine
from config import load_agent_configs, save_agent_config, AgentConfigYAML


def render():
    st.header("Vorschläge")

    # --- Manual Chief Analysis ---
    col_btn, col_info = st.columns([1, 2])
    with col_btn:
        if st.button("Chief-Analyse starten", type="primary"):
            _run_chief_analysis()
    with col_info:
        st.caption("Lässt den Chief Agent alle Daten analysieren und Vorschläge generieren.")

    st.divider()

    # --- Pending Suggestions ---
    pending = engine.query(
        "SELECT * FROM suggestions WHERE status = 'pending' ORDER BY created_at DESC"
    )

    if pending:
        st.subheader(f"Offene Vorschläge ({len(pending)})")

        for sugg in pending:
            type_icons = {
                "trade": "📈", "new_agent": "🤖", "config_change": "⚙️",
                "alert": "⚠️", "analysis": "🔍", "risk_adjustment": "🛡️",
                "source_change": "📰",
            }
            icon = type_icons.get(sugg["type"], "💡")

            with st.expander(f"{icon} {sugg['title']}", expanded=True):
                st.markdown(sugg["description"] or "")

                if sugg.get("payload"):
                    try:
                        payload = json.loads(sugg["payload"])
                        with st.popover("Details anzeigen"):
                            st.json(payload)
                    except Exception:
                        pass

                st.caption(f"Von: {sugg['agent_id']} | Typ: {sugg['type']} | {sugg['created_at'][:16]}")

                col1, col2, col3 = st.columns(3)
                with col1:
                    if st.button("Ja", key=f"yes_{sugg['id']}", type="primary"):
                        _respond_and_execute(sugg, "yes")
                        st.rerun()
                with col2:
                    if st.button("Nein", key=f"no_{sugg['id']}"):
                        _respond(sugg["id"], "no")
                        st.rerun()
                with col3:
                    if st.button("Testen", key=f"test_{sugg['id']}"):
                        _respond(sugg["id"], "test")
                        st.rerun()
    else:
        st.info("Keine offenen Vorschläge. Starte eine Chief-Analyse oder warte auf den nächsten Scheduler-Lauf.")

    st.divider()

    # --- Suggestion Statistics ---
    stats = engine.query_one("""
        SELECT COUNT(*) as total,
               SUM(CASE WHEN user_response='yes' THEN 1 ELSE 0 END) as approved,
               SUM(CASE WHEN user_response='no' THEN 1 ELSE 0 END) as rejected,
               SUM(CASE WHEN user_response='test' THEN 1 ELSE 0 END) as testing
        FROM suggestions WHERE status != 'pending'
    """)
    if stats and stats["total"] > 0:
        sc = st.columns(4)
        with sc[0]:
            st.metric("Total", stats["total"])
        with sc[1]:
            st.metric("Genehmigt", stats["approved"])
        with sc[2]:
            st.metric("Abgelehnt", stats["rejected"])
        with sc[3]:
            st.metric("Im Test", stats["testing"])

    # --- History ---
    st.subheader("Verlauf")

    filter_type = st.selectbox("Filtern nach Typ", ["Alle", "trade", "new_agent", "config_change", "analysis", "risk_adjustment"])

    if filter_type == "Alle":
        history = engine.query(
            "SELECT * FROM suggestions WHERE status != 'pending' ORDER BY resolved_at DESC LIMIT 50"
        )
    else:
        history = engine.query(
            "SELECT * FROM suggestions WHERE status != 'pending' AND type = ? ORDER BY resolved_at DESC LIMIT 50",
            (filter_type,),
        )

    if history:
        for sugg in history:
            response_icons = {"yes": "✅", "no": "❌", "test": "🧪"}
            icon = response_icons.get(sugg.get("user_response", ""), "•")
            ts = (sugg.get("resolved_at") or "")[:16]
            st.caption(f"{icon} **{sugg['title']}** — {sugg.get('user_response', '?')} ({ts})")
    else:
        st.caption("Kein Verlauf vorhanden.")


def _run_chief_analysis():
    """Manually trigger chief agent analysis cycle."""
    try:
        from config import AppConfig, load_agent_configs as lac
        from agents.agent_factory import create_agent
        from services.telegram_bridge import get_bridge

        config = AppConfig.from_env()
        bridge = get_bridge(config)

        # Find chief config
        chief_cfg = None
        for cfg in lac():
            if cfg.role == "chief":
                chief_cfg = cfg
                break

        if not chief_cfg:
            st.error("Kein Chief Agent konfiguriert. Erstelle chief.yaml in agent_configs/")
            return

        with st.spinner("Chief Agent analysiert..."):
            agent = create_agent(chief_cfg, bridge)
            result = agent.run_cycle()

        if result["ok"]:
            st.success("Analyse abgeschlossen! Neue Vorschläge wurden erstellt.")
        else:
            st.warning(f"Analyse mit Hinweis: {result['summary']}")

    except Exception as e:
        st.error(f"Fehler: {e}")


def _respond(suggestion_id: int, response: str):
    """Record user response."""
    status_map = {"yes": "approved", "no": "rejected", "test": "testing"}
    engine.execute(
        "UPDATE suggestions SET status = ?, user_response = ?, resolved_at = ? WHERE id = ?",
        (status_map.get(response, "pending"), response, datetime.utcnow().isoformat(), suggestion_id),
    )


def _respond_and_execute(sugg: dict, response: str):
    """Respond yes and execute the suggestion action."""
    _respond(sugg["id"], response)

    if not sugg.get("payload"):
        return

    try:
        payload = json.loads(sugg["payload"])
    except Exception:
        return

    # Execute based on suggestion type
    sugg_type = sugg["type"]

    if sugg_type == "new_agent" and "agent_config" in payload:
        _create_agent_from_suggestion(payload["agent_config"])

    elif sugg_type == "config_change" and "changes" in payload:
        _apply_config_changes(payload["changes"])


def _create_agent_from_suggestion(agent_data: dict):
    """Create a new agent from a suggestion payload."""
    try:
        agent_cfg = AgentConfigYAML(
            id=agent_data.get("id", f"auto_{datetime.utcnow().strftime('%Y%m%d%H%M')}"),
            name=agent_data.get("name", "Auto-Created Agent"),
            role=agent_data.get("role", "custom"),
            persona=agent_data.get("persona", ""),
            skills=agent_data.get("skills", []),
            schedule=agent_data.get("schedule", "*/60 * * * *"),
            budget_daily_usd=agent_data.get("budget_daily_usd", 0.50),
            model=agent_data.get("model", "haiku"),
            enabled=True,
        )
        save_agent_config(agent_cfg)
        st.success(f"Agent '{agent_cfg.name}' erstellt!")
    except Exception as e:
        st.error(f"Agent-Erstellung fehlgeschlagen: {e}")


def _apply_config_changes(changes: dict):
    """Apply configuration changes from a suggestion."""
    try:
        from config import load_platform_config, save_platform_config
        config = load_platform_config()

        for key_path, value in changes.items():
            keys = key_path.split(".")
            target = config
            for k in keys[:-1]:
                target = target.setdefault(k, {})
            target[keys[-1]] = value

        save_platform_config(config)
        st.success("Konfiguration aktualisiert!")
    except Exception as e:
        st.error(f"Config-Änderung fehlgeschlagen: {e}")
