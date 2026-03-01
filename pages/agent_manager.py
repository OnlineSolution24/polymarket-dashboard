"""
Agent-Team Manager — Read-only view of agents running on the bot.
Agent creation and configuration happens on the bot VPS.
All data loaded from Bot REST API.
"""

import json
import streamlit as st

from services.bot_api_client import get_bot_client


def render():
    st.header("Agent-Team Manager")

    client = get_bot_client()

    # --- Active Agents Overview ---
    st.subheader("Aktive Agents")

    agents = client.get_agents()

    if agents:
        for agent in agents:
            status_icon = "🟢" if agent.get("status") == "active" else "🔴"
            with st.expander(f"{status_icon} {agent.get('name', '?')} ({agent.get('role', '?')})", expanded=False):
                col1, col2, col3 = st.columns([2, 1, 1])

                with col1:
                    st.markdown(f"**Role:** {agent.get('role', '?')}")
                    st.markdown(f"**Status:** {agent.get('status', '?')}")
                    st.markdown(f"**Config:** `{agent.get('config_file', 'N/A')}`")

                with col2:
                    budget = agent.get("budget_used_today", 0) or 0
                    st.metric("Budget heute", f"${budget:.2f}")

                with col3:
                    try:
                        skills_raw = agent.get("skills", "[]")
                        skills = json.loads(skills_raw) if isinstance(skills_raw, str) else (skills_raw or [])
                    except Exception:
                        skills = []
                    st.markdown("**Skills:**")
                    for skill in skills:
                        st.caption(f"• {skill}")

                # Agent Logs
                if st.button("Logs anzeigen", key=f"logs_{agent.get('id', '')}"):
                    logs = client.get_logs(agent_id=agent["id"], limit=20)
                    if logs:
                        for log in logs:
                            st.caption(f"`{(log.get('created_at') or '')[:16]}` [{log.get('level', '?')}] {log.get('message', '')}")
                    else:
                        st.caption("Keine Logs vorhanden.")
    else:
        st.info("Keine Agents vom Bot gemeldet. Prüfe die Bot-Verbindung.")

    st.divider()

    st.caption("Agents werden auf dem Bot-VPS konfiguriert (agent_configs/*.yaml). Das Dashboard zeigt nur den Status.")
