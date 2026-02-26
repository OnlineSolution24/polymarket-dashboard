"""
Tab 2: Agent-Team Manager
Create, configure, and manage agents from YAML configs.
"""

import streamlit as st
import json
from datetime import datetime

from config import load_agent_configs, save_agent_config, AgentConfigYAML, AGENT_CONFIGS_DIR
from db import engine


def render():
    st.header("Agent-Team Manager")

    # --- Load configs from YAML ---
    yaml_agents = load_agent_configs()

    # --- Sync YAML → DB ---
    _sync_agents_to_db(yaml_agents)

    # --- Active Agents Overview ---
    st.subheader("Aktive Agents")

    db_agents = engine.query("SELECT * FROM agents ORDER BY created_at")

    if db_agents:
        for agent in db_agents:
            with st.expander(f"{'🟢' if agent['status'] == 'active' else '🔴'} {agent['name']} ({agent['role']})", expanded=False):
                col1, col2, col3 = st.columns([2, 1, 1])

                with col1:
                    st.markdown(f"**Role:** {agent['role']}")
                    st.markdown(f"**Status:** {agent['status']}")
                    st.markdown(f"**Config:** `{agent.get('config_file', 'N/A')}`")

                with col2:
                    budget = agent.get("budget_used_today", 0) or 0
                    st.metric("Budget heute", f"${budget:.2f}")

                with col3:
                    skills = json.loads(agent.get("skills", "[]"))
                    st.markdown("**Skills:**")
                    for skill in skills:
                        st.caption(f"• {skill}")

                # Actions
                action_col1, action_col2 = st.columns(2)
                with action_col1:
                    new_status = "paused" if agent["status"] == "active" else "active"
                    if st.button(f"{'Pausieren' if agent['status'] == 'active' else 'Aktivieren'}", key=f"toggle_{agent['id']}"):
                        engine.execute(
                            "UPDATE agents SET status = ?, updated_at = ? WHERE id = ?",
                            (new_status, datetime.utcnow().isoformat(), agent["id"]),
                        )
                        st.rerun()

                with action_col2:
                    if st.button("Logs anzeigen", key=f"logs_{agent['id']}"):
                        logs = engine.query(
                            "SELECT * FROM agent_logs WHERE agent_id = ? ORDER BY created_at DESC LIMIT 20",
                            (agent["id"],),
                        )
                        for log in logs:
                            st.caption(f"`{log['created_at'][:16]}` [{log['level']}] {log['message']}")
    else:
        st.info("Keine Agents in der Datenbank. Agents werden aus `agent_configs/` geladen.")

    st.divider()

    # --- Create New Agent ---
    st.subheader("Neuen Agent erstellen")

    with st.form("new_agent_form"):
        agent_id = st.text_input("Agent ID (wird Dateiname)", placeholder="sport_observer")
        name = st.text_input("Name", placeholder="Sport Market Observer")
        role = st.selectbox("Rolle", ["custom", "observer", "analyst", "risk_manager", "trader", "backtester"])
        persona = st.text_area("Persona / System Prompt", height=150,
                               placeholder="Beschreibe die Persönlichkeit und Aufgaben des Agents...")
        skills_input = st.text_input("Skills (kommagetrennt)", placeholder="market_scanning, analysis")
        schedule = st.text_input("Schedule (Cron)", value="*/60 * * * *")
        budget = st.number_input("Tagesbudget (USD)", min_value=0.0, max_value=10.0, value=0.50, step=0.10)
        model = st.selectbox("LLM Modell", ["haiku", "claude-sonnet", "gemini-flash", "claude-opus"])
        enabled = st.checkbox("Sofort aktivieren", value=True)

        submitted = st.form_submit_button("Agent erstellen", type="primary")

        if submitted and agent_id and name:
            skills = [s.strip() for s in skills_input.split(",") if s.strip()]
            new_agent = AgentConfigYAML(
                id=agent_id,
                name=name,
                role=role,
                persona=persona,
                skills=skills,
                schedule=schedule,
                budget_daily_usd=budget,
                model=model,
                enabled=enabled,
            )
            save_agent_config(new_agent)
            _sync_single_agent(new_agent)
            st.success(f"Agent '{name}' erstellt! Config: `agent_configs/{agent_id}.yaml`")
            st.rerun()
        elif submitted:
            st.error("Agent ID und Name sind Pflichtfelder.")

    st.divider()

    # --- YAML Files Overview ---
    st.subheader("Agent Config Dateien")
    for cfg in yaml_agents:
        status_icon = "🟢" if cfg.enabled else "⚪"
        st.caption(f"{status_icon} `agent_configs/{cfg.id}.yaml` — {cfg.name} ({cfg.role}) — Budget: ${cfg.budget_daily_usd}/Tag")


def _sync_agents_to_db(yaml_agents: list[AgentConfigYAML]) -> None:
    """Ensure all YAML agent configs exist in the DB."""
    for cfg in yaml_agents:
        _sync_single_agent(cfg)


def _sync_single_agent(cfg: AgentConfigYAML) -> None:
    """Sync a single agent config to DB."""
    existing = engine.query_one("SELECT id FROM agents WHERE id = ?", (cfg.id,))
    if not existing:
        engine.execute(
            """INSERT INTO agents (id, name, role, config_file, persona, skills, status)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (cfg.id, cfg.name, cfg.role, f"agent_configs/{cfg.id}.yaml",
             cfg.persona, json.dumps(cfg.skills),
             "active" if cfg.enabled else "paused"),
        )
