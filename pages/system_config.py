"""
Tab 9: System Config
Live YAML config editor for platform_config.yaml and agent configs.
"""

import streamlit as st
import yaml

from config import PLATFORM_CONFIG_PATH, AGENT_CONFIGS_DIR, load_platform_config, save_platform_config


def render():
    st.header("System Konfiguration")

    st.markdown("""
    Bearbeite die Plattform-Konfiguration und Agent-Configs live.
    Änderungen werden sofort gespeichert und beim nächsten Scheduler-Lauf wirksam.
    """)

    # --- Platform Config ---
    st.subheader("platform_config.yaml")

    current_config = load_platform_config()
    config_text = yaml.dump(current_config, default_flow_style=False, allow_unicode=True, sort_keys=False)

    edited_config = st.text_area(
        "Konfiguration bearbeiten",
        value=config_text,
        height=500,
        key="platform_config_editor",
    )

    if st.button("Speichern", type="primary", key="save_platform"):
        try:
            parsed = yaml.safe_load(edited_config)
            if not isinstance(parsed, dict):
                st.error("Ungültiges YAML: Root muss ein Dictionary sein.")
            else:
                save_platform_config(parsed)
                st.success("platform_config.yaml gespeichert!")
        except yaml.YAMLError as e:
            st.error(f"YAML Fehler: {e}")

    st.divider()

    # --- Agent Configs ---
    st.subheader("Agent Configs")

    yaml_files = sorted(AGENT_CONFIGS_DIR.glob("*.yaml"))
    yaml_files = [f for f in yaml_files if not f.stem.startswith("_")]

    if yaml_files:
        selected_file = st.selectbox(
            "Agent Config auswählen",
            options=[f.name for f in yaml_files],
        )

        if selected_file:
            filepath = AGENT_CONFIGS_DIR / selected_file
            with open(filepath, "r", encoding="utf-8") as f:
                agent_text = f.read()

            edited_agent = st.text_area(
                f"agent_configs/{selected_file}",
                value=agent_text,
                height=300,
                key=f"agent_config_{selected_file}",
            )

            if st.button("Agent Config speichern", key=f"save_agent_{selected_file}"):
                try:
                    parsed = yaml.safe_load(edited_agent)
                    if not isinstance(parsed, dict):
                        st.error("Ungültiges YAML.")
                    else:
                        with open(filepath, "w", encoding="utf-8") as f:
                            f.write(edited_agent)
                        st.success(f"{selected_file} gespeichert!")
                except yaml.YAMLError as e:
                    st.error(f"YAML Fehler: {e}")
    else:
        st.info("Keine Agent-Configs gefunden.")

    st.divider()

    # --- Quick Info ---
    st.subheader("System Info")
    st.markdown(f"**Config Pfad:** `{PLATFORM_CONFIG_PATH}`")
    st.markdown(f"**Agent Configs:** `{AGENT_CONFIGS_DIR}/`")
    st.markdown(f"**Anzahl Agent Configs:** {len(yaml_files)}")
