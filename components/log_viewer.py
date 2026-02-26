"""
Scrollable log viewer component for agent logs.
"""

import streamlit as st
from db import engine


def render_log_viewer(agent_id: str | None = None, limit: int = 100) -> None:
    """Display agent logs in a scrollable container."""
    if agent_id:
        logs = engine.query(
            "SELECT * FROM agent_logs WHERE agent_id = ? ORDER BY created_at DESC LIMIT ?",
            (agent_id, limit),
        )
    else:
        logs = engine.query(
            "SELECT * FROM agent_logs ORDER BY created_at DESC LIMIT ?",
            (limit,),
        )

    if not logs:
        st.info("Keine Logs vorhanden.")
        return

    level_colors = {
        "debug": "gray",
        "info": "blue",
        "warn": "orange",
        "error": "red",
    }

    log_html = ['<div style="max-height: 400px; overflow-y: auto; font-family: monospace; font-size: 13px;">']
    for log in logs:
        color = level_colors.get(log["level"], "white")
        timestamp = log["created_at"][:19] if log["created_at"] else ""
        agent = log["agent_id"][:12] if log["agent_id"] else "system"
        log_html.append(
            f'<div style="padding: 2px 0; border-bottom: 1px solid #333;">'
            f'<span style="color: #888;">{timestamp}</span> '
            f'<span style="color: {color}; font-weight: bold;">[{log["level"].upper()}]</span> '
            f'<span style="color: #aaa;">{agent}</span> '
            f'{log["message"]}</div>'
        )
    log_html.append("</div>")

    st.markdown("".join(log_html), unsafe_allow_html=True)
