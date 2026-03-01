"""
Scrollable log viewer component for agent logs.
Accepts pre-fetched log data (no direct DB access).
"""

import streamlit as st


def render_log_viewer(logs: list[dict] | None = None, agent_id: str | None = None, limit: int = 100) -> None:
    """Display agent logs in a scrollable container.

    Args:
        logs: Pre-fetched log entries. If None, fetches from bot API.
        agent_id: Optional filter (only used if logs is None).
        limit: Max entries (only used if logs is None).
    """
    if logs is None:
        from services.bot_api_client import get_bot_client
        client = get_bot_client()
        logs = client.get_logs(agent_id=agent_id, limit=limit)

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
        color = level_colors.get(log.get("level", ""), "white")
        timestamp = (log.get("created_at") or "")[:19]
        agent = (log.get("agent_id") or "system")[:12]
        log_html.append(
            f'<div style="padding: 2px 0; border-bottom: 1px solid #333;">'
            f'<span style="color: #888;">{timestamp}</span> '
            f'<span style="color: {color}; font-weight: bold;">[{(log.get("level") or "?").upper()}]</span> '
            f'<span style="color: #aaa;">{agent}</span> '
            f'{log.get("message", "")}</div>'
        )
    log_html.append("</div>")

    st.markdown("".join(log_html), unsafe_allow_html=True)
