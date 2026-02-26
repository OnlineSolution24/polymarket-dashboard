"""
Reusable metric cards and status indicators for the dashboard.
"""

import streamlit as st


def kpi_row(metrics: list[dict]) -> None:
    """
    Display a row of KPI metric cards.
    Each metric: {"label": str, "value": str|number, "delta": str|None, "delta_color": str|None}
    """
    cols = st.columns(len(metrics))
    for col, m in zip(cols, metrics):
        with col:
            st.metric(
                label=m["label"],
                value=m["value"],
                delta=m.get("delta"),
                delta_color=m.get("delta_color", "normal"),
            )


def status_badge(label: str, status: str) -> None:
    """Display a colored status badge."""
    colors = {
        "active": "🟢",
        "paused": "🟡",
        "error": "🔴",
        "archived": "⚪",
        "ok": "🟢",
        "warning": "🟡",
        "critical": "🔴",
    }
    icon = colors.get(status, "⚪")
    st.markdown(f"{icon} **{label}**: {status}")


def info_card(title: str, content: str, color: str = "blue") -> None:
    """Display an info card with colored border."""
    border_colors = {
        "blue": "#1f77b4",
        "green": "#2ca02c",
        "red": "#d62728",
        "orange": "#ff7f0e",
        "gray": "#7f7f7f",
    }
    border = border_colors.get(color, "#1f77b4")
    st.markdown(
        f"""<div style="border-left: 4px solid {border}; padding: 10px 15px;
        margin: 5px 0; background: rgba(255,255,255,0.05); border-radius: 4px;">
        <strong>{title}</strong><br>{content}</div>""",
        unsafe_allow_html=True,
    )
