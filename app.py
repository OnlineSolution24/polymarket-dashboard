"""
Polymarket Agent Dashboard - Main Entrypoint
Monitoring-only dashboard that reads data from the Trading Bot REST API.
Run: streamlit run app.py
"""

import streamlit as st

# --- Page Config (must be first Streamlit call) ---
st.set_page_config(
    page_title="Polymarket Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

from config import AppConfig
from components.auth_guard import require_auth

# --- Dark Theme Custom CSS ---
st.markdown("""
<style>
    /* === Global === */
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');

    html, body, [class*="css"] {
        font-family: 'Inter', sans-serif;
    }

    .stApp {
        background: linear-gradient(135deg, #0E1117 0%, #151B28 50%, #0E1117 100%);
    }

    /* === Sidebar === */
    section[data-testid="stSidebar"] {
        background: linear-gradient(180deg, #131927 0%, #0D1220 100%);
        border-right: 1px solid rgba(0, 212, 170, 0.15);
    }

    section[data-testid="stSidebar"] .stMarkdown p {
        color: #8892A4;
    }

    /* === Navigation items === */
    section[data-testid="stSidebar"] a[data-testid="stSidebarNavLink"] {
        border-radius: 8px;
        margin: 2px 8px;
        transition: all 0.2s ease;
    }

    section[data-testid="stSidebar"] a[data-testid="stSidebarNavLink"]:hover {
        background: rgba(0, 212, 170, 0.1);
    }

    section[data-testid="stSidebar"] a[data-testid="stSidebarNavLink"][aria-current="page"] {
        background: rgba(0, 212, 170, 0.15);
        border-left: 3px solid #00D4AA;
    }

    /* === Cards / Containers === */
    div[data-testid="stExpander"],
    div[data-testid="stForm"],
    div[data-testid="column"] > div {
        border-radius: 12px;
    }

    div[data-testid="stMetric"] {
        background: linear-gradient(135deg, #1A1F2E 0%, #1E2538 100%);
        border: 1px solid rgba(0, 212, 170, 0.12);
        border-radius: 12px;
        padding: 16px 20px;
        box-shadow: 0 4px 16px rgba(0, 0, 0, 0.25);
        transition: transform 0.2s ease, box-shadow 0.2s ease;
    }

    div[data-testid="stMetric"]:hover {
        transform: translateY(-2px);
        box-shadow: 0 8px 24px rgba(0, 212, 170, 0.1);
        border-color: rgba(0, 212, 170, 0.3);
    }

    div[data-testid="stMetric"] label {
        color: #8892A4 !important;
        font-weight: 500;
        text-transform: uppercase;
        font-size: 0.75rem;
        letter-spacing: 0.05em;
    }

    div[data-testid="stMetric"] [data-testid="stMetricValue"] {
        color: #E8ECF1 !important;
        font-weight: 700;
    }

    div[data-testid="stMetric"] [data-testid="stMetricDelta"] svg {
        display: inline;
    }

    /* === Positive / Negative deltas === */
    [data-testid="stMetricDelta"] > div:has(svg[viewBox="0 0 8 8"]) {
        color: #00D4AA !important;
    }

    /* === Buttons === */
    .stButton > button {
        background: linear-gradient(135deg, #00D4AA 0%, #00B894 100%);
        color: #0E1117;
        border: none;
        border-radius: 8px;
        font-weight: 600;
        padding: 8px 24px;
        transition: all 0.2s ease;
        text-transform: none;
    }

    .stButton > button:hover {
        background: linear-gradient(135deg, #00E8BC 0%, #00D4AA 100%);
        box-shadow: 0 4px 16px rgba(0, 212, 170, 0.3);
        transform: translateY(-1px);
    }

    .stButton > button:active {
        transform: translateY(0px);
    }

    /* Secondary buttons */
    .stButton > button[kind="secondary"] {
        background: transparent;
        border: 1px solid rgba(0, 212, 170, 0.3);
        color: #00D4AA;
    }

    .stButton > button[kind="secondary"]:hover {
        background: rgba(0, 212, 170, 0.1);
    }

    /* === Tabs === */
    .stTabs [data-baseweb="tab-list"] {
        gap: 0px;
        background: #1A1F2E;
        border-radius: 10px;
        padding: 4px;
    }

    .stTabs [data-baseweb="tab"] {
        border-radius: 8px;
        padding: 8px 20px;
        color: #8892A4;
        font-weight: 500;
    }

    .stTabs [aria-selected="true"] {
        background: rgba(0, 212, 170, 0.15) !important;
        color: #00D4AA !important;
    }

    /* === Tables === */
    .stDataFrame {
        border-radius: 10px;
        overflow: hidden;
    }

    .stDataFrame [data-testid="stDataFrameResizable"] {
        border: 1px solid rgba(0, 212, 170, 0.12);
        border-radius: 10px;
    }

    /* === Inputs === */
    .stTextInput > div > div,
    .stNumberInput > div > div,
    .stSelectbox > div > div,
    .stTextArea > div > div {
        background: #1A1F2E;
        border: 1px solid rgba(0, 212, 170, 0.15);
        border-radius: 8px;
        color: #E0E0E0;
    }

    .stTextInput > div > div:focus-within,
    .stNumberInput > div > div:focus-within,
    .stSelectbox > div > div:focus-within,
    .stTextArea > div > div:focus-within {
        border-color: #00D4AA;
        box-shadow: 0 0 0 2px rgba(0, 212, 170, 0.15);
    }

    /* === Success / Error / Warning boxes === */
    .stAlert [data-testid="stNotificationContentSuccess"] {
        background: rgba(0, 212, 170, 0.1);
        border-left: 4px solid #00D4AA;
    }

    .stAlert [data-testid="stNotificationContentError"] {
        background: rgba(255, 82, 82, 0.1);
        border-left: 4px solid #FF5252;
    }

    .stAlert [data-testid="stNotificationContentWarning"] {
        background: rgba(255, 183, 77, 0.1);
        border-left: 4px solid #FFB74D;
    }

    /* === Expander === */
    details[data-testid="stExpander"] {
        background: #1A1F2E;
        border: 1px solid rgba(0, 212, 170, 0.1);
        border-radius: 10px;
    }

    details[data-testid="stExpander"] summary:hover {
        color: #00D4AA;
    }

    /* === Progress bars === */
    .stProgress > div > div > div {
        background: linear-gradient(90deg, #00D4AA 0%, #00B894 100%);
        border-radius: 4px;
    }

    /* === Dividers === */
    hr {
        border-color: rgba(0, 212, 170, 0.1);
    }

    /* === Scrollbar === */
    ::-webkit-scrollbar {
        width: 6px;
        height: 6px;
    }

    ::-webkit-scrollbar-track {
        background: #0E1117;
    }

    ::-webkit-scrollbar-thumb {
        background: #2A3042;
        border-radius: 3px;
    }

    ::-webkit-scrollbar-thumb:hover {
        background: #00D4AA;
    }

    /* === Plotly chart containers === */
    .js-plotly-plot .plotly .main-svg {
        border-radius: 10px;
    }

    /* === Header styling === */
    h1 {
        color: #E8ECF1 !important;
        font-weight: 700 !important;
    }

    h2, h3 {
        color: #C8D0DC !important;
        font-weight: 600 !important;
    }

    /* === Sidebar status indicators === */
    .sidebar-status {
        background: #1A1F2E;
        border-radius: 8px;
        padding: 12px;
        margin: 4px 0;
        border-left: 3px solid #00D4AA;
    }
</style>
""", unsafe_allow_html=True)

# --- Load Config ---
config = AppConfig.from_env()

# --- Auth Gate ---
if not require_auth(config):
    st.stop()

# --- Import Pages ---
from pages import (
    home,
    security_setup,
    agent_manager,
    live_monitoring,
    backtesting_ui,
    ml_improvement,
    cost_tracker_ui,
    suggestions,
    execution_control,
    system_config,
)

# --- Navigation ---
pages = [
    st.Page(home.render, title="Dashboard", icon="🏠", default=True, url_path="dashboard"),
    st.Page(live_monitoring.render, title="Live Monitoring", icon="📈", url_path="monitoring"),
    st.Page(execution_control.render, title="Execution", icon="⚡", url_path="execution"),
    st.Page(suggestions.render, title="Vorschläge", icon="💡", url_path="suggestions"),
    st.Page(agent_manager.render, title="Agent Manager", icon="🤖", url_path="agents"),
    st.Page(cost_tracker_ui.render, title="Cost Tracker", icon="💰", url_path="costs"),
    st.Page(backtesting_ui.render, title="Backtesting", icon="📊", url_path="backtesting"),
    st.Page(ml_improvement.render, title="ML Improvement", icon="🧠", url_path="ml"),
    st.Page(system_config.render, title="System Config", icon="⚙️", url_path="config"),
    st.Page(security_setup.render, title="Security & Setup", icon="🔒", url_path="security"),
]

nav = st.navigation(pages)

# --- Sidebar (always visible) ---
with st.sidebar:
    st.markdown("""
    <div style="text-align:center; padding: 8px 0 4px 0;">
        <span style="font-size: 2rem;">📊</span><br>
        <span style="font-size: 1.1rem; font-weight: 700; color: #00D4AA;">Polymarket</span><br>
        <span style="font-size: 0.75rem; color: #5A6478; letter-spacing: 0.1em;">AGENT DASHBOARD</span>
    </div>
    """, unsafe_allow_html=True)
    st.divider()

    # Quick status from Bot API
    from services.bot_api_client import get_bot_client
    client = get_bot_client()
    status = client.get_status()

    if status:
        active_agents = status.get("active_agents", 0)
        cost_today = status.get("cost_today_usd", 0)
        pending = status.get("pending_suggestions", 0)
        cb = status.get("circuit_breaker", {})
        paused_until = cb.get("paused_until")
        trading_mode = status.get("trading_mode", "?")
        bot_paused = status.get("bot_paused", False)

        # Bot connection indicator
        mode_color = {"paper": "#FFB74D", "semi-auto": "#448AFF", "full-auto": "#00D4AA"}.get(trading_mode, "#888")
        st.markdown(f"""
        <div style="background: linear-gradient(135deg, #1A1F2E, #1E2538); border-radius: 10px;
                    padding: 14px; margin-bottom: 10px; border: 1px solid rgba(0,212,170,0.1);">
            <div style="color: #5A6478; font-size: 0.7rem; text-transform: uppercase; letter-spacing: 0.08em;">Bot Status</div>
            <div style="color: {'#FF5252' if bot_paused else '#00D4AA'}; font-size: 1.2rem; font-weight: 700;">
                {'PAUSIERT' if bot_paused else 'AKTIV'}
            </div>
            <div style="color: {mode_color}; font-size: 0.75rem; margin-top: 4px;">Mode: {trading_mode}</div>
        </div>
        """, unsafe_allow_html=True)

        st.markdown(f"""
        <div style="background: linear-gradient(135deg, #1A1F2E, #1E2538); border-radius: 10px;
                    padding: 14px; margin-bottom: 10px; border: 1px solid rgba(0,212,170,0.1);">
            <div style="color: #5A6478; font-size: 0.7rem; text-transform: uppercase; letter-spacing: 0.08em;">Aktive Agents</div>
            <div style="color: #00D4AA; font-size: 1.5rem; font-weight: 700;">{active_agents}</div>
        </div>
        <div style="background: linear-gradient(135deg, #1A1F2E, #1E2538); border-radius: 10px;
                    padding: 14px; margin-bottom: 10px; border: 1px solid rgba(0,212,170,0.1);">
            <div style="color: #5A6478; font-size: 0.7rem; text-transform: uppercase; letter-spacing: 0.08em;">Kosten heute</div>
            <div style="color: {'#FF5252' if cost_today > 4.0 else '#FFB74D' if cost_today > 2.0 else '#E8ECF1'};
                        font-size: 1.5rem; font-weight: 700;">${cost_today:.2f}</div>
        </div>
        """, unsafe_allow_html=True)

        if pending > 0:
            st.markdown(f"""
            <div style="background: rgba(255, 183, 77, 0.1); border-radius: 10px;
                        padding: 14px; margin-bottom: 10px; border-left: 3px solid #FFB74D;">
                <div style="color: #FFB74D; font-size: 0.85rem; font-weight: 600;">
                    {pending} offene Vorschläge
                </div>
            </div>
            """, unsafe_allow_html=True)

        circuit_active = False
        if paused_until:
            from datetime import datetime
            try:
                paused = datetime.fromisoformat(paused_until)
                if paused > datetime.utcnow():
                    circuit_active = True
            except Exception:
                pass

        if circuit_active:
            st.markdown("""
            <div style="background: rgba(255, 82, 82, 0.1); border-radius: 10px;
                        padding: 14px; margin-bottom: 10px; border-left: 3px solid #FF5252;
                        animation: pulse 2s infinite;">
                <div style="color: #FF5252; font-size: 0.85rem; font-weight: 700;">
                    CIRCUIT BREAKER AKTIV
                </div>
            </div>
            <style>
                @keyframes pulse { 0%,100% {opacity:1;} 50% {opacity:0.7;} }
            </style>
            """, unsafe_allow_html=True)
    else:
        st.markdown("""
        <div style="background: rgba(255, 82, 82, 0.1); border-radius: 10px;
                    padding: 14px; margin-bottom: 10px; border-left: 3px solid #FF5252;">
            <div style="color: #FF5252; font-size: 0.85rem; font-weight: 600;">
                Bot nicht erreichbar
            </div>
        </div>
        """, unsafe_allow_html=True)

    st.divider()
    st.markdown("""
    <div style="text-align: center; color: #3A4258; font-size: 0.65rem; padding-top: 4px;">
        v2.0 &middot; Remote Monitoring &middot; REST API
    </div>
    """, unsafe_allow_html=True)

# --- Run Selected Page ---
nav.run()
