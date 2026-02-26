"""
Polymarket Agent Dashboard - Main Entrypoint
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
from db.migrations import initialize_database
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

# --- Initialize Database on Startup ---
if "db_initialized" not in st.session_state:
    initialize_database()
    st.session_state["db_initialized"] = True

# --- Load Config ---
config = AppConfig.from_env()

# --- Start Background Scheduler (once) ---
if "scheduler_started" not in st.session_state:
    try:
        from services.scheduler import start_scheduler
        start_scheduler(config)
        st.session_state["scheduler_started"] = True
    except Exception:
        pass

# --- Load Plugins (once) ---
if "plugins_loaded" not in st.session_state:
    try:
        from plugins.plugin_loader import load_plugins
        loaded = load_plugins()
        st.session_state["plugins_loaded"] = loaded
    except Exception:
        st.session_state["plugins_loaded"] = []

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
    st.Page(home.render, title="Dashboard", icon="🏠", default=True),
    st.Page(security_setup.render, title="Security & Setup", icon="🔒"),
    st.Page(agent_manager.render, title="Agent Manager", icon="🤖"),
    st.Page(live_monitoring.render, title="Live Monitoring", icon="📈"),
    st.Page(backtesting_ui.render, title="Backtesting", icon="📊"),
    st.Page(ml_improvement.render, title="ML Improvement", icon="🧠"),
    st.Page(cost_tracker_ui.render, title="Cost Tracker", icon="💰"),
    st.Page(suggestions.render, title="Vorschläge", icon="💡"),
    st.Page(execution_control.render, title="Execution", icon="⚡"),
    st.Page(system_config.render, title="System Config", icon="⚙️"),
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

    # Quick status in sidebar
    from db import engine
    from datetime import date

    agents_row = engine.query_one("SELECT COUNT(*) as cnt FROM agents WHERE status = 'active'")
    active_agents = agents_row['cnt'] if agents_row else 0

    today = date.today().isoformat()
    cost_row = engine.query_one(
        "SELECT COALESCE(SUM(cost_usd), 0) as total FROM api_costs WHERE date(created_at) = ?", (today,)
    )
    cost_today = cost_row['total'] if cost_row else 0.0

    pending_row = engine.query_one("SELECT COUNT(*) as cnt FROM suggestions WHERE status = 'pending'")
    pending = pending_row["cnt"] if pending_row else 0

    cb = engine.query_one("SELECT * FROM circuit_breaker WHERE id = 1")
    circuit_active = False
    if cb and cb.get("paused_until"):
        from datetime import datetime
        try:
            paused = datetime.fromisoformat(cb["paused_until"])
            if paused > datetime.utcnow():
                circuit_active = True
        except Exception:
            pass

    # Status cards in sidebar
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
                ⚠️ {pending} offene Vorschläge
            </div>
        </div>
        """, unsafe_allow_html=True)

    if circuit_active:
        st.markdown("""
        <div style="background: rgba(255, 82, 82, 0.1); border-radius: 10px;
                    padding: 14px; margin-bottom: 10px; border-left: 3px solid #FF5252;
                    animation: pulse 2s infinite;">
            <div style="color: #FF5252; font-size: 0.85rem; font-weight: 700;">
                🔴 Circuit Breaker AKTIV
            </div>
        </div>
        <style>
            @keyframes pulse { 0%,100% {opacity:1;} 50% {opacity:0.7;} }
        </style>
        """, unsafe_allow_html=True)

    st.divider()
    st.markdown("""
    <div style="text-align: center; color: #3A4258; font-size: 0.65rem; padding-top: 4px;">
        v1.0 &middot; Config-Driven &middot; Self-Extending
    </div>
    """, unsafe_allow_html=True)

# --- Run Selected Page ---
nav.run()
