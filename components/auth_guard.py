"""
Password gate for the Streamlit app.
Uses query_params for URL-based persistence + auto-submit on Enter.
"""

import hashlib
import streamlit as st

from config import AppConfig


def require_auth(config: AppConfig) -> bool:
    """
    Show login form if not authenticated.
    Returns True if user is authenticated.
    """
    token = _make_token(config.app_password)

    # Already authenticated in this session
    if st.session_state.get("authenticated", False):
        # Keep token in URL so F5 works
        if st.query_params.get("token") != token:
            st.query_params["token"] = token
        return True

    # Check for token in URL (survives F5)
    if st.query_params.get("token") == token:
        st.session_state["authenticated"] = True
        return True

    # Show login form
    _render_login_form(config)
    return False


def _make_token(password: str) -> str:
    """Simple hash token for URL persistence."""
    return hashlib.sha256(f"pm-session-{password}".encode()).hexdigest()[:16]


def _render_login_form(config: AppConfig) -> None:
    """Render a centered, styled login form."""
    st.markdown("""
    <style>
        /* Hide sidebar on login page */
        [data-testid="stSidebar"] { display: none; }
        /* Force password eye-button inside the input field */
        [data-testid="stTextInput"] [data-testid="baseButton-header"] {
            position: absolute !important;
            right: 4px !important;
            top: 50% !important;
            transform: translateY(-50%) !important;
            z-index: 10 !important;
            background: transparent !important;
            border: none !important;
        }
        [data-testid="stTextInput"] > div {
            position: relative !important;
        }
        [data-testid="stTextInput"] input {
            padding-right: 40px !important;
        }
        /* Hide "Press Enter to apply" hint */
        [data-testid="InputInstructions"] { display: none !important; }
    </style>
    """, unsafe_allow_html=True)

    st.markdown("")
    st.markdown("")
    st.markdown("")
    st.markdown("")

    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        # Logo & Title
        st.markdown("""
        <div style="text-align: center; padding: 0 0 20px 0;">
            <div style="font-size: 4rem; margin-bottom: 8px;">📊</div>
            <div style="font-size: 2rem; font-weight: 700; color: #00D4AA; letter-spacing: -0.02em;">
                Polymarket
            </div>
            <div style="font-size: 0.8rem; color: #5A6478; letter-spacing: 0.2em; margin-top: 4px;">
                AGENT DASHBOARD
            </div>
        </div>
        """, unsafe_allow_html=True)

        # Use a form so Enter key submits (no separate button click needed)
        with st.form("login_form"):
            password = st.text_input(
                "Passwort",
                type="password",
                placeholder="Passwort eingeben...",
            )
            submitted = st.form_submit_button(
                "Anmelden", type="primary", use_container_width=True
            )

        if submitted:
            if password == config.app_password:
                st.session_state["authenticated"] = True
                st.query_params["token"] = _make_token(password)
                st.rerun()
            else:
                st.error("Falsches Passwort.")
