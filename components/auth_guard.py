"""
Password gate for the Streamlit app.
Uses session_state + browser cookie (via st.query_params) for persistence.
Login survives page refreshes and navigation.
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
        # Ensure token stays in URL
        if st.query_params.get("token") != token:
            st.query_params["token"] = token
        return True

    # Check for token in query params (survives refresh)
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
    # CSS fix: password visibility toggle button overflow
    st.markdown("""
    <style>
        /* Fix password eye-button overflow */
        [data-testid="stTextInput"] button {
            position: absolute;
            right: 8px;
            top: 50%;
            transform: translateY(-50%);
        }
        [data-testid="stTextInput"] {
            position: relative;
        }
        /* Hide sidebar on login page */
        [data-testid="stSidebar"] { display: none; }
        /* Center the form vertically */
        .login-spacer { height: 15vh; }
    </style>
    <div class="login-spacer"></div>
    """, unsafe_allow_html=True)

    col1, col2, col3 = st.columns([1.2, 1.6, 1.2])
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

        password = st.text_input(
            "Passwort",
            type="password",
            key="login_password",
            placeholder="Passwort eingeben...",
        )

        if st.button("Anmelden", type="primary", use_container_width=True):
            if password == config.app_password:
                st.session_state["authenticated"] = True
                st.query_params["token"] = _make_token(password)
                st.rerun()
            else:
                st.error("Falsches Passwort.")
