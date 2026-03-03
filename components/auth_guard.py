"""
Password gate for the Streamlit app.
Uses session_state for in-session persistence and query params for cross-reload persistence.
"""

import streamlit as st

from config import AppConfig


def require_auth(config: AppConfig) -> bool:
    """
    Show login form if not authenticated.
    Returns True if user is authenticated.
    """
    # Already authenticated in this session
    if st.session_state.get("authenticated", False):
        return True

    # Check for token in query params (persistent across reloads)
    params = st.query_params
    if params.get("token") == _make_token(config.app_password):
        st.session_state["authenticated"] = True
        return True

    # Show login form
    _render_login_form(config)
    return False


def _make_token(password: str) -> str:
    """Simple hash token for URL persistence."""
    import hashlib
    return hashlib.sha256(f"pm-session-{password}".encode()).hexdigest()[:16]


def _render_login_form(config: AppConfig) -> None:
    """Render a centered, styled login form."""
    # Vertical spacer
    st.markdown("<div style='height: 12vh'></div>", unsafe_allow_html=True)

    col1, col2, col3 = st.columns([1.2, 1.6, 1.2])
    with col2:
        # Logo & Title
        st.markdown("""
        <div style="text-align: center; padding: 0 0 30px 0;">
            <div style="font-size: 4rem; margin-bottom: 8px;">📊</div>
            <div style="font-size: 2rem; font-weight: 700; color: #00D4AA; letter-spacing: -0.02em;">
                Polymarket
            </div>
            <div style="font-size: 0.8rem; color: #5A6478; letter-spacing: 0.2em; margin-top: 4px;">
                AGENT DASHBOARD
            </div>
        </div>
        """, unsafe_allow_html=True)

        # Login card
        st.markdown("""
        <div style="background: linear-gradient(135deg, #1A1F2E 0%, #1E2538 100%);
                    border: 1px solid rgba(0, 212, 170, 0.15);
                    border-radius: 16px; padding: 32px 28px 24px 28px;
                    box-shadow: 0 8px 32px rgba(0, 0, 0, 0.3);">
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
                # Set token in URL for persistence across reloads
                st.query_params["token"] = _make_token(password)
                st.rerun()
            else:
                st.error("Falsches Passwort.")

        st.markdown("</div>", unsafe_allow_html=True)
