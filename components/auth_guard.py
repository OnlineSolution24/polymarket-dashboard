"""
Simple password gate for the Streamlit app.
Uses bcrypt for password hashing, st.session_state for session persistence.
"""

import streamlit as st
import bcrypt

from config import AppConfig


def hash_password(password: str) -> str:
    """Hash a password with bcrypt."""
    return bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()


def check_password(password: str, hashed: str) -> bool:
    """Verify a password against a bcrypt hash."""
    return bcrypt.checkpw(password.encode(), hashed.encode())


def require_auth(config: AppConfig) -> bool:
    """
    Show login form if not authenticated.
    Returns True if user is authenticated.
    """
    if st.session_state.get("authenticated", False):
        return True

    # Centered login card
    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        st.markdown("""
        <div style="text-align: center; padding: 40px 0 20px 0;">
            <span style="font-size: 3.5rem;">📊</span><br><br>
            <span style="font-size: 1.8rem; font-weight: 700; color: #00D4AA;">Polymarket</span><br>
            <span style="font-size: 0.85rem; color: #5A6478; letter-spacing: 0.15em;">AGENT DASHBOARD</span>
        </div>
        """, unsafe_allow_html=True)

        st.markdown("<div style='height: 20px'></div>", unsafe_allow_html=True)
        password = st.text_input("Passwort", type="password", key="login_password",
                                 placeholder="Passwort eingeben...")

        if st.button("Login", type="primary", use_container_width=True):
            if password == config.app_password:
                st.session_state["authenticated"] = True
                st.rerun()
            else:
                st.error("Falsches Passwort.")

    return False
