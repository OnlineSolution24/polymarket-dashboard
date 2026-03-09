"""
Password gate for the Streamlit app.
Uses session_state for in-session auth + browser cookie for
persistence across F5/page refreshes.
"""

import hashlib
import streamlit as st
import streamlit.components.v1 as components

from config import AppConfig

_COOKIE_NAME = "pm_auth_token"
_COOKIE_MAX_AGE = 86400 * 7  # 7 days


def require_auth(config: AppConfig) -> bool:
    """
    Show login form if not authenticated.
    Returns True if user is authenticated.
    """
    token = _make_token(config.app_password)

    # 1. Already authenticated in this session
    if st.session_state.get("authenticated", False):
        return True

    # 2. Check browser cookie (survives F5 and page navigation)
    cookie_token = _read_cookie()
    if cookie_token == token:
        st.session_state["authenticated"] = True
        return True

    # 3. Show login form
    _render_login_form(config)
    return False


def _make_token(password: str) -> str:
    """Hash token for cookie persistence."""
    return hashlib.sha256(f"pm-session-{password}".encode()).hexdigest()[:32]


def _read_cookie() -> str:
    """Read auth cookie from browser via st.context."""
    try:
        return st.context.cookies.get(_COOKIE_NAME, "")
    except Exception:
        return ""


def _set_cookie(token: str):
    """Set auth cookie in browser via JS."""
    components.html(
        f"""
        <script>
            document.cookie = "{_COOKIE_NAME}={token}; path=/; max-age={_COOKIE_MAX_AGE}; SameSite=Strict";
        </script>
        """,
        height=0,
    )


def _render_login_form(config: AppConfig) -> None:
    """Render a centered, styled login form."""
    st.markdown("""
    <style>
        [data-testid="stSidebar"] { display: none; }
        [data-testid="InputInstructions"] { display: none !important; }
    </style>
    """, unsafe_allow_html=True)

    st.markdown("")
    st.markdown("")
    st.markdown("")
    st.markdown("")

    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
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

        with st.form("login_form"):
            password = st.text_input(
                "Passwort",
                type="password",
                placeholder="Passwort eingeben...",
                autocomplete="current-password",
            )
            submitted = st.form_submit_button(
                "Anmelden", type="primary", use_container_width=True
            )

        if submitted:
            if password == config.app_password:
                token = _make_token(password)
                st.session_state["authenticated"] = True
                _set_cookie(token)
                st.rerun()
            else:
                st.error("Falsches Passwort.")
