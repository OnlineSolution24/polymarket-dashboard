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

    # 2. Check browser cookie (sent with HTTP request, survives F5)
    cookie_token = _read_cookie()
    if cookie_token == token:
        st.session_state["authenticated"] = True
        return True

    # 3. Check if we just logged in and need a real reload
    #    (cookie was set via JS but st.rerun only uses WebSocket)
    if st.session_state.get("_login_pending", False):
        st.session_state["_login_pending"] = False
        st.session_state["authenticated"] = True
        return True

    # 4. Show login form
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


def _set_cookie_and_reload(token: str):
    """Set auth cookie in browser via JS and trigger a real page reload."""
    components.html(
        f"""
        <script>
            document.cookie = "{_COOKIE_NAME}={token}; path=/; max-age={_COOKIE_MAX_AGE}; SameSite=Lax";
            // Real reload so the cookie gets sent with the next HTTP request
            window.parent.location.reload();
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

        /* Password eye-button fix */
        [data-testid="stTextInput"] [data-testid="baseButton-header"] {
            position: absolute !important;
            right: 8px !important;
            top: 50% !important;
            transform: translateY(-50%) !important;
            z-index: 10 !important;
            background: transparent !important;
            border: none !important;
            color: #8892A4 !important;
        }
        [data-testid="stTextInput"] [data-testid="baseButton-header"]:hover {
            color: #00D4AA !important;
        }
        [data-testid="stTextInput"] > div > div {
            position: relative !important;
            overflow: visible !important;
        }
        [data-testid="stTextInput"] input[type="password"],
        [data-testid="stTextInput"] input[type="text"] {
            padding-right: 44px !important;
            background: #1A1F2E !important;
            border: 1px solid rgba(0, 212, 170, 0.2) !important;
            color: #E8ECF1 !important;
            border-radius: 8px !important;
        }
        [data-testid="stTextInput"] input:focus {
            border-color: #00D4AA !important;
            box-shadow: 0 0 0 2px rgba(0, 212, 170, 0.15) !important;
        }
        [data-testid="stTextInput"] input::placeholder {
            color: #5A6478 !important;
        }
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
                st.session_state["_login_pending"] = True
                _set_cookie_and_reload(token)
            else:
                st.error("Falsches Passwort.")
