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

        /* Hide any component iframes (cookie setter etc.) */
        iframe[height="0"], iframe[style*="height: 0"] {
            display: none !important;
            position: absolute !important;
            width: 0 !important;
            height: 0 !important;
            border: none !important;
            overflow: hidden !important;
        }

        /* Password eye-button: keep inside input */
        [data-testid="stTextInput"] > div > div {
            position: relative !important;
            overflow: hidden !important;
        }
        [data-testid="stTextInput"] [data-testid="baseButton-header"] {
            position: absolute !important;
            right: 6px !important;
            top: 50% !important;
            transform: translateY(-50%) !important;
            z-index: 10 !important;
            background: transparent !important;
            border: none !important;
            color: #5A6478 !important;
            padding: 4px !important;
            margin: 0 !important;
            width: 28px !important;
            height: 28px !important;
        }
        [data-testid="stTextInput"] [data-testid="baseButton-header"]:hover {
            color: #00D4AA !important;
        }

        /* Input field styling */
        [data-testid="stTextInput"] input[type="password"],
        [data-testid="stTextInput"] input[type="text"] {
            padding-right: 40px !important;
            background: #131927 !important;
            border: 1px solid rgba(0, 212, 170, 0.15) !important;
            color: #C8D0DC !important;
            border-radius: 8px !important;
        }
        [data-testid="stTextInput"] input:focus {
            border-color: #00D4AA !important;
            box-shadow: 0 0 0 2px rgba(0, 212, 170, 0.1) !important;
        }
        [data-testid="stTextInput"] input::placeholder {
            color: #3A4258 !important;
        }

        /* Override browser autofill yellow/olive background */
        [data-testid="stTextInput"] input:-webkit-autofill,
        [data-testid="stTextInput"] input:-webkit-autofill:hover,
        [data-testid="stTextInput"] input:-webkit-autofill:focus {
            -webkit-box-shadow: 0 0 0 30px #131927 inset !important;
            -webkit-text-fill-color: #C8D0DC !important;
            border: 1px solid rgba(0, 212, 170, 0.15) !important;
            transition: background-color 5000s ease-in-out 0s;
        }

        /* Form border subtler */
        [data-testid="stForm"] {
            border-color: rgba(0, 212, 170, 0.08) !important;
            background: rgba(19, 25, 39, 0.5) !important;
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
