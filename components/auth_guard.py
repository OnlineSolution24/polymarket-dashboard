"""
Password gate for the Streamlit app.
Uses session_state for in-session auth + JavaScript localStorage for
persistence across F5/page refreshes.
"""

import hashlib
import streamlit as st
import streamlit.components.v1 as components

from config import AppConfig

_TOKEN_KEY = "pm_auth_token"


def require_auth(config: AppConfig) -> bool:
    """
    Show login form if not authenticated.
    Returns True if user is authenticated.
    """
    token = _make_token(config.app_password)

    # Already authenticated in this session
    if st.session_state.get("authenticated", False):
        return True

    # Check if localStorage has valid token (set by JS on previous login)
    _read_token_from_localstorage()

    # Token was read from localStorage on a previous render cycle
    if st.session_state.get("_ls_token") == token:
        st.session_state["authenticated"] = True
        return True

    # Show login form
    _render_login_form(config)
    return False


def _make_token(password: str) -> str:
    """Simple hash token for persistence."""
    return hashlib.sha256(f"pm-session-{password}".encode()).hexdigest()[:16]


def _read_token_from_localstorage():
    """Inject JS to read token from localStorage and write to hidden input."""
    # Use a small HTML component that reads localStorage and sends value back
    if "_ls_token" not in st.session_state:
        st.session_state["_ls_token"] = ""

    result = components.html(
        f"""
        <script>
            const token = localStorage.getItem("{_TOKEN_KEY}") || "";
            // Send token to Streamlit via query params trick
            const url = new URL(window.parent.location);
            const currentToken = url.searchParams.get("token") || "";
            if (token && token !== currentToken) {{
                url.searchParams.set("token", token);
                window.parent.history.replaceState({{}}, "", url);
                // Trigger reload to let Streamlit pick up the param
                window.parent.location.reload();
            }}
        </script>
        """,
        height=0,
    )

    # Also check query params (set by JS above)
    params = st.query_params
    if params.get("token"):
        st.session_state["_ls_token"] = params.get("token")


def _set_token_in_localstorage(token: str):
    """Inject JS to save token to localStorage."""
    components.html(
        f"""
        <script>
            localStorage.setItem("{_TOKEN_KEY}", "{token}");
            const url = new URL(window.parent.location);
            url.searchParams.set("token", "{token}");
            window.parent.history.replaceState({{}}, "", url);
        </script>
        """,
        height=0,
    )


def _render_login_form(config: AppConfig) -> None:
    """Render a centered, styled login form."""
    st.markdown("""
    <style>
        [data-testid="stSidebar"] { display: none; }
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
            )
            submitted = st.form_submit_button(
                "Anmelden", type="primary", use_container_width=True
            )

        if submitted:
            if password == config.app_password:
                token = _make_token(password)
                st.session_state["authenticated"] = True
                st.session_state["_ls_token"] = token
                _set_token_in_localstorage(token)
                st.query_params["token"] = token
                st.rerun()
            else:
                st.error("Falsches Passwort.")
