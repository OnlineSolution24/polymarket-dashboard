"""
Password gate for the Streamlit app.
Uses cookie-based persistent sessions so users stay logged in across page reloads.
"""

import hashlib
import hmac
import streamlit as st
import streamlit.components.v1 as components

from config import AppConfig

# Secret used to sign session cookies
_COOKIE_SECRET = "polymarket-dashboard-session-v1"
_COOKIE_NAME = "pm_session"
_COOKIE_MAX_AGE = 30 * 24 * 3600  # 30 days


def _make_token(password: str) -> str:
    """Create a signed session token from the password."""
    return hmac.new(
        _COOKIE_SECRET.encode(), password.encode(), hashlib.sha256
    ).hexdigest()[:32]


def _set_cookie_js(name: str, value: str, max_age: int) -> None:
    """Inject JavaScript to set a cookie in the browser."""
    components.html(
        f"""<script>
        document.cookie = "{name}={value}; path=/; max-age={max_age}; SameSite=Lax";
        </script>""",
        height=0,
    )


def _get_cookie_js(name: str) -> str | None:
    """Read cookie value via query params bridge.
    On first load, inject JS that reads the cookie and sets it as a query param.
    """
    # Check if cookie value was already passed back via query params
    params = st.query_params
    cookie_val = params.get(f"_c_{name}")
    if cookie_val:
        return cookie_val
    return None


def _inject_cookie_reader(name: str) -> None:
    """Inject JS that reads the cookie and passes it back via query param."""
    components.html(
        f"""<script>
        (function() {{
            const cookies = document.cookie.split(';');
            for (let c of cookies) {{
                c = c.trim();
                if (c.startsWith('{name}=')) {{
                    const val = c.substring('{name}='.length);
                    const url = new URL(window.parent.location);
                    if (url.searchParams.get('_c_{name}') !== val) {{
                        url.searchParams.set('_c_{name}', val);
                        window.parent.history.replaceState(null, '', url.toString());
                        window.parent.location.reload();
                    }}
                }}
            }}
        }})();
        </script>""",
        height=0,
    )


def require_auth(config: AppConfig) -> bool:
    """
    Show login form if not authenticated.
    Uses cookies for persistent sessions across page reloads.
    Returns True if user is authenticated.
    """
    # Already authenticated in this session
    if st.session_state.get("authenticated", False):
        return True

    # Check for valid session cookie
    expected_token = _make_token(config.app_password)
    cookie_token = _get_cookie_js(_COOKIE_NAME)

    if cookie_token and cookie_token == expected_token:
        st.session_state["authenticated"] = True
        return True

    # No valid cookie found - inject reader to check browser cookies
    if not cookie_token:
        _inject_cookie_reader(_COOKIE_NAME)

    # Show login form
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
                # Set persistent cookie
                token = _make_token(password)
                _set_cookie_js(_COOKIE_NAME, token, _COOKIE_MAX_AGE)
                st.rerun()
            else:
                st.error("Falsches Passwort.")

    return False
