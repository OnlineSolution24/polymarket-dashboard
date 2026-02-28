"""
API Key authentication middleware for the Bot REST API.
Validates Bearer tokens against the configured BOT_API_KEY.
"""

import os
import secrets
from fastapi import HTTPException, Security
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

_security = HTTPBearer()


def get_api_key() -> str:
    """Get the configured API key. Generate a random one if not set."""
    key = os.getenv("BOT_API_KEY", "")
    if not key:
        key = secrets.token_urlsafe(32)
        os.environ["BOT_API_KEY"] = key
        import logging
        logging.getLogger(__name__).warning(
            f"No BOT_API_KEY configured. Generated temporary key: {key}"
        )
    return key


async def verify_api_key(
    credentials: HTTPAuthorizationCredentials = Security(_security),
) -> str:
    """FastAPI dependency that validates the Bearer token."""
    expected = get_api_key()
    if not secrets.compare_digest(credentials.credentials, expected):
        raise HTTPException(status_code=401, detail="Invalid API key")
    return credentials.credentials
