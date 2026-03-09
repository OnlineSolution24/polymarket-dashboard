"""
OpenRouter Cost API Client.
Fetches real usage data from https://openrouter.ai/api/v1/key
"""

import logging
import time
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

_OPENROUTER_API_URL = "https://openrouter.ai/api/v1/key"
_CACHE_TTL = 120  # seconds
_cache: dict = {"data": None, "ts": 0}


def get_openrouter_costs(api_key: str) -> Optional[dict]:
    """
    Fetch current usage from OpenRouter API.
    Returns dict with keys: usage, usage_daily, usage_weekly, usage_monthly,
    limit, limit_remaining, etc.
    Caches for 2 minutes to avoid rate limits.
    """
    now = time.time()
    if _cache["data"] and (now - _cache["ts"]) < _CACHE_TTL:
        return _cache["data"]

    try:
        resp = httpx.get(
            _OPENROUTER_API_URL,
            headers={"Authorization": f"Bearer {api_key}"},
            timeout=10.0,
        )
        resp.raise_for_status()
        data = resp.json().get("data", {})
        _cache["data"] = data
        _cache["ts"] = now
        return data
    except Exception as e:
        logger.error(f"OpenRouter API error: {e}")
        return _cache["data"]  # return stale cache on error
