"""
Direct LLM client using OpenRouter API.
Replaces the Telegram Bridge for agent thinking.
Supports model routing from platform_config.yaml.
"""

import logging
from typing import Optional

import httpx

from config import AppConfig, load_platform_config

logger = logging.getLogger(__name__)

OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions"

# Map config model names to OpenRouter model IDs
MODEL_MAP = {
    "claude-sonnet": "anthropic/claude-sonnet-4",
    "claude-opus": "anthropic/claude-opus-4",
    "claude-opus-4-6": "anthropic/claude-opus-4-6",
    "haiku": "anthropic/claude-3.5-haiku",
    "gemini-flash": "google/gemini-2.5-flash",
    "gemini-pro": "google/gemini-2.5-flash",
    "gpt-4o-mini": "openai/gpt-4o-mini",
}

# Fallback if model not in map
DEFAULT_MODEL = "google/gemini-2.5-flash"


def _resolve_model(config_model: str) -> str:
    """Resolve a config model name to an OpenRouter model ID."""
    return MODEL_MAP.get(config_model, config_model if "/" in config_model else DEFAULT_MODEL)


def _get_routing_model(task_type: str) -> str:
    """Get the model for a specific task type from model_routing config."""
    platform_cfg = load_platform_config()
    routing = platform_cfg.get("model_routing", {})
    config_name = routing.get(task_type, routing.get("default", "haiku"))
    return _resolve_model(config_name)


def call_llm(
    prompt: str,
    system_prompt: str = "",
    model: str = None,
    task_type: str = "default",
    max_tokens: int = 2000,
    temperature: float = 0.3,
) -> Optional[str]:
    """
    Call an LLM via OpenRouter API.

    Args:
        prompt: The user prompt to send.
        system_prompt: Optional system prompt for context.
        model: Explicit OpenRouter model ID. If None, uses task_type routing.
        task_type: Used to look up model from model_routing config.
        max_tokens: Max response tokens.
        temperature: Sampling temperature.

    Returns:
        The LLM response text, or None on error.
    """
    config = AppConfig.from_env()
    api_key = config.openrouter_api_key

    if not api_key:
        logger.warning("No OPENROUTER_API_KEY configured. Cannot call LLM.")
        return None

    # Resolve model
    resolved_model = model if model else _get_routing_model(task_type)

    # Build messages
    messages = []
    if system_prompt:
        messages.append({"role": "system", "content": system_prompt})
    messages.append({"role": "user", "content": prompt})

    try:
        resp = httpx.post(
            OPENROUTER_URL,
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
            json={
                "model": resolved_model,
                "messages": messages,
                "max_tokens": max_tokens,
                "temperature": temperature,
            },
            timeout=120.0,
        )
        resp.raise_for_status()
        data = resp.json()

        # Extract response
        choices = data.get("choices", [])
        if not choices:
            logger.error(f"LLM returned no choices: {data}")
            return None

        content = choices[0].get("message", {}).get("content", "")

        # Log usage
        usage = data.get("usage", {})
        logger.info(
            f"LLM call: model={resolved_model}, "
            f"tokens_in={usage.get('prompt_tokens', 0)}, "
            f"tokens_out={usage.get('completion_tokens', 0)}"
        )

        return content

    except httpx.HTTPStatusError as e:
        logger.error(f"LLM API error {e.response.status_code}: {e.response.text[:200]}")
        return None
    except Exception as e:
        logger.error(f"LLM call failed: {e}")
        return None
