"""Tests for the Telegram bridge service."""

from config import AppConfig
from services.telegram_bridge import TelegramBridge


def test_bridge_not_started():
    """Test bridge without credentials doesn't start."""
    config = AppConfig(telegram_api_id=0, telegram_api_hash="")
    bridge = TelegramBridge(config)
    result = bridge.start()
    assert result is False
    assert bridge.is_connected() is False


def test_bridge_send_without_connection():
    """Test sending message without connection returns False."""
    config = AppConfig(telegram_api_id=0, telegram_api_hash="")
    bridge = TelegramBridge(config)
    result = bridge.send_message("test")
    assert result is False


def test_bridge_send_and_wait_without_connection():
    """Test send_and_wait without connection returns None."""
    config = AppConfig(telegram_api_id=0, telegram_api_hash="")
    bridge = TelegramBridge(config)
    result = bridge.send_and_wait("test", timeout=1)
    assert result is None
