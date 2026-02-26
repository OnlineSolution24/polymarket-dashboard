"""
Telegram Bridge to OpenClaw.
Runs Telethon in a background thread with its own asyncio event loop.
Provides sync-friendly methods for Streamlit to call.
"""

import asyncio
import logging
import threading
import queue
from typing import Optional

from config import AppConfig

logger = logging.getLogger(__name__)


class TelegramBridge:
    """
    Bridges Streamlit (sync) with OpenClaw via Telegram (async).
    Runs a Telethon client in a background thread.
    """

    def __init__(self, config: AppConfig):
        self.config = config
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._thread: Optional[threading.Thread] = None
        self._client = None
        self._response_queue: queue.Queue = queue.Queue()
        self._started = False

    def start(self) -> bool:
        """Start the Telegram bridge in a background thread."""
        if self._started:
            return True

        if not self.config.telegram_api_id or not self.config.telegram_api_hash:
            logger.warning("Telegram credentials not configured. Bridge not started.")
            return False

        try:
            self._loop = asyncio.new_event_loop()
            self._thread = threading.Thread(
                target=self._run_loop,
                daemon=True,
                name="telegram-bridge",
            )
            self._thread.start()
            self._started = True
            logger.info("Telegram bridge started")
            return True
        except Exception as e:
            logger.error(f"Failed to start Telegram bridge: {e}")
            return False

    def _run_loop(self):
        """Run the asyncio event loop in the background thread."""
        asyncio.set_event_loop(self._loop)
        self._loop.run_until_complete(self._async_main())

    async def _async_main(self):
        """Main async function: connect Telethon and listen for messages."""
        try:
            from telethon import TelegramClient, events

            self._client = TelegramClient(
                "polymarket_dashboard_session",
                self.config.telegram_api_id,
                self.config.telegram_api_hash,
            )

            await self._client.start()
            logger.info("Telethon client connected")

            # Listen for responses from OpenClaw
            @self._client.on(events.NewMessage(chats=self.config.openclaw_chat_id))
            async def on_openclaw_message(event):
                self._response_queue.put(event.message.text)

            # Keep running
            await self._client.run_until_disconnected()

        except ImportError:
            logger.warning("Telethon not installed. Telegram bridge running in mock mode.")
            # Keep thread alive in mock mode
            while True:
                await asyncio.sleep(60)
        except Exception as e:
            logger.error(f"Telegram bridge error: {e}")

    def send_message(self, message: str) -> bool:
        """
        Send a message to OpenClaw (fire-and-forget).
        Thread-safe, can be called from Streamlit.
        """
        if not self._started or not self._loop:
            logger.warning("Bridge not started. Message not sent.")
            return False

        try:
            future = asyncio.run_coroutine_threadsafe(
                self._async_send(message), self._loop
            )
            future.result(timeout=30)
            return True
        except Exception as e:
            logger.error(f"Failed to send message: {e}")
            return False

    def send_and_wait(self, message: str, timeout: float = 120) -> Optional[str]:
        """
        Send message to OpenClaw and wait for a response.
        Thread-safe, blocking call.
        """
        # Clear any stale responses
        while not self._response_queue.empty():
            try:
                self._response_queue.get_nowait()
            except queue.Empty:
                break

        if not self.send_message(message):
            return None

        try:
            response = self._response_queue.get(timeout=timeout)
            return response
        except queue.Empty:
            logger.warning(f"No response from OpenClaw within {timeout}s")
            return None

    async def _async_send(self, message: str):
        """Async send message via Telethon."""
        if self._client:
            await self._client.send_message(self.config.openclaw_chat_id, message)

    def is_connected(self) -> bool:
        """Check if bridge is running."""
        return self._started and self._thread is not None and self._thread.is_alive()


# Singleton instance
_bridge: Optional[TelegramBridge] = None


def get_bridge(config: AppConfig) -> TelegramBridge:
    """Get or create the singleton TelegramBridge instance."""
    global _bridge
    if _bridge is None:
        _bridge = TelegramBridge(config)
    return _bridge
