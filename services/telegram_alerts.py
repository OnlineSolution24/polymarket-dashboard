"""
Telegram alerts to YOU personally.
Sends notifications for important events (budget warnings, circuit breaker, etc.).
Uses the bot token to send messages directly to your user ID.
"""

import logging
from typing import Optional

import httpx

from config import AppConfig, get_alert_config

logger = logging.getLogger(__name__)


class TelegramAlerts:
    """Send alert messages to the user's personal Telegram."""

    def __init__(self, config: AppConfig):
        self.bot_token = config.telegram_bot_token
        self.user_id = config.alert_telegram_user_id
        self.alert_config = get_alert_config()
        self._enabled = bool(self.bot_token and self.user_id and self.alert_config.get("enabled", True))

    def send(self, message: str, parse_mode: str = "HTML") -> bool:
        """Send a message to the user via Telegram Bot API."""
        if not self._enabled:
            logger.debug("Alerts disabled or not configured.")
            return False

        url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
        payload = {
            "chat_id": self.user_id,
            "text": message,
            "parse_mode": parse_mode,
        }

        try:
            resp = httpx.post(url, json=payload, timeout=10)
            if resp.status_code == 200:
                logger.info(f"Alert sent: {message[:50]}...")
                return True
            else:
                logger.error(f"Alert failed: {resp.status_code} {resp.text}")
                return False
        except Exception as e:
            logger.error(f"Alert error: {e}")
            return False

    def alert_budget_warning(self, current: float, limit: float, period: str = "Tag"):
        """Send budget warning alert."""
        if not self.alert_config.get("on_budget_warning", True):
            return
        pct = (current / limit * 100) if limit > 0 else 0
        self.send(
            f"⚠️ <b>Budget-Warnung ({period})</b>\n"
            f"Verbraucht: ${current:.2f} / ${limit:.2f} ({pct:.0f}%)"
        )

    def alert_circuit_breaker(self, losses: int, paused_until: str):
        """Send circuit breaker activation alert."""
        if not self.alert_config.get("on_circuit_breaker", True):
            return
        self.send(
            f"🔴 <b>Circuit Breaker AKTIV</b>\n"
            f"Verluste in Folge: {losses}\n"
            f"Trading pausiert bis: {paused_until}"
        )

    def alert_new_suggestion(self, title: str, agent_id: str):
        """Send new suggestion alert."""
        if not self.alert_config.get("on_new_suggestion", True):
            return
        self.send(
            f"💡 <b>Neuer Vorschlag</b>\n"
            f"Von: {agent_id}\n"
            f"{title}"
        )

    def alert_trade_executed(self, market: str, side: str, amount: float):
        """Send trade execution alert."""
        if not self.alert_config.get("on_trade_executed", True):
            return
        self.send(
            f"⚡ <b>Trade ausgeführt</b>\n"
            f"Markt: {market}\n"
            f"Seite: {side} | Betrag: ${amount:.2f}"
        )

    def alert_trade_settled(self, market: str, side: str, result: str,
                            pnl: float, amount: float):
        """Send trade settlement alert (win/loss)."""
        if not self.alert_config.get("on_trade_settled", True):
            return

        # Sanity check: PnL should never exceed ~20x the invested amount
        # (max theoretical is buy at 0.05 → resolve at 1.0 = 19x, but >10x is suspicious)
        suspicious = abs(pnl) > amount * 10 if amount > 0 else False

        emoji = "✅" if result == "win" else "❌"
        if suspicious:
            emoji = "🚨"
        pnl_str = f"+${pnl:.2f}" if pnl >= 0 else f"-${abs(pnl):.2f}"
        msg = (
            f"{emoji} <b>Trade {result.upper()}</b>\n"
            f"Markt: {market}\n"
            f"Seite: {side} | Einsatz: ${amount:.2f}\n"
            f"Ergebnis: {pnl_str}"
        )
        if suspicious:
            msg += (
                f"\n\n🚨 <b>ACHTUNG: PnL unplausibel!</b>\n"
                f"PnL ${pnl:+.2f} bei Einsatz ${amount:.2f} ({abs(pnl/amount):.0f}x).\n"
                f"Bitte manuell pruefen — moeglicherweise falsche Side/Resolution."
            )
        self.send(msg)

    # Errors to ignore in alerts (auth retries, known harmless patterns)
    # NOTE: never ignore HTTP status codes here — they can mask real API breakage
    _IGNORED_ERROR_PATTERNS = [
        "auth/api-key",
        "auth/derive-api-key",
        "create_or_derive_api_creds",
    ]

    def alert_agent_error(self, agent_id: str, error: str):
        """Send agent error alert. Ignores auth-related and known harmless errors."""
        if not self.alert_config.get("on_agent_error", True):
            return
        # Filter out known harmless errors (auth retries etc.)
        error_lower = error.lower()
        if any(p.lower() in error_lower for p in self._IGNORED_ERROR_PATTERNS):
            return
        self.send(
            f"❌ <b>Agent Fehler</b>\n"
            f"Agent: {agent_id}\n"
            f"Fehler: {error[:200]}"
        )

    def send_daily_summary(self, summary: str):
        """Send daily summary."""
        if not self.alert_config.get("on_daily_summary", True):
            return
        self.send(f"📊 <b>Tages-Zusammenfassung</b>\n{summary}")


# Singleton
_alerts: Optional[TelegramAlerts] = None


def get_alerts(config: AppConfig) -> TelegramAlerts:
    global _alerts
    if _alerts is None:
        _alerts = TelegramAlerts(config)
    return _alerts
