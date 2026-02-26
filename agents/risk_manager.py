"""
Risk Manager Agent - Portfolio risk assessment and circuit breaker management.
"""

from datetime import datetime, timedelta

from agents.base_agent import BaseAgent
from config import load_platform_config
from db import engine


class RiskManagerAgent(BaseAgent):
    """Monitors risk exposure and manages the circuit breaker."""

    def run_cycle(self) -> dict:
        self.log("info", "Risk check gestartet")

        try:
            # 1. Check consecutive losses
            self._check_circuit_breaker()

            # 2. Check portfolio exposure
            exposure = self._check_exposure()

            # 3. Generate risk report
            if exposure["total_exposure_usd"] > 0:
                prompt = (
                    f"Risk Report:\n"
                    f"Offene Positionen: {exposure['open_count']}\n"
                    f"Total Exposure: ${exposure['total_exposure_usd']:.2f}\n"
                    f"Max Einzelposition: ${exposure['max_position_usd']:.2f}\n"
                    f"Verluste in Folge: {exposure['consecutive_losses']}\n\n"
                    f"Bewerte das aktuelle Risiko und schlage Anpassungen vor falls nötig."
                )
                response = self.think(prompt)
                if response:
                    self.log("info", f"Risk assessment: {response[:200]}")

            return {"ok": True, "summary": f"Risk check complete. Exposure: ${exposure['total_exposure_usd']:.2f}"}

        except Exception as e:
            self.log("error", f"Risk check fehlgeschlagen: {e}")
            return {"ok": False, "summary": str(e)}

    def _check_circuit_breaker(self) -> None:
        """Check and update circuit breaker status."""
        platform_cfg = load_platform_config()
        cb_config = platform_cfg.get("circuit_breaker", {})
        max_losses = cb_config.get("max_consecutive_losses", 3)
        pause_hours = cb_config.get("pause_hours", 24)

        # Count consecutive losses
        recent_trades = engine.query(
            "SELECT result FROM trades WHERE status = 'executed' ORDER BY executed_at DESC LIMIT ?",
            (max_losses,),
        )

        consecutive_losses = 0
        for t in recent_trades:
            if t["result"] == "loss":
                consecutive_losses += 1
            else:
                break

        # Update circuit breaker
        cb = engine.query_one("SELECT * FROM circuit_breaker WHERE id = 1")
        paused_until = None

        if consecutive_losses >= max_losses:
            paused_until = (datetime.utcnow() + timedelta(hours=pause_hours)).isoformat()
            self.log("warn", f"Circuit Breaker aktiviert! {consecutive_losses} Verluste in Folge.")

            # Send alert
            try:
                from config import AppConfig
                from services.telegram_alerts import get_alerts
                alerts = get_alerts(AppConfig.from_env())
                alerts.alert_circuit_breaker(consecutive_losses, paused_until)
            except Exception:
                pass

        engine.execute(
            "UPDATE circuit_breaker SET consecutive_losses = ?, paused_until = ?, last_updated = ? WHERE id = 1",
            (consecutive_losses, paused_until, datetime.utcnow().isoformat()),
        )

    def _check_exposure(self) -> dict:
        """Calculate current portfolio exposure."""
        open_trades = engine.query(
            "SELECT * FROM trades WHERE status = 'executed' AND (result = 'open' OR result IS NULL)"
        )

        total_exposure = sum(t.get("amount_usd", 0) for t in open_trades)
        max_position = max((t.get("amount_usd", 0) for t in open_trades), default=0)

        cb = engine.query_one("SELECT consecutive_losses FROM circuit_breaker WHERE id = 1")

        return {
            "open_count": len(open_trades),
            "total_exposure_usd": total_exposure,
            "max_position_usd": max_position,
            "consecutive_losses": cb["consecutive_losses"] if cb else 0,
        }
