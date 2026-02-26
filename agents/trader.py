"""
Trader Agent - Executes trades ONLY when user gives explicit EXECUTE command.
Never trades autonomously.
"""

from agents.base_agent import BaseAgent
from db import engine


class TraderAgent(BaseAgent):
    """
    Handles trade execution. NEVER initiates trades autonomously.
    Only processes trades with status 'pending' that were created by user EXECUTE commands.
    """

    def run_cycle(self) -> dict:
        """Check for pending trades and attempt execution."""
        self.log("debug", "Trader check für pending trades")

        try:
            pending = engine.query("SELECT * FROM trades WHERE status = 'pending' ORDER BY created_at")

            if not pending:
                return {"ok": True, "summary": "No pending trades"}

            for trade in pending:
                self.log("info", f"Pending trade gefunden: {trade['market_id']} {trade['side']} ${trade['amount_usd']}")
                # Actual execution will be implemented in Phase 2
                # For now, just log the pending trades

            return {"ok": True, "summary": f"{len(pending)} pending trades"}

        except Exception as e:
            self.log("error", f"Trader check fehlgeschlagen: {e}")
            return {"ok": False, "summary": str(e)}
