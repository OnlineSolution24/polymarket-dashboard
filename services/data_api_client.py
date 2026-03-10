"""
Polymarket Data API client — Whale tracking, Open Interest, Volume signals.
All endpoints are public (no auth required).
"""

import logging
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

DATA_API = "https://data-api.polymarket.com"
CLOB_API = "https://clob.polymarket.com"

# Trades above this USD value count as "whale" activity
WHALE_THRESHOLD_USD = 500


class DataAPIClient:
    """Fetches whale activity, holder data, open interest, and leaderboard."""

    def __init__(self, timeout: int = 30):
        self._client = httpx.Client(
            timeout=timeout,
            headers={"Accept": "application/json"},
        )

    def close(self):
        self._client.close()

    # ------------------------------------------------------------------
    # Whale Trades
    # ------------------------------------------------------------------

    def get_whale_trades(
        self,
        condition_id: str,
        min_amount: float = WHALE_THRESHOLD_USD,
        limit: int = 100,
    ) -> list[dict]:
        """Fetch large trades for a market (no auth needed).

        Returns list of trade dicts with: side, size, price, timestamp, proxyWallet, pseudonym.
        """
        try:
            resp = self._client.get(
                f"{DATA_API}/trades",
                params={
                    "market": condition_id,
                    "filterType": "CASH",
                    "filterAmount": str(min_amount),
                    "limit": limit,
                },
            )
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.error(f"Whale trades fetch failed for {condition_id[:20]}: {e}")
            return []

    def compute_whale_signals(self, condition_id: str) -> dict:
        """Compute whale buy/sell counts and net flow for a market (last 100 big trades)."""
        trades = self.get_whale_trades(condition_id)
        buy_count = 0
        sell_count = 0
        net_flow = 0.0

        for t in trades:
            side = (t.get("side") or "").upper()
            size = float(t.get("size", 0) or 0)
            if side == "BUY":
                buy_count += 1
                net_flow += size
            elif side == "SELL":
                sell_count += 1
                net_flow -= size

        return {
            "whale_buy_count": buy_count,
            "whale_sell_count": sell_count,
            "whale_net_flow": round(net_flow, 2),
        }

    # ------------------------------------------------------------------
    # Top Holders
    # ------------------------------------------------------------------

    def get_top_holders(self, condition_id: str, limit: int = 20) -> list[dict]:
        """Fetch top holders for a market.

        Returns list with: proxyWallet, amount, pseudonym, name.
        """
        try:
            resp = self._client.get(
                f"{DATA_API}/holders",
                params={"market": condition_id, "limit": limit},
            )
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.error(f"Holders fetch failed for {condition_id[:20]}: {e}")
            return []

    def compute_holder_concentration(self, condition_id: str) -> Optional[float]:
        """Compute top-5 holder concentration (% of total held by top 5).

        Returns 0-1 float. High concentration = few players dominate.
        """
        holders = self.get_top_holders(condition_id)
        if not holders:
            return None

        amounts = [float(h.get("amount", 0) or 0) for h in holders]
        total = sum(amounts)
        if total <= 0:
            return None

        top5 = sum(sorted(amounts, reverse=True)[:5])
        return round(top5 / total, 4)

    # ------------------------------------------------------------------
    # Open Interest
    # ------------------------------------------------------------------

    def get_open_interest(self, condition_id: str) -> Optional[float]:
        """Fetch open interest for a market."""
        try:
            resp = self._client.get(
                f"{DATA_API}/oi",
                params={"market": condition_id},
            )
            resp.raise_for_status()
            data = resp.json()
            if isinstance(data, list) and data:
                return float(data[0].get("value", 0) or 0)
            return None
        except Exception as e:
            logger.error(f"OI fetch failed for {condition_id[:20]}: {e}")
            return None

    # ------------------------------------------------------------------
    # Live Volume
    # ------------------------------------------------------------------

    def get_live_volume(self, event_id: str) -> Optional[float]:
        """Fetch live volume for an event."""
        try:
            resp = self._client.get(
                f"{DATA_API}/live-volume",
                params={"id": event_id},
            )
            resp.raise_for_status()
            data = resp.json()
            return float(data.get("total", 0) or 0)
        except Exception as e:
            logger.error(f"Live volume fetch failed: {e}")
            return None

    # ------------------------------------------------------------------
    # Leaderboard (Top Traders)
    # ------------------------------------------------------------------

    def get_leaderboard(
        self,
        category: str = "OVERALL",
        time_period: str = "WEEK",
        order_by: str = "PNL",
        limit: int = 50,
    ) -> list[dict]:
        """Fetch top trader leaderboard.

        Returns list with: rank, proxyWallet, userName, vol, pnl.
        """
        try:
            resp = self._client.get(
                f"{DATA_API}/v1/leaderboard",
                params={
                    "category": category,
                    "timePeriod": time_period,
                    "orderBy": order_by,
                    "limit": limit,
                },
            )
            resp.raise_for_status()
            data = resp.json()
            # Response may be nested under "leaderboard" key
            if isinstance(data, dict):
                return data.get("leaderboard", data.get("data", []))
            return data if isinstance(data, list) else []
        except Exception as e:
            logger.error(f"Leaderboard fetch failed: {e}")
            return []

    def get_smart_money_wallets(self, limit: int = 30) -> list[str]:
        """Get proxy wallet addresses of top traders (by PnL this week)."""
        lb = self.get_leaderboard(limit=limit)
        return [
            entry.get("proxyWallet")
            for entry in lb
            if entry.get("proxyWallet")
        ]

    # ------------------------------------------------------------------
    # Slippage estimation
    # ------------------------------------------------------------------

    def estimate_slippage(
        self, token_id: str, amount: float, side: str = "BUY"
    ) -> Optional[float]:
        """Estimate execution price for a given order size (vs midpoint).

        Returns slippage as fraction (e.g. 0.02 = 2% worse than mid).
        """
        try:
            # Get market price for the amount
            resp = self._client.get(
                f"{CLOB_API}/market-price",
                params={
                    "token_id": token_id,
                    "side": side,
                    "amount": str(amount),
                },
            )
            resp.raise_for_status()
            exec_price = float(resp.json().get("price", 0))

            # Get midpoint
            mid_resp = self._client.get(
                f"{CLOB_API}/midpoint",
                params={"token_id": token_id},
            )
            mid_resp.raise_for_status()
            midpoint = float(mid_resp.json().get("mid", 0))

            if midpoint <= 0:
                return None

            slippage = abs(exec_price - midpoint) / midpoint
            return round(slippage, 4)
        except Exception as e:
            logger.debug(f"Slippage estimate failed: {e}")
            return None

    # ------------------------------------------------------------------
    # Composite: Smart Money Score
    # ------------------------------------------------------------------

    def compute_smart_money_score(self, condition_id: str) -> float:
        """Compute a 0-100 smart money score for a market.

        Combines:
        - Whale net flow direction (+/- signal)
        - Holder concentration (high = insiders know something)
        - Open interest magnitude
        """
        score = 50.0  # neutral baseline

        # Whale flow
        whale = self.compute_whale_signals(condition_id)
        buy = whale["whale_buy_count"]
        sell = whale["whale_sell_count"]
        total_whale = buy + sell
        if total_whale > 0:
            # More buys = bullish, more sells = bearish
            buy_ratio = buy / total_whale
            score += (buy_ratio - 0.5) * 40  # +-20 points max

        # Holder concentration
        conc = self.compute_holder_concentration(condition_id)
        if conc is not None and conc > 0.5:
            score += 10  # high concentration = conviction

        # Clamp
        return round(max(0, min(100, score)), 1)
