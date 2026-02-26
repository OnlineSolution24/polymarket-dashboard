"""
Polymarket API wrapper.
Supports both public (unauthenticated) and authenticated (trading) modes.
Uses py-clob-client under the hood.
"""

import logging
from datetime import datetime

from config import AppConfig

logger = logging.getLogger(__name__)


class PolymarketService:
    """Wrapper around Polymarket CLOB API."""

    def __init__(self, config: AppConfig):
        self.config = config
        self._public_client = None
        self._auth_client = None
        self._init_clients()

    def _init_clients(self):
        """Initialize API clients."""
        try:
            from py_clob_client.client import ClobClient

            # Public client (always available)
            self._public_client = ClobClient(self.config.polymarket_host)

            # Authenticated client (only if private key provided)
            if self.config.polymarket_private_key:
                self._auth_client = ClobClient(
                    self.config.polymarket_host,
                    key=self.config.polymarket_private_key,
                    chain_id=self.config.polymarket_chain_id,
                    signature_type=1,
                    funder=self.config.polymarket_funder or None,
                )
                creds = self._auth_client.create_or_derive_api_creds()
                self._auth_client.set_api_creds(creds)
                logger.info("Authenticated Polymarket client initialized")
        except ImportError:
            logger.warning("py-clob-client not installed. Using mock data.")
        except Exception as e:
            logger.error(f"Failed to initialize Polymarket client: {e}")

    def fetch_markets(self, limit: int = 50) -> list[dict]:
        """
        Fetch current markets from Polymarket.
        Returns normalized list of market dicts.
        """
        if self._public_client:
            return self._fetch_live_markets(limit)
        return self._mock_markets()

    def _fetch_live_markets(self, limit: int) -> list[dict]:
        """Fetch markets from actual Polymarket API."""
        try:
            response = self._public_client.get_markets(limit=limit)
            markets = []

            data = response if isinstance(response, list) else response.get("data", [])

            for item in data[:limit]:
                tokens = item.get("tokens", [])
                yes_price = 0.0
                no_price = 0.0

                if len(tokens) >= 2:
                    yes_price = float(tokens[0].get("price", 0))
                    no_price = float(tokens[1].get("price", 0))
                elif len(tokens) == 1:
                    yes_price = float(tokens[0].get("price", 0))
                    no_price = 1.0 - yes_price

                markets.append({
                    "id": item.get("condition_id", item.get("id", "")),
                    "question": item.get("question", "Unknown"),
                    "slug": item.get("slug", ""),
                    "yes_price": yes_price,
                    "no_price": no_price,
                    "volume": float(item.get("volume", 0)),
                    "liquidity": float(item.get("liquidity", 0)),
                    "end_date": item.get("end_date_iso", item.get("end_date")),
                    "category": item.get("category", ""),
                })

            logger.info(f"Fetched {len(markets)} markets from Polymarket")
            return markets

        except Exception as e:
            logger.error(f"Error fetching markets: {e}")
            return []

    def get_order_book(self, token_id: str) -> dict:
        """Get order book for a specific token."""
        if not self._public_client:
            return {"bids": [], "asks": []}
        try:
            return self._public_client.get_order_book(token_id)
        except Exception as e:
            logger.error(f"Error fetching order book: {e}")
            return {"bids": [], "asks": []}

    def place_market_order(self, token_id: str, amount: float, side: str) -> dict:
        """
        Place a market order (requires authenticated client).
        Returns order result or error.
        """
        if not self._auth_client:
            return {"error": "Authenticated client not configured. Set POLYMARKET_PRIVATE_KEY in .env"}

        try:
            from py_clob_client.order_builder.constants import BUY, SELL

            order_side = BUY if side.upper() == "YES" else SELL
            order = self._auth_client.create_market_order(
                token_id=token_id,
                amount=amount,
                side=order_side,
            )
            result = self._auth_client.post_order(order)
            logger.info(f"Order placed: {side} ${amount} on {token_id}")
            return {"ok": True, "result": result}

        except Exception as e:
            logger.error(f"Order failed: {e}")
            return {"error": str(e)}

    @staticmethod
    def _mock_markets() -> list[dict]:
        """Return mock market data when API is unavailable."""
        return [
            {
                "id": "mock_001",
                "question": "Will BTC reach $100k by end of 2026?",
                "slug": "btc-100k-2026",
                "yes_price": 0.65,
                "no_price": 0.35,
                "volume": 1250000,
                "liquidity": 450000,
                "end_date": "2026-12-31",
                "category": "crypto",
            },
            {
                "id": "mock_002",
                "question": "Will there be a US government shutdown in Q1 2026?",
                "slug": "us-shutdown-q1-2026",
                "yes_price": 0.30,
                "no_price": 0.70,
                "volume": 890000,
                "liquidity": 320000,
                "end_date": "2026-03-31",
                "category": "politics",
            },
            {
                "id": "mock_003",
                "question": "Will SpaceX Starship reach orbit by June 2026?",
                "slug": "starship-orbit-2026",
                "yes_price": 0.82,
                "no_price": 0.18,
                "volume": 560000,
                "liquidity": 180000,
                "end_date": "2026-06-30",
                "category": "science",
            },
        ]
