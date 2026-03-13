"""
Polymarket API wrapper.
Supports both public (unauthenticated) and authenticated (trading) modes.
Uses py-clob-client for order books/trading and Gamma API for market discovery.
"""

import logging
from datetime import datetime

import httpx

from config import AppConfig, load_platform_config

logger = logging.getLogger(__name__)


class PolymarketService:
    """Wrapper around Polymarket CLOB + Gamma APIs."""

    def __init__(self, config: AppConfig):
        self.config = config
        self._public_client = None
        self._auth_client = None
        self._gamma = None
        self._init_clients()

    def _init_clients(self):
        """Initialize API clients."""
        # Gamma API client (public, for market discovery)
        platform_cfg = load_platform_config()
        gamma_cfg = platform_cfg.get("gamma_api", {})
        gamma_url = gamma_cfg.get("base_url", "https://gamma-api.polymarket.com")
        self._gamma = httpx.Client(
            base_url=gamma_url,
            timeout=gamma_cfg.get("timeout_seconds", 30),
            headers={"Accept": "application/json"},
        )

        try:
            from py_clob_client.client import ClobClient

            # Public client (for order books)
            self._public_client = ClobClient(self.config.polymarket_host)

            # Authenticated client (for trading)
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

    # ------------------------------------------------------------------
    # Market Discovery (Gamma API)
    # ------------------------------------------------------------------

    def fetch_markets(self, limit: int = 100) -> list[dict]:
        """
        Fetch current active markets.
        Primary: Gamma API (active, sorted by volume) + tag-based discovery.
        Fallback: CLOB API cursor pagination.
        """
        # 1. Fetch top markets by volume (general discovery)
        markets = self._fetch_gamma_markets(limit)

        # 2. Also fetch markets from configured tag_slugs (e.g. weather, climate)
        platform_cfg = load_platform_config()
        tag_slugs = platform_cfg.get("gamma_api", {}).get("tag_slugs", [])
        if tag_slugs:
            seen_ids = {m["id"] for m in markets}
            for slug in tag_slugs:
                tagged = self._fetch_gamma_markets(limit=50, tag_slug=slug)
                for m in tagged:
                    if m["id"] not in seen_ids:
                        markets.append(m)
                        seen_ids.add(m["id"])

        if markets:
            return markets
        if self._public_client:
            return self._fetch_live_markets(limit)
        return self._mock_markets()

    def _fetch_gamma_markets(self, limit: int = 100, tag_slug: str = None) -> list[dict]:
        """Fetch active markets from Gamma Markets API, optionally filtered by tag_slug."""
        platform_cfg = load_platform_config()
        gamma_cfg = platform_cfg.get("gamma_api", {})
        poly_cfg = platform_cfg.get("polymarket", {})
        min_volume = gamma_cfg.get("min_volume_usd", poly_cfg.get("min_volume_usd", 1000))
        min_liquidity = gamma_cfg.get("min_liquidity_usd", poly_cfg.get("min_liquidity_usd", 500))

        # Lower thresholds for tag-based discovery (niche markets have less volume)
        if tag_slug:
            min_volume = min(min_volume, 100)
            min_liquidity = min(min_liquidity, 50)

        try:
            params = {
                "active": "true",
                "closed": "false",
                "order": "volume",
                "ascending": "false",
                "limit": min(limit, 200),
            }
            if tag_slug:
                params["tag_slug"] = tag_slug

            response = self._gamma.get("/markets", params=params)
            response.raise_for_status()
            raw = response.json()

            markets = []
            for item in raw:
                if not item.get("acceptingOrders", True):
                    continue

                # Parse outcome prices (string array like ["0.85","0.15"])
                outcome_prices = item.get("outcomePrices", "")
                if isinstance(outcome_prices, str):
                    try:
                        import json
                        outcome_prices = json.loads(outcome_prices)
                    except Exception:
                        outcome_prices = []

                yes_price = float(outcome_prices[0]) if len(outcome_prices) >= 1 else 0.0
                no_price = float(outcome_prices[1]) if len(outcome_prices) >= 2 else 1.0 - yes_price

                volume = float(item.get("volumeNum", 0) or 0)
                liquidity = float(item.get("liquidityNum", 0) or 0)

                if volume < min_volume or liquidity < min_liquidity:
                    continue

                # Extract CLOB token IDs (critical for trading)
                clob_token_ids = item.get("clobTokenIds", "")
                if isinstance(clob_token_ids, str):
                    try:
                        import json
                        clob_token_ids = json.loads(clob_token_ids)
                    except Exception:
                        clob_token_ids = []

                yes_token = clob_token_ids[0] if len(clob_token_ids) >= 1 else ""
                no_token = clob_token_ids[1] if len(clob_token_ids) >= 2 else ""

                markets.append({
                    "id": item.get("conditionId", item.get("condition_id", item.get("id", ""))),
                    "question": item.get("question", "Unknown"),
                    "description": item.get("description", ""),
                    "slug": item.get("slug", ""),
                    "yes_price": yes_price,
                    "no_price": no_price,
                    "volume": volume,
                    "liquidity": liquidity,
                    "end_date": item.get("endDate", item.get("end_date")),
                    "category": item.get("groupItemTitle", item.get("category", "")),
                    # Gamma-specific fields
                    "yes_token_id": yes_token,
                    "no_token_id": no_token,
                    "best_bid": float(item.get("bestBid", 0) or 0),
                    "best_ask": float(item.get("bestAsk", 0) or 0),
                    "spread": float(item.get("spread", 0) or 0),
                    "volume_24h": float(item.get("volume24hr", 0) or 0),
                    "volume_1w": float(item.get("volume1wk", 0) or 0),
                    "volume_1m": float(item.get("volume1mo", 0) or 0),
                    "last_trade_price": float(item.get("lastTradePrice", 0) or 0),
                    "accepting_orders": 1 if item.get("acceptingOrders", True) else 0,
                })

            tag_info = f" [tag:{tag_slug}]" if tag_slug else ""
            logger.info(f"Gamma API{tag_info}: fetched {len(markets)} active markets (limit={limit})")
            return markets[:limit]

        except Exception as e:
            logger.error(f"Gamma API fetch failed: {e}")
            return []

    def fetch_market_events(self, limit: int = 20) -> list[dict]:
        """Fetch active events from Gamma Events API for event grouping."""
        try:
            params = {
                "active": "true",
                "closed": "false",
                "order": "volume",
                "ascending": "false",
                "limit": limit,
            }
            response = self._gamma.get("/events", params=params)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Gamma Events API fetch failed: {e}")
            return []

    # ------------------------------------------------------------------
    # Market Resolution (Settlement)
    # ------------------------------------------------------------------

    def get_market_resolution(self, condition_id: str) -> dict | None:
        """
        Check if a market has been resolved via Gamma API.
        Returns dict with 'resolved', 'winning_side', 'outcome_prices' or None on error.
        """
        try:
            response = self._gamma.get("/markets", params={
                "condition_id": condition_id,
                "limit": 1,
            })
            response.raise_for_status()
            results = response.json()
            if not results:
                return None

            market = results[0] if isinstance(results, list) else results
            closed = market.get("closed", False)
            resolution_status = market.get("umaResolutionStatus", "")

            if not closed or resolution_status != "resolved":
                return {"resolved": False}

            # Parse final outcome prices: ["1","0"] = YES won, ["0","1"] = NO won
            outcome_prices = market.get("outcomePrices", "")
            if isinstance(outcome_prices, str):
                try:
                    import json
                    outcome_prices = json.loads(outcome_prices)
                except Exception:
                    outcome_prices = []

            yes_final = float(outcome_prices[0]) if len(outcome_prices) >= 1 else 0
            no_final = float(outcome_prices[1]) if len(outcome_prices) >= 2 else 0

            if yes_final >= 0.99:
                winning_side = "YES"
            elif no_final >= 0.99:
                winning_side = "NO"
            else:
                # Market closed but not cleanly resolved (partial / disputed)
                return {"resolved": False}

            return {
                "resolved": True,
                "winning_side": winning_side,
                "yes_final": yes_final,
                "no_final": no_final,
                "closed_time": market.get("closedTime"),
            }

        except Exception as e:
            logger.error(f"Resolution check failed for {condition_id[:20]}: {e}")
            return None

    # ------------------------------------------------------------------
    # Order Book
    # ------------------------------------------------------------------

    def get_order_book(self, token_id: str) -> dict:
        """Get order book for a specific token."""
        if not self._public_client:
            return {"bids": [], "asks": []}
        try:
            return self._public_client.get_order_book(token_id)
        except Exception as e:
            logger.error(f"Error fetching order book: {e}")
            return {"bids": [], "asks": []}

    def get_order_book_analysis(self, token_id: str) -> dict:
        """Analyze order book depth, imbalance, and spread for a token."""
        book = self.get_order_book(token_id)

        # Handle both dict and OrderBookSummary object
        if isinstance(book, dict):
            bids = book.get("bids", [])
            asks = book.get("asks", [])
        else:
            bids = getattr(book, "bids", []) or []
            asks = getattr(book, "asks", []) or []

        if not bids or not asks:
            return {
                "bid_ask_spread": None, "book_imbalance": None,
                "bid_depth": 0, "ask_depth": 0,
            }

        def _get(obj, key, default=0):
            return obj.get(key, default) if isinstance(obj, dict) else getattr(obj, key, default)

        best_bid = float(_get(bids[0], "price", 0))
        best_ask = float(_get(asks[0], "price", 0))
        spread = best_ask - best_bid if best_ask > best_bid else 0

        bid_depth = sum(float(_get(b, "size", 0)) for b in bids[:10])
        ask_depth = sum(float(_get(a, "size", 0)) for a in asks[:10])
        total = bid_depth + ask_depth
        imbalance = (bid_depth - ask_depth) / total if total > 0 else 0

        return {
            "bid_ask_spread": round(spread, 4),
            "book_imbalance": round(imbalance, 4),
            "bid_depth": round(bid_depth, 2),
            "ask_depth": round(ask_depth, 2),
            "best_bid": best_bid,
            "best_ask": best_ask,
        }

    # ------------------------------------------------------------------
    # Trading
    # ------------------------------------------------------------------

    def place_market_order(self, token_id: str, amount: float, side: str) -> dict:
        """
        Place a market order (requires authenticated client).
        token_id must be the specific CLOB token (YES or NO token).
        We always BUY the specific outcome token.
        """
        if not self._auth_client:
            return {"error": "Authenticated client not configured. Set POLYMARKET_PRIVATE_KEY in .env"}

        try:
            from py_clob_client.order_builder.constants import BUY
            from py_clob_client.clob_types import MarketOrderArgs

            # Always BUY the specific outcome token
            # (buying YES token = betting YES, buying NO token = betting NO)
            # CLOB API: taker amount max 2 decimals, maker amount max 4 decimals
            amount = round(amount, 2)
            order_args = MarketOrderArgs(
                token_id=token_id,
                amount=amount,
                side=BUY,
            )
            order = self._auth_client.create_market_order(order_args)
            result = self._auth_client.post_order(order)
            logger.info(f"Order placed: BUY ${amount} on token {token_id[:20]}... (side={side})")
            return {"ok": True, "result": result}

        except Exception as e:
            logger.error(f"Order failed: {e}")
            return {"error": str(e)}

    def place_sell_order(self, token_id: str, amount: float) -> dict:
        """
        Place a SELL market order for outcome tokens we already hold.
        Used for profit-taking / hedging.
        """
        if not self._auth_client:
            return {"error": "Authenticated client not configured. Set POLYMARKET_PRIVATE_KEY in .env"}

        try:
            from py_clob_client.order_builder.constants import SELL
            from py_clob_client.clob_types import MarketOrderArgs

            # CLOB API: taker amount max 2 decimals
            amount = round(amount, 2)
            order_args = MarketOrderArgs(
                token_id=token_id,
                amount=amount,
                side=SELL,
            )
            order = self._auth_client.create_market_order(order_args)
            result = self._auth_client.post_order(order)
            logger.info(f"Sell order response: {result}")

            # Check if order was actually matched/filled
            if isinstance(result, dict):
                status = result.get("status", "").lower()
                if status in ("matched", "filled", "delayed"):
                    return {"ok": True, "result": result, "filled": True}
                elif status in ("unmatched", "live"):
                    logger.warn(f"Sell order placed but NOT filled (status={status}). No buyer available.")
                    return {"ok": False, "error": f"Order not filled: {status}", "result": result}

            # Fallback: assume ok if we got a non-error response
            return {"ok": True, "result": result, "filled": True}

        except Exception as e:
            logger.error(f"Sell order failed: {e}")
            return {"error": str(e)}

    # ------------------------------------------------------------------
    # Search & Discovery (Gamma API)
    # ------------------------------------------------------------------

    def search_markets(self, query: str, limit: int = 20) -> list[dict]:
        """Search markets by text using Gamma public-search endpoint."""
        try:
            response = self._gamma.get("/public-search", params={"q": query})
            response.raise_for_status()
            data = response.json()

            # public-search returns grouped results (markets, events, profiles)
            markets_raw = data if isinstance(data, list) else data.get("markets", [])
            results = []
            for item in markets_raw[:limit]:
                results.append({
                    "id": item.get("conditionId", item.get("condition_id", item.get("id", ""))),
                    "question": item.get("question", ""),
                    "slug": item.get("slug", ""),
                    "volume": float(item.get("volumeNum", 0) or 0),
                    "liquidity": float(item.get("liquidityNum", 0) or 0),
                    "active": item.get("active", True),
                    "closed": item.get("closed", False),
                })
            logger.info(f"Search '{query}': found {len(results)} markets")
            return results
        except Exception as e:
            logger.error(f"Market search failed: {e}")
            return []

    def get_all_tags(self) -> list[dict]:
        """Fetch all available market tags/categories from Gamma API."""
        try:
            response = self._gamma.get("/tags")
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Tags fetch failed: {e}")
            return []

    # ------------------------------------------------------------------
    # Data API (positions, trades, price history)
    # ------------------------------------------------------------------

    def get_user_positions(self, wallet_address: str) -> list[dict]:
        """Fetch real on-chain positions for a wallet via Data API."""
        try:
            resp = httpx.get(
                "https://data-api.polymarket.com/positions",
                params={"user": wallet_address},
                timeout=30,
            )
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.error(f"Positions fetch failed: {e}")
            return []

    def get_user_trades(self, wallet_address: str, limit: int = 100) -> list[dict]:
        """Fetch trade history for a wallet via Data API."""
        try:
            resp = httpx.get(
                "https://data-api.polymarket.com/activity",
                params={"user": wallet_address, "limit": limit},
                timeout=30,
            )
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.error(f"User trades fetch failed: {e}")
            return []

    def get_price_history(self, token_id: str) -> list[dict]:
        """Fetch price history for a token via CLOB API."""
        try:
            resp = httpx.get(
                f"https://clob.polymarket.com/prices-history",
                params={"token_id": token_id},
                timeout=30,
            )
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.error(f"Price history fetch failed: {e}")
            return []

    def get_midpoint_price(self, token_id: str) -> float | None:
        """Get exact midpoint price (avg of best bid/ask) via CLOB API."""
        try:
            resp = httpx.get(
                "https://clob.polymarket.com/midpoint",
                params={"token_id": token_id},
                timeout=10,
            )
            resp.raise_for_status()
            data = resp.json()
            return float(data.get("mid", 0))
        except Exception as e:
            logger.error(f"Midpoint fetch failed: {e}")
            return None

    # ------------------------------------------------------------------
    # CLOB API fallback
    # ------------------------------------------------------------------

    def _fetch_live_markets(self, limit: int) -> list[dict]:
        """Fallback: Fetch markets from CLOB API using cursor pagination."""
        try:
            all_data = []
            next_cursor = None

            while len(all_data) < limit:
                if next_cursor:
                    response = self._public_client.get_markets(next_cursor=next_cursor)
                else:
                    response = self._public_client.get_markets()

                if isinstance(response, list):
                    all_data.extend(response)
                    break
                else:
                    page = response.get("data", [])
                    all_data.extend(page)
                    next_cursor = response.get("next_cursor")
                    if not next_cursor or not page:
                        break

            markets = []
            for item in all_data[:limit]:
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

            logger.info(f"CLOB fallback: fetched {len(markets)} markets")
            return markets

        except Exception as e:
            logger.error(f"Error fetching markets (CLOB): {e}")
            return []

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
        ]
