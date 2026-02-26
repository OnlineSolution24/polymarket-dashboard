"""Tests for the Polymarket client service."""

from config import AppConfig
from services.polymarket_client import PolymarketService


def test_mock_markets():
    """Test that mock markets are returned when API is unavailable."""
    config = AppConfig.from_env()
    service = PolymarketService(config)

    # Without real API credentials, should return mock data
    markets = service._mock_markets()

    assert len(markets) > 0
    assert "id" in markets[0]
    assert "question" in markets[0]
    assert "yes_price" in markets[0]
    assert "no_price" in markets[0]
    assert "volume" in markets[0]

    # Prices should be between 0 and 1
    for m in markets:
        assert 0 <= m["yes_price"] <= 1
        assert 0 <= m["no_price"] <= 1


def test_fetch_markets_returns_list():
    """Test that fetch_markets always returns a list."""
    config = AppConfig.from_env()
    service = PolymarketService(config)
    markets = service.fetch_markets()

    assert isinstance(markets, list)


def test_order_book_without_auth():
    """Test order book returns empty without auth."""
    config = AppConfig.from_env()
    service = PolymarketService(config)
    book = service.get_order_book("test_token")

    assert isinstance(book, dict)


def test_place_order_without_auth():
    """Test that placing an order without auth returns error."""
    config = AppConfig(polymarket_private_key="")
    service = PolymarketService(config)
    result = service.place_market_order("token", 10.0, "YES")

    assert "error" in result
