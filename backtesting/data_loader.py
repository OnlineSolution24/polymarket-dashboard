"""
Historical data loader for backtesting.
Loads from DB trades, generates synthetic data for simulation,
or fetches from Polymarket API historical endpoints.
"""

import random
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

from db import engine


def load_trade_history() -> pd.DataFrame:
    """Load completed trades from DB as a DataFrame."""
    rows = engine.query(
        "SELECT * FROM trades WHERE result IS NOT NULL AND pnl IS NOT NULL ORDER BY executed_at"
    )
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    df["executed_at"] = pd.to_datetime(df["executed_at"])
    return df


def load_market_snapshots() -> pd.DataFrame:
    """Load market data from DB."""
    rows = engine.query("SELECT * FROM markets ORDER BY last_updated")
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    df["last_updated"] = pd.to_datetime(df["last_updated"])
    return df


def generate_synthetic_trades(
    n_trades: int = 200,
    win_rate: float = 0.55,
    avg_bet: float = 25.0,
    avg_odds: float = 0.55,
    start_date: str | None = None,
) -> pd.DataFrame:
    """
    Generate synthetic trade data for backtesting when real data is insufficient.
    Simulates a prediction market trading strategy.
    """
    rng = np.random.default_rng(42)

    if start_date is None:
        start = datetime.utcnow() - timedelta(days=180)
    else:
        start = datetime.fromisoformat(start_date)

    trades = []
    for i in range(n_trades):
        # Random time progression
        trade_time = start + timedelta(hours=int(rng.integers(1, 48)))
        start = trade_time

        # Bet parameters
        bet_size = max(5, rng.normal(avg_bet, avg_bet * 0.3))
        entry_price = max(0.1, min(0.9, rng.normal(avg_odds, 0.15)))
        side = "YES" if rng.random() > 0.4 else "NO"

        # Outcome
        is_win = rng.random() < win_rate
        if is_win:
            pnl = bet_size * (1 / entry_price - 1) * rng.uniform(0.7, 1.0)
        else:
            pnl = -bet_size

        # Simulated sentiment and volume
        sentiment = rng.normal(0, 0.3)
        volume = rng.lognormal(12, 1)

        trades.append({
            "id": i + 1,
            "market_id": f"sim_{i:04d}",
            "market_question": f"Simulated Market {i+1}",
            "side": side,
            "amount_usd": round(bet_size, 2),
            "price": round(entry_price, 4),
            "result": "win" if is_win else "loss",
            "pnl": round(pnl, 2),
            "executed_at": trade_time.isoformat(),
            "sentiment_score": round(sentiment, 3),
            "volume": round(volume, 0),
        })

    return pd.DataFrame(trades)


def generate_price_series(
    n_steps: int = 1000,
    initial_price: float = 0.5,
    drift: float = 0.0001,
    volatility: float = 0.02,
) -> pd.Series:
    """Generate a synthetic price series using geometric Brownian motion."""
    rng = np.random.default_rng()
    returns = rng.normal(drift, volatility, n_steps)
    prices = initial_price * np.exp(np.cumsum(returns))
    prices = np.clip(prices, 0.01, 0.99)
    return pd.Series(prices)
