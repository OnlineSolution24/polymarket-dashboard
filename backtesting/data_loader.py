"""
Historical data loader for backtesting.
Loads from DB trades, generates synthetic data for simulation,
loads from blockchain Parquet files via DuckDB,
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


def load_blockchain_trades(
    wallet_address: str | None = None,
    limit: int = 10000,
) -> pd.DataFrame:
    """
    Load historical trades from blockchain Parquet data via DuckDB.

    Returns a DataFrame compatible with the backtesting simulator:
    columns: executed_at, pnl, amount_usd, result, price, side, market_id

    Args:
        wallet_address: If set, only load trades for this wallet.
        limit: Max trades to load.

    Falls back to empty DataFrame if blockchain data unavailable.
    """
    try:
        from services.historical_analytics import _has_data, _get_duckdb, _trades_glob

        if not _has_data():
            return pd.DataFrame()

        duckdb = _get_duckdb()
        if not duckdb:
            return pd.DataFrame()

        where = ""
        if wallet_address:
            where = f"WHERE LOWER(maker) = LOWER('{wallet_address}') OR LOWER(taker) = LOWER('{wallet_address}')"

        df = duckdb.sql(f"""
            SELECT
                block_number,
                transaction_hash,
                maker,
                taker,
                maker_asset_id,
                taker_asset_id,
                maker_amount,
                taker_amount,
                fee
            FROM read_parquet('{_trades_glob()}')
            {where}
            ORDER BY block_number
            LIMIT {limit}
        """).fetchdf()

        if df.empty:
            return pd.DataFrame()

        # Transform to backtesting format
        trades = []
        for _, row in df.iterrows():
            maker_asset = str(row["maker_asset_id"])
            taker_asset = str(row["taker_asset_id"])
            maker_amt = int(row["maker_amount"])
            taker_amt = int(row["taker_amount"])
            fee = int(row["fee"])

            # Determine side: maker_asset_id = '0' means maker pays USDC (BUY)
            is_buy = maker_asset == "0"

            if is_buy:
                amount_usdc = maker_amt / 1e6
                tokens = taker_amt / 1e6
                price = (maker_amt / taker_amt) if taker_amt > 0 else 0
                market_id = taker_asset
            else:
                amount_usdc = taker_amt / 1e6
                tokens = maker_amt / 1e6
                price = (taker_amt / maker_amt) if maker_amt > 0 else 0
                market_id = maker_asset

            fee_usdc = fee / 1e6

            trades.append({
                "market_id": market_id,
                "side": "YES" if is_buy else "NO",
                "amount_usd": round(amount_usdc, 4),
                "price": round(min(price, 1.0), 6),
                "fee_usd": round(fee_usdc, 4),
                "block_number": row["block_number"],
                "transaction_hash": row["transaction_hash"],
            })

        result = pd.DataFrame(trades)

        # For backtesting: compute PnL from round trips (buy→sell pairs per market)
        result = _compute_round_trip_pnl(result)

        return result

    except ImportError:
        return pd.DataFrame()
    except Exception as e:
        import logging
        logging.getLogger(__name__).error(f"Error loading blockchain trades: {e}")
        return pd.DataFrame()


def _compute_round_trip_pnl(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute PnL from buy/sell round trips per market_id.

    Groups trades by market_id, pairs buys with sells, and calculates
    PnL as: sell_proceeds - buy_cost - fees.

    Returns DataFrame with: executed_at, pnl, amount_usd, result, price, side, market_id
    """
    if df.empty or "market_id" not in df.columns:
        return pd.DataFrame()

    completed_trades = []

    for market_id, group in df.groupby("market_id"):
        buys = group[group["side"] == "YES"].to_dict("records")
        sells = group[group["side"] == "NO"].to_dict("records")

        # Match buys with sells
        for i, buy in enumerate(buys):
            if i >= len(sells):
                break

            sell = sells[i]
            buy_cost = buy["amount_usd"]
            sell_proceeds = sell["amount_usd"]
            fees = buy.get("fee_usd", 0) + sell.get("fee_usd", 0)
            pnl = sell_proceeds - buy_cost - fees

            completed_trades.append({
                "market_id": str(market_id),
                "side": "YES",
                "amount_usd": round(buy_cost, 2),
                "price": buy["price"],
                "pnl": round(pnl, 2),
                "result": "win" if pnl > 0 else "loss",
                "executed_at": datetime.utcnow().isoformat(),  # placeholder
                "block_number": sell["block_number"],
            })

    if not completed_trades:
        return pd.DataFrame()

    result = pd.DataFrame(completed_trades)
    result["executed_at"] = pd.to_datetime(result["executed_at"])
    result.sort_values("block_number", inplace=True)
    return result


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
