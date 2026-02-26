"""
Feature engineering for ML training.
Extracts features from trade history, market data, and sentiment scores.
"""

import numpy as np
import pandas as pd

from db import engine


def build_feature_matrix() -> tuple[pd.DataFrame, pd.Series]:
    """
    Build feature matrix X and target y from completed trades.
    Returns (X, y) where y = 1 for win, 0 for loss.
    """
    trades = engine.query("""
        SELECT t.*, m.volume, m.liquidity, m.sentiment_score, m.calculated_edge,
               m.yes_price, m.no_price, m.category
        FROM trades t
        LEFT JOIN markets m ON t.market_id = m.id
        WHERE t.result IS NOT NULL AND t.pnl IS NOT NULL
        ORDER BY t.executed_at
    """)

    if not trades or len(trades) < 10:
        return pd.DataFrame(), pd.Series(dtype=float)

    df = pd.DataFrame(trades)

    # Target: 1 = win, 0 = loss
    y = (df["result"] == "win").astype(int)

    # Features
    features = pd.DataFrame()

    # Price features
    features["entry_price"] = df["price"].fillna(0.5)
    features["yes_price"] = df["yes_price"].fillna(0.5)
    features["no_price"] = df["no_price"].fillna(0.5)
    features["price_deviation"] = abs(features["yes_price"] - 0.5)
    features["is_yes"] = (df["side"] == "YES").astype(int)

    # Market features
    features["volume_log"] = np.log1p(df["volume"].fillna(0))
    features["liquidity_log"] = np.log1p(df["liquidity"].fillna(0))
    features["vol_liq_ratio"] = (df["volume"].fillna(0) / df["liquidity"].fillna(1).replace(0, 1))

    # Sentiment
    features["sentiment"] = df["sentiment_score"].fillna(0)
    features["sentiment_abs"] = features["sentiment"].abs()

    # Edge
    features["edge"] = df["calculated_edge"].fillna(0)
    features["edge_abs"] = features["edge"].abs()

    # Bet size features
    features["bet_size"] = df["amount_usd"].fillna(0)
    features["bet_size_log"] = np.log1p(features["bet_size"])

    # Time features
    df["executed_at"] = pd.to_datetime(df["executed_at"])
    features["hour"] = df["executed_at"].dt.hour
    features["day_of_week"] = df["executed_at"].dt.dayofweek
    features["is_weekend"] = (features["day_of_week"] >= 5).astype(int)

    # Rolling performance (last N trades)
    features["rolling_win_rate_5"] = y.rolling(5, min_periods=1).mean().shift(1).fillna(0.5)
    features["rolling_win_rate_10"] = y.rolling(10, min_periods=1).mean().shift(1).fillna(0.5)
    features["rolling_pnl_5"] = df["pnl"].rolling(5, min_periods=1).sum().shift(1).fillna(0)

    # Time decay: days since market was last updated
    if "last_updated" in df.columns:
        df["last_updated"] = pd.to_datetime(df["last_updated"], errors="coerce")
        features["days_to_expiry"] = (pd.to_datetime(df.get("end_date", None), errors="coerce") - df["executed_at"]).dt.days.fillna(30)
    else:
        features["days_to_expiry"] = 30

    # Category encoding
    if "category" in df.columns:
        cat_dummies = pd.get_dummies(df["category"].fillna("unknown"), prefix="cat")
        features = pd.concat([features, cat_dummies], axis=1)

    # Drop any NaN rows
    valid = features.notna().all(axis=1) & y.notna()
    features = features[valid].reset_index(drop=True)
    y = y[valid].reset_index(drop=True)

    return features, y


def get_feature_names() -> list[str]:
    """Get the list of feature names used in training."""
    X, _ = build_feature_matrix()
    return list(X.columns) if not X.empty else []
