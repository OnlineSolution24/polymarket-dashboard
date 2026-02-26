"""
Walk-Forward Analysis.
Splits trade history into rolling train/test windows
to evaluate strategy robustness over time.
"""

from dataclasses import dataclass

import numpy as np
import pandas as pd


@dataclass
class WalkForwardWindow:
    """Results for a single walk-forward window."""
    window_id: int
    train_start: str
    train_end: str
    test_start: str
    test_end: str
    train_trades: int
    test_trades: int
    train_win_rate: float
    test_win_rate: float
    train_pnl: float
    test_pnl: float
    test_sharpe: float


@dataclass
class WalkForwardResult:
    """Aggregate walk-forward analysis results."""
    windows: list[WalkForwardWindow]
    n_windows: int
    avg_test_win_rate: float
    avg_test_pnl: float
    consistency_score: float   # % of windows where test performance > 0
    degradation: float         # avg train_win_rate - avg test_win_rate


def run_walk_forward(
    trades: pd.DataFrame,
    n_windows: int = 5,
    train_ratio: float = 0.7,
) -> WalkForwardResult:
    """
    Run walk-forward analysis.

    Splits the trade history into n overlapping windows,
    each with a train portion and a test portion.

    Args:
        trades: DataFrame sorted by executed_at with 'pnl' and 'result' columns
        n_windows: Number of walk-forward windows
        train_ratio: Fraction of each window used for training
    """
    if trades.empty or len(trades) < 20:
        return WalkForwardResult(
            windows=[], n_windows=0,
            avg_test_win_rate=0, avg_test_pnl=0,
            consistency_score=0, degradation=0,
        )

    df = trades.sort_values("executed_at").reset_index(drop=True)
    n = len(df)

    # Window size with overlap
    window_size = n // n_windows
    if window_size < 10:
        n_windows = max(1, n // 10)
        window_size = n // n_windows

    step = max(1, (n - window_size) // max(1, n_windows - 1))

    windows = []
    for i in range(n_windows):
        start = i * step
        end = min(start + window_size, n)
        if end - start < 10:
            continue

        split = start + int((end - start) * train_ratio)
        train = df.iloc[start:split]
        test = df.iloc[split:end]

        if len(test) < 3:
            continue

        train_pnls = train["pnl"].fillna(0)
        test_pnls = test["pnl"].fillna(0)

        train_wins = (train_pnls > 0).sum()
        test_wins = (test_pnls > 0).sum()

        train_wr = train_wins / len(train) if len(train) > 0 else 0
        test_wr = test_wins / len(test) if len(test) > 0 else 0

        # Test Sharpe
        if len(test_pnls) > 1 and test_pnls.std() > 0:
            test_sharpe = float(test_pnls.mean() / test_pnls.std() * np.sqrt(250))
        else:
            test_sharpe = 0.0

        windows.append(WalkForwardWindow(
            window_id=i,
            train_start=str(train["executed_at"].iloc[0])[:10] if len(train) > 0 else "",
            train_end=str(train["executed_at"].iloc[-1])[:10] if len(train) > 0 else "",
            test_start=str(test["executed_at"].iloc[0])[:10] if len(test) > 0 else "",
            test_end=str(test["executed_at"].iloc[-1])[:10] if len(test) > 0 else "",
            train_trades=len(train),
            test_trades=len(test),
            train_win_rate=train_wr,
            test_win_rate=test_wr,
            train_pnl=float(train_pnls.sum()),
            test_pnl=float(test_pnls.sum()),
            test_sharpe=test_sharpe,
        ))

    if not windows:
        return WalkForwardResult(
            windows=[], n_windows=0,
            avg_test_win_rate=0, avg_test_pnl=0,
            consistency_score=0, degradation=0,
        )

    avg_test_wr = np.mean([w.test_win_rate for w in windows])
    avg_test_pnl = np.mean([w.test_pnl for w in windows])
    avg_train_wr = np.mean([w.train_win_rate for w in windows])
    profitable_windows = sum(1 for w in windows if w.test_pnl > 0)
    consistency = profitable_windows / len(windows)

    return WalkForwardResult(
        windows=windows,
        n_windows=len(windows),
        avg_test_win_rate=float(avg_test_wr),
        avg_test_pnl=float(avg_test_pnl),
        consistency_score=float(consistency),
        degradation=float(avg_train_wr - avg_test_wr),
    )
