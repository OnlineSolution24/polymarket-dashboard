"""Tests for the backtesting engine."""

import numpy as np
import pandas as pd

from backtesting.data_loader import generate_synthetic_trades, generate_price_series
from backtesting.simulator import run_backtest
from backtesting.monte_carlo import run_monte_carlo
from backtesting.walk_forward import run_walk_forward
from backtesting.drawdown import analyze_drawdowns


def test_generate_synthetic_trades():
    """Test synthetic trade generation."""
    df = generate_synthetic_trades(n_trades=100)
    assert len(df) == 100
    assert "pnl" in df.columns
    assert "side" in df.columns
    assert "result" in df.columns
    assert set(df["result"].unique()).issubset({"win", "loss"})


def test_generate_price_series():
    """Test synthetic price series generation."""
    prices = generate_price_series(n_steps=500)
    assert len(prices) == 500
    assert all(0.01 <= p <= 0.99 for p in prices)


def test_run_backtest():
    """Test basic backtest execution."""
    trades = generate_synthetic_trades(n_trades=100)
    result = run_backtest(trades, initial_capital=1000.0, max_position_pct=0.05)

    assert result.total_trades > 0
    assert 0 <= result.win_rate <= 1
    assert len(result.equity_curve) > 0
    assert len(result.drawdown_curve) > 0
    assert result.max_drawdown_pct >= 0


def test_backtest_empty():
    """Test backtest with empty data."""
    result = run_backtest(pd.DataFrame(), initial_capital=1000.0)
    assert result.total_trades == 0
    assert result.total_pnl == 0


def test_monte_carlo():
    """Test Monte Carlo simulation."""
    pnls = np.random.normal(5, 20, 100)
    mc = run_monte_carlo(pnls, n_simulations=200, initial_capital=1000.0)

    assert mc.n_simulations == 200
    assert len(mc.final_capitals) == 200
    assert 0 <= mc.prob_profitable <= 1
    assert len(mc.equity_curves) > 0


def test_walk_forward():
    """Test walk-forward analysis."""
    trades = generate_synthetic_trades(n_trades=100)
    wf = run_walk_forward(trades, n_windows=5)

    assert wf.n_windows > 0
    assert 0 <= wf.consistency_score <= 1
    for w in wf.windows:
        assert w.train_trades > 0
        assert w.test_trades > 0


def test_walk_forward_insufficient_data():
    """Test walk-forward with too little data."""
    trades = generate_synthetic_trades(n_trades=5)
    wf = run_walk_forward(trades, n_windows=5)
    assert wf.n_windows == 0


def test_drawdown_analysis():
    """Test drawdown analysis."""
    equity = [1000, 1050, 1020, 980, 1010, 1080, 1060, 1100]
    dd = analyze_drawdowns(equity)

    assert dd.max_drawdown_abs > 0
    assert dd.max_drawdown_pct > 0
    assert 0 <= dd.time_in_drawdown_pct <= 1
    assert len(dd.drawdown_curve) == len(equity)


def test_drawdown_empty():
    """Test drawdown with minimal data."""
    dd = analyze_drawdowns([1000])
    assert dd.max_drawdown_abs == 0
