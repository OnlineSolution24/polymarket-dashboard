"""
Monte Carlo simulation for backtesting.
Generates multiple random permutations of trade sequences
to estimate confidence intervals for strategy performance.
"""

import numpy as np
import pandas as pd
from dataclasses import dataclass


@dataclass
class MonteCarloResult:
    """Monte Carlo simulation results."""
    n_simulations: int
    final_capitals: list[float]
    max_drawdowns: list[float]
    median_pnl: float
    mean_pnl: float
    pnl_5th: float          # 5th percentile (worst case)
    pnl_95th: float         # 95th percentile (best case)
    prob_profitable: float  # % of simulations that ended profitable
    median_max_dd: float
    worst_max_dd: float
    equity_curves: list[list[float]]  # Sampled curves for plotting


def run_monte_carlo(
    pnls: np.ndarray,
    n_simulations: int = 1000,
    initial_capital: float = 1000.0,
    n_curves_to_store: int = 50,
) -> MonteCarloResult:
    """
    Run Monte Carlo simulation by shuffling trade PnLs.

    Args:
        pnls: Array of trade PnLs
        n_simulations: Number of random permutations
        initial_capital: Starting capital
        n_curves_to_store: How many equity curves to keep for plotting
    """
    rng = np.random.default_rng(42)
    n_trades = len(pnls)

    final_capitals = []
    max_drawdowns = []
    equity_curves = []

    for i in range(n_simulations):
        # Shuffle trade order
        shuffled = rng.permutation(pnls)

        # Build equity curve
        equity = initial_capital + np.cumsum(shuffled)
        equity = np.insert(equity, 0, initial_capital)

        final_cap = float(equity[-1])
        final_capitals.append(final_cap)

        # Max drawdown
        running_max = np.maximum.accumulate(equity)
        drawdown = running_max - equity
        max_dd = float(drawdown.max())
        max_drawdowns.append(max_dd)

        # Store some curves for plotting
        if i < n_curves_to_store:
            equity_curves.append(equity.tolist())

    finals = np.array(final_capitals)
    dds = np.array(max_drawdowns)

    return MonteCarloResult(
        n_simulations=n_simulations,
        final_capitals=final_capitals,
        max_drawdowns=max_drawdowns,
        median_pnl=float(np.median(finals) - initial_capital),
        mean_pnl=float(np.mean(finals) - initial_capital),
        pnl_5th=float(np.percentile(finals, 5) - initial_capital),
        pnl_95th=float(np.percentile(finals, 95) - initial_capital),
        prob_profitable=float(np.mean(finals > initial_capital)),
        median_max_dd=float(np.median(dds)),
        worst_max_dd=float(np.max(dds)),
        equity_curves=equity_curves,
    )
