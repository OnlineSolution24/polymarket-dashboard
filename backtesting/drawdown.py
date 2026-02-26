"""
Drawdown analysis module.
Computes detailed drawdown statistics and identifies drawdown periods.
"""

from dataclasses import dataclass

import numpy as np
import pandas as pd


@dataclass
class DrawdownPeriod:
    """A single drawdown period."""
    start_idx: int
    end_idx: int
    recovery_idx: int | None
    peak_value: float
    trough_value: float
    drawdown_abs: float
    drawdown_pct: float
    duration_trades: int
    recovery_trades: int | None


@dataclass
class DrawdownResult:
    """Full drawdown analysis results."""
    drawdown_curve: list[float]
    drawdown_pct_curve: list[float]
    max_drawdown_abs: float
    max_drawdown_pct: float
    avg_drawdown_pct: float
    time_in_drawdown_pct: float   # % of time spent in drawdown
    longest_drawdown_trades: int
    top_drawdowns: list[DrawdownPeriod]


def analyze_drawdowns(
    equity_curve: list[float],
    top_n: int = 5,
) -> DrawdownResult:
    """
    Perform detailed drawdown analysis on an equity curve.

    Args:
        equity_curve: List of equity values over time
        top_n: Number of top drawdowns to return
    """
    equity = np.array(equity_curve)
    if len(equity) < 2:
        return DrawdownResult(
            drawdown_curve=[], drawdown_pct_curve=[],
            max_drawdown_abs=0, max_drawdown_pct=0,
            avg_drawdown_pct=0, time_in_drawdown_pct=0,
            longest_drawdown_trades=0, top_drawdowns=[],
        )

    running_max = np.maximum.accumulate(equity)
    drawdown_abs = running_max - equity
    drawdown_pct = np.where(running_max > 0, drawdown_abs / running_max, 0)

    max_dd_abs = float(drawdown_abs.max())
    max_dd_pct = float(drawdown_pct.max())

    # Time in drawdown
    in_dd = drawdown_abs > 0
    time_in_dd_pct = float(in_dd.sum() / len(in_dd)) if len(in_dd) > 0 else 0

    # Average drawdown when in drawdown
    dd_when_active = drawdown_pct[in_dd]
    avg_dd_pct = float(dd_when_active.mean()) if len(dd_when_active) > 0 else 0

    # Identify drawdown periods
    periods = _find_drawdown_periods(equity, running_max, drawdown_abs, drawdown_pct)

    # Sort by magnitude and take top N
    periods.sort(key=lambda p: p.drawdown_abs, reverse=True)
    top_periods = periods[:top_n]

    longest = max((p.duration_trades for p in periods), default=0) if periods else 0

    return DrawdownResult(
        drawdown_curve=drawdown_abs.tolist(),
        drawdown_pct_curve=drawdown_pct.tolist(),
        max_drawdown_abs=max_dd_abs,
        max_drawdown_pct=max_dd_pct,
        avg_drawdown_pct=avg_dd_pct,
        time_in_drawdown_pct=time_in_dd_pct,
        longest_drawdown_trades=longest,
        top_drawdowns=top_periods,
    )


def _find_drawdown_periods(
    equity: np.ndarray,
    running_max: np.ndarray,
    dd_abs: np.ndarray,
    dd_pct: np.ndarray,
) -> list[DrawdownPeriod]:
    """Identify individual drawdown periods."""
    periods = []
    in_drawdown = False
    start = 0
    peak_val = 0.0
    trough_val = float("inf")
    trough_idx = 0

    for i in range(len(equity)):
        if dd_abs[i] > 0 and not in_drawdown:
            # Start of drawdown
            in_drawdown = True
            start = i - 1 if i > 0 else 0
            peak_val = running_max[i]
            trough_val = equity[i]
            trough_idx = i

        elif dd_abs[i] > 0 and in_drawdown:
            # Continue drawdown
            if equity[i] < trough_val:
                trough_val = equity[i]
                trough_idx = i

        elif dd_abs[i] == 0 and in_drawdown:
            # Recovery
            in_drawdown = False
            periods.append(DrawdownPeriod(
                start_idx=start,
                end_idx=trough_idx,
                recovery_idx=i,
                peak_value=peak_val,
                trough_value=trough_val,
                drawdown_abs=peak_val - trough_val,
                drawdown_pct=(peak_val - trough_val) / peak_val if peak_val > 0 else 0,
                duration_trades=trough_idx - start,
                recovery_trades=i - trough_idx,
            ))

    # Handle ongoing drawdown
    if in_drawdown:
        periods.append(DrawdownPeriod(
            start_idx=start,
            end_idx=trough_idx,
            recovery_idx=None,
            peak_value=peak_val,
            trough_value=trough_val,
            drawdown_abs=peak_val - trough_val,
            drawdown_pct=(peak_val - trough_val) / peak_val if peak_val > 0 else 0,
            duration_trades=trough_idx - start,
            recovery_trades=None,
        ))

    return periods
