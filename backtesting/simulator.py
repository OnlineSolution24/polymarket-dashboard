"""
Core backtesting engine.
Runs strategies against historical or synthetic data and produces performance metrics.
"""

from dataclasses import dataclass, field

import numpy as np
import pandas as pd


@dataclass
class BacktestResult:
    """Container for backtest results."""
    trades: pd.DataFrame
    total_pnl: float = 0.0
    win_rate: float = 0.0
    total_trades: int = 0
    wins: int = 0
    losses: int = 0
    max_drawdown: float = 0.0
    max_drawdown_pct: float = 0.0
    sharpe_ratio: float = 0.0
    profit_factor: float = 0.0
    avg_win: float = 0.0
    avg_loss: float = 0.0
    equity_curve: list[float] = field(default_factory=list)
    drawdown_curve: list[float] = field(default_factory=list)


def run_backtest(
    trades: pd.DataFrame,
    initial_capital: float = 1000.0,
    max_position_pct: float = 0.05,
    stop_loss_pct: float | None = None,
) -> BacktestResult:
    """
    Run a backtest on trade data.

    Args:
        trades: DataFrame with columns [pnl, amount_usd, result, executed_at]
        initial_capital: Starting capital
        max_position_pct: Maximum position size as % of capital
        stop_loss_pct: Stop loss as % of capital (None = no stop)
    """
    if trades.empty:
        return BacktestResult(trades=trades)

    df = trades.sort_values("executed_at").copy()
    capital = initial_capital
    equity_curve = [capital]
    peak = capital

    results = []
    for _, trade in df.iterrows():
        pnl = trade.get("pnl", 0) or 0

        # Apply position sizing
        max_bet = capital * max_position_pct
        actual_bet = min(trade.get("amount_usd", max_bet), max_bet)
        scale = actual_bet / trade.get("amount_usd", actual_bet) if trade.get("amount_usd", 0) > 0 else 1
        adjusted_pnl = pnl * scale

        capital += adjusted_pnl
        equity_curve.append(capital)
        peak = max(peak, capital)

        results.append({
            **trade.to_dict(),
            "adjusted_pnl": adjusted_pnl,
            "capital_after": capital,
        })

        # Stop loss check
        if stop_loss_pct and capital < initial_capital * (1 - stop_loss_pct):
            break

    result_df = pd.DataFrame(results)

    # Compute metrics
    pnls = result_df["adjusted_pnl"].values if not result_df.empty else np.array([])
    wins_mask = pnls > 0
    losses_mask = pnls < 0

    total_pnl = float(pnls.sum())
    wins = int(wins_mask.sum())
    losses = int(losses_mask.sum())
    total_trades = len(pnls)
    win_rate = wins / total_trades if total_trades > 0 else 0

    avg_win = float(pnls[wins_mask].mean()) if wins > 0 else 0
    avg_loss = float(pnls[losses_mask].mean()) if losses > 0 else 0

    gross_profit = float(pnls[wins_mask].sum()) if wins > 0 else 0
    gross_loss = abs(float(pnls[losses_mask].sum())) if losses > 0 else 0
    profit_factor = gross_profit / gross_loss if gross_loss > 0 else float("inf") if gross_profit > 0 else 0

    # Sharpe ratio (annualized, assuming ~250 trading days)
    if len(pnls) > 1 and np.std(pnls) > 0:
        sharpe_ratio = float(np.mean(pnls) / np.std(pnls) * np.sqrt(250))
    else:
        sharpe_ratio = 0.0

    # Drawdown
    equity = np.array(equity_curve)
    running_max = np.maximum.accumulate(equity)
    drawdown = running_max - equity
    drawdown_pct = drawdown / running_max
    max_drawdown = float(drawdown.max())
    max_drawdown_pct = float(drawdown_pct.max())
    drawdown_curve = drawdown.tolist()

    return BacktestResult(
        trades=result_df,
        total_pnl=total_pnl,
        win_rate=win_rate,
        total_trades=total_trades,
        wins=wins,
        losses=losses,
        max_drawdown=max_drawdown,
        max_drawdown_pct=max_drawdown_pct,
        sharpe_ratio=sharpe_ratio,
        profit_factor=profit_factor,
        avg_win=avg_win,
        avg_loss=avg_loss,
        equity_curve=equity_curve,
        drawdown_curve=drawdown_curve,
    )
