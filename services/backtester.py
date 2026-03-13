"""
Snapshot-based Backtester.
Replays the 30k+ market_snapshots chronologically to simulate
strategy performance without needing actual trade history.

Key difference from backtest_service.py:
- backtest_service uses completed trades (sparse)
- this module uses raw price snapshots (30k+) to simulate entries/exits
"""

import json
import logging
import math
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Optional

from db import engine
from services.strategy_evaluator import evaluate_rules

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class SimulatedTrade:
    market_id: str
    market_question: str
    side: str
    entry_price: float
    entry_time: str
    amount_usd: float
    shares: float  # amount / price
    exit_price: Optional[float] = None
    exit_time: Optional[str] = None
    pnl: Optional[float] = None
    pnl_pct: Optional[float] = None
    result: Optional[str] = None  # "win", "loss", "open"
    exit_reason: Optional[str] = None  # "profit_target", "stop_loss", "expired"


@dataclass
class BacktestMetrics:
    strategy_id: str
    strategy_name: str
    start_date: str
    end_date: str
    initial_capital: float
    final_capital: float
    total_pnl: float
    total_pnl_pct: float
    win_rate: float
    total_trades: int
    wins: int
    losses: int
    open_positions: int
    max_drawdown_pct: float
    sharpe_ratio: float
    profit_factor: float
    avg_win: float
    avg_loss: float
    avg_hold_hours: float
    equity_curve: list = field(default_factory=list)
    trades: list = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "strategy_id": self.strategy_id,
            "strategy_name": self.strategy_name,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "initial_capital": self.initial_capital,
            "final_capital": round(self.final_capital, 2),
            "total_pnl": round(self.total_pnl, 2),
            "total_pnl_pct": round(self.total_pnl_pct, 2),
            "win_rate": round(self.win_rate, 4),
            "total_trades": self.total_trades,
            "wins": self.wins,
            "losses": self.losses,
            "open_positions": self.open_positions,
            "max_drawdown_pct": round(self.max_drawdown_pct, 4),
            "sharpe_ratio": round(self.sharpe_ratio, 3),
            "profit_factor": round(self.profit_factor, 3),
            "avg_win": round(self.avg_win, 4),
            "avg_loss": round(self.avg_loss, 4),
            "avg_hold_hours": round(self.avg_hold_hours, 1),
            "equity_curve": self.equity_curve,
            "trades": self.trades,
        }


# ---------------------------------------------------------------------------
# Main Backtester class
# ---------------------------------------------------------------------------

class SnapshotBacktester:
    """Replays market snapshots to simulate strategy trading."""

    def __init__(
        self,
        strategy_id: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        initial_capital: float = 1000.0,
        max_positions: int = 20,
        max_position_pct: float = 0.05,
    ):
        self.strategy_id = strategy_id
        self.start_date = start_date
        self.end_date = end_date
        self.initial_capital = initial_capital
        self.capital = initial_capital
        self.max_positions = max_positions
        self.max_position_pct = max_position_pct

        self.positions: list = []
        self.closed_trades: list = []
        self.equity_curve: list = []

        self._strategy = None
        self._definition = None

    def run(self) -> BacktestMetrics:
        """Execute the backtest."""
        # 1. Load strategy
        self._strategy = engine.query_one(
            "SELECT * FROM strategies WHERE id = ?", (self.strategy_id,)
        )
        if not self._strategy:
            raise ValueError(f"Strategy {self.strategy_id} not found")

        try:
            self._definition = json.loads(self._strategy["definition"])
        except (json.JSONDecodeError, TypeError):
            raise ValueError("Invalid strategy definition JSON")

        entry_rules = self._definition.get("entry_rules", [])
        exit_rules = self._definition.get("exit_rules", [])
        trade_params = self._definition.get("trade_params", {})

        # Extract exit thresholds from exit_rules
        profit_target = None
        stop_loss = None
        max_hold_hours = None
        for rule in exit_rules:
            f = rule.get("field", "")
            op = rule.get("op", "")
            val = rule.get("value")
            if f == "pnl_pct" and op == "gte" and val is not None:
                profit_target = val / 100.0  # e.g. 15 -> 0.15
            elif f == "pnl_pct" and op == "lte" and val is not None:
                stop_loss = val / 100.0  # e.g. -10 -> -0.10
            elif f == "hold_time_hours" and op == "gte" and val is not None:
                max_hold_hours = val

        # Defaults if no exit rules defined
        if profit_target is None:
            profit_target = 0.10  # 10%
        if stop_loss is None:
            stop_loss = -0.50  # -50%

        side = trade_params.get("side", "YES")
        sizing_method = trade_params.get("sizing_method", "fixed_amount")
        sizing_value = trade_params.get("sizing_value", 5)

        # 2. Load snapshots in date range
        snapshots = self._load_snapshots()
        if not snapshots:
            return self._empty_result()

        # 3. Load market metadata (for fields not in snapshots like whale data)
        market_meta = self._load_market_metadata()

        # 4. Group snapshots by timestamp
        timestamps = sorted(set(s["snapshot_at"] for s in snapshots))

        # Build lookup: timestamp -> list of snapshots
        snap_by_time = {}
        for s in snapshots:
            t = s["snapshot_at"]
            if t not in snap_by_time:
                snap_by_time[t] = []
            snap_by_time[t].append(s)

        # 5. Replay through time
        for ts in timestamps:
            current_snapshots = snap_by_time[ts]
            snap_lookup = {s["market_id"]: s for s in current_snapshots}

            # 5a. Check existing positions for exit
            self._check_exits(
                snap_lookup, ts, profit_target, stop_loss, max_hold_hours, side
            )

            # 5b. Evaluate entry rules for new positions
            if len(self.positions) < self.max_positions:
                for snap in current_snapshots:
                    mid = snap["market_id"]
                    if not mid:
                        continue

                    # Skip if already in position for this market
                    if any(p.market_id == mid for p in self.positions):
                        continue

                    # Build virtual market dict merging snapshot + metadata
                    market_data = self._build_market_dict(snap, market_meta.get(mid, {}))

                    # Evaluate entry rules
                    if evaluate_rules(market_data, entry_rules):
                        # Calculate position size
                        amount = self._calc_position_size(sizing_method, sizing_value)
                        if amount <= 0 or amount > self.capital:
                            continue

                        entry_price = (
                            snap["yes_price"] if side == "YES" else snap["no_price"]
                        )
                        if not entry_price or entry_price <= 0 or entry_price >= 1:
                            continue

                        shares = amount / entry_price
                        trade = SimulatedTrade(
                            market_id=mid,
                            market_question=market_meta.get(mid, {}).get("question", mid[:20]),
                            side=side,
                            entry_price=entry_price,
                            entry_time=ts,
                            amount_usd=amount,
                            shares=shares,
                        )
                        self.positions.append(trade)
                        self.capital -= amount

                        if len(self.positions) >= self.max_positions:
                            break

            # Record equity at each timestamp
            portfolio_value = self._portfolio_value(snap_lookup, side)
            self.equity_curve.append({
                "time": ts,
                "capital": round(self.capital, 2),
                "positions_value": round(portfolio_value, 2),
                "total": round(self.capital + portfolio_value, 2),
            })

        # 6. Mark remaining positions as open
        for p in self.positions:
            p.result = "open"

        # 7. Compute metrics
        return self._compute_metrics()

    # -------------------------------------------------------------------
    # Private helpers
    # -------------------------------------------------------------------

    def _load_snapshots(self) -> list:
        """Load market snapshots for the backtest period."""
        query = "SELECT * FROM market_snapshots WHERE market_id != ''"
        params = []

        if self.start_date:
            query += " AND snapshot_at >= ?"
            params.append(self.start_date)
        if self.end_date:
            query += " AND snapshot_at <= ?"
            params.append(self.end_date)

        query += " ORDER BY snapshot_at ASC"
        return engine.query(query, tuple(params))

    def _load_market_metadata(self) -> dict:
        """Load market metadata indexed by market_id."""
        rows = engine.query("SELECT * FROM markets")
        return {r["id"]: r for r in rows}

    def _build_market_dict(self, snapshot: dict, meta: dict) -> dict:
        """Merge snapshot data with market metadata for rule evaluation."""
        result = dict(meta) if meta else {}
        # Snapshot fields override current market values
        result["yes_price"] = snapshot.get("yes_price") or result.get("yes_price", 0)
        result["no_price"] = snapshot.get("no_price") or result.get("no_price", 0)
        result["volume"] = snapshot.get("volume") or result.get("volume", 0)
        result["liquidity"] = snapshot.get("liquidity") or result.get("liquidity", 0)
        if snapshot.get("sentiment_score") is not None:
            result["sentiment_score"] = snapshot["sentiment_score"]
        return result

    def _calc_position_size(self, method: str, value: float) -> float:
        """Calculate position size based on strategy config."""
        if method == "fixed_amount":
            return min(value, self.capital * self.max_position_pct)
        elif method == "fixed_pct":
            return self.capital * value
        elif method == "kelly":
            return self.capital * min(value, self.max_position_pct)
        return min(value, self.capital * self.max_position_pct)

    def _check_exits(
        self,
        snap_lookup: dict,
        current_time: str,
        profit_target: float,
        stop_loss: float,
        max_hold_hours: Optional[float],
        side: str,
    ):
        """Check all open positions for exit conditions."""
        to_close = []
        for i, pos in enumerate(self.positions):
            snap = snap_lookup.get(pos.market_id)
            if not snap:
                continue

            current_price = (
                snap["yes_price"] if side == "YES" else snap["no_price"]
            )
            if not current_price or current_price <= 0:
                continue

            # Calculate unrealized PnL %
            pnl_pct = (current_price - pos.entry_price) / pos.entry_price

            exit_reason = None

            # Check profit target
            if pnl_pct >= profit_target:
                exit_reason = "profit_target"

            # Check stop loss
            elif pnl_pct <= stop_loss:
                exit_reason = "stop_loss"

            # Check max hold time
            elif max_hold_hours:
                try:
                    entry_dt = datetime.fromisoformat(pos.entry_time)
                    current_dt = datetime.fromisoformat(current_time)
                    hold_hours = (current_dt - entry_dt).total_seconds() / 3600
                    if hold_hours >= max_hold_hours:
                        exit_reason = "expired"
                except (ValueError, TypeError):
                    pass

            if exit_reason:
                pos.exit_price = current_price
                pos.exit_time = current_time
                pos.pnl_pct = pnl_pct
                pos.pnl = pos.shares * (current_price - pos.entry_price)
                pos.result = "win" if pos.pnl > 0 else "loss"
                pos.exit_reason = exit_reason

                # Return capital + pnl
                self.capital += pos.amount_usd + pos.pnl

                to_close.append(i)

        # Remove closed positions (reverse order to preserve indices)
        for i in sorted(to_close, reverse=True):
            self.closed_trades.append(self.positions.pop(i))

    def _portfolio_value(self, snap_lookup: dict, side: str) -> float:
        """Calculate current value of open positions."""
        total = 0.0
        for pos in self.positions:
            snap = snap_lookup.get(pos.market_id)
            if snap:
                current_price = (
                    snap["yes_price"] if side == "YES" else snap["no_price"]
                )
                if current_price and current_price > 0:
                    total += pos.shares * current_price
                else:
                    total += pos.amount_usd  # fallback
            else:
                total += pos.amount_usd  # no data, use cost basis
        return total

    def _compute_metrics(self) -> BacktestMetrics:
        """Compute final backtest metrics from closed trades."""
        all_closed = self.closed_trades
        pnls = [t.pnl for t in all_closed if t.pnl is not None]

        total_pnl = sum(pnls) if pnls else 0.0
        wins = sum(1 for p in pnls if p > 0)
        losses = sum(1 for p in pnls if p <= 0)
        total_trades = len(pnls)
        win_rate = wins / total_trades if total_trades > 0 else 0.0

        win_pnls = [p for p in pnls if p > 0]
        loss_pnls = [p for p in pnls if p <= 0]
        avg_win = sum(win_pnls) / len(win_pnls) if win_pnls else 0.0
        avg_loss = sum(loss_pnls) / len(loss_pnls) if loss_pnls else 0.0

        gross_profit = sum(win_pnls) if win_pnls else 0.0
        gross_loss = abs(sum(loss_pnls)) if loss_pnls else 0.0
        profit_factor = (
            gross_profit / gross_loss
            if gross_loss > 0
            else (float("inf") if gross_profit > 0 else 0.0)
        )

        # Sharpe ratio (annualized)
        if len(pnls) > 1:
            import statistics
            mean_pnl = statistics.mean(pnls)
            std_pnl = statistics.stdev(pnls)
            sharpe_ratio = (mean_pnl / std_pnl * math.sqrt(365)) if std_pnl > 0 else 0.0
        else:
            sharpe_ratio = 0.0

        # Max drawdown from equity curve
        max_dd_pct = 0.0
        if self.equity_curve:
            peak = self.equity_curve[0]["total"]
            for point in self.equity_curve:
                total = point["total"]
                if total > peak:
                    peak = total
                dd = (peak - total) / peak if peak > 0 else 0
                if dd > max_dd_pct:
                    max_dd_pct = dd

        # Average hold time
        hold_hours = []
        for t in all_closed:
            if t.entry_time and t.exit_time:
                try:
                    entry_dt = datetime.fromisoformat(t.entry_time)
                    exit_dt = datetime.fromisoformat(t.exit_time)
                    hold_hours.append((exit_dt - entry_dt).total_seconds() / 3600)
                except (ValueError, TypeError):
                    pass
        avg_hold = sum(hold_hours) / len(hold_hours) if hold_hours else 0.0

        # Open positions value
        open_value = sum(t.amount_usd for t in self.positions)
        final_capital = self.capital + open_value

        # Prepare trade list for storage
        trade_dicts = []
        for t in all_closed:
            trade_dicts.append({
                "market_id": t.market_id,
                "question": t.market_question[:80] if t.market_question else "",
                "side": t.side,
                "entry_price": round(t.entry_price, 4),
                "exit_price": round(t.exit_price, 4) if t.exit_price else None,
                "entry_time": t.entry_time,
                "exit_time": t.exit_time,
                "amount": round(t.amount_usd, 2),
                "pnl": round(t.pnl, 4) if t.pnl is not None else None,
                "pnl_pct": round(t.pnl_pct * 100, 2) if t.pnl_pct is not None else None,
                "result": t.result,
                "exit_reason": t.exit_reason,
            })

        # Thin out equity curve for storage (max 200 points)
        eq_curve = self.equity_curve
        if len(eq_curve) > 200:
            step = len(eq_curve) // 200
            eq_curve = eq_curve[::step] + [eq_curve[-1]]

        return BacktestMetrics(
            strategy_id=self.strategy_id,
            strategy_name=self._strategy["name"],
            start_date=self.start_date or "",
            end_date=self.end_date or "",
            initial_capital=self.initial_capital,
            final_capital=final_capital,
            total_pnl=total_pnl,
            total_pnl_pct=(total_pnl / self.initial_capital * 100) if self.initial_capital > 0 else 0,
            win_rate=win_rate,
            total_trades=total_trades,
            wins=wins,
            losses=losses,
            open_positions=len(self.positions),
            max_drawdown_pct=max_dd_pct,
            sharpe_ratio=sharpe_ratio,
            profit_factor=profit_factor,
            avg_win=avg_win,
            avg_loss=avg_loss,
            avg_hold_hours=avg_hold,
            equity_curve=eq_curve,
            trades=trade_dicts,
        )

    def _empty_result(self) -> BacktestMetrics:
        return BacktestMetrics(
            strategy_id=self.strategy_id,
            strategy_name=self._strategy["name"] if self._strategy else "unknown",
            start_date=self.start_date or "",
            end_date=self.end_date or "",
            initial_capital=self.initial_capital,
            final_capital=self.initial_capital,
            total_pnl=0, total_pnl_pct=0, win_rate=0,
            total_trades=0, wins=0, losses=0, open_positions=0,
            max_drawdown_pct=0, sharpe_ratio=0, profit_factor=0,
            avg_win=0, avg_loss=0, avg_hold_hours=0,
        )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def run_snapshot_backtest(
    strategy_id: str,
    days: int = 7,
    initial_capital: float = 1000.0,
) -> dict:
    """Run a snapshot-based backtest for a strategy.

    Args:
        strategy_id: Strategy ID from strategies table
        days: Number of days to look back
        initial_capital: Starting capital in USD

    Returns:
        Result dict with metrics and trade list
    """
    end_date = datetime.utcnow().isoformat()
    start_date = (datetime.utcnow() - timedelta(days=days)).isoformat()

    bt = SnapshotBacktester(
        strategy_id=strategy_id,
        start_date=start_date,
        end_date=end_date,
        initial_capital=initial_capital,
    )

    try:
        metrics = bt.run()
        result = metrics.to_dict()

        # Store in backtest_results table
        _store_result(metrics)

        # Update strategy backtest fields
        _update_strategy(metrics)

        return {"ok": True, **result}

    except Exception as e:
        logger.error(f"Backtest failed for {strategy_id}: {e}", exc_info=True)
        return {"ok": False, "error": str(e)}


def run_all_backtests(days: int = 7) -> list:
    """Run backtests for all active strategies. Called by scheduler."""
    strategies = engine.query(
        "SELECT id, name FROM strategies WHERE status = 'active'"
    )
    results = []
    for strat in strategies:
        logger.info(f"Running snapshot backtest for {strat['name']} ({strat['id']})")
        result = run_snapshot_backtest(strat["id"], days=days)
        results.append({
            "strategy_id": strat["id"],
            "strategy_name": strat["name"],
            "ok": result.get("ok", False),
            "total_pnl": result.get("total_pnl", 0),
            "win_rate": result.get("win_rate", 0),
            "total_trades": result.get("total_trades", 0),
        })
    return results


def get_backtest_history(strategy_id: str, limit: int = 10) -> list:
    """Get recent backtest results for a strategy."""
    try:
        rows = engine.query(
            """SELECT id, strategy_id, strategy_name, start_date, end_date,
                      total_pnl, total_pnl_pct, win_rate, total_trades,
                      wins, losses, max_drawdown_pct, sharpe_ratio,
                      profit_factor, avg_hold_hours, created_at
               FROM backtest_results
               WHERE strategy_id = ?
               ORDER BY created_at DESC LIMIT ?""",
            (strategy_id, limit),
        )
        return rows or []
    except Exception:
        return []


def get_latest_all_results() -> list:
    """Get the latest backtest result for each active strategy."""
    try:
        rows = engine.query("""
            SELECT br.* FROM backtest_results br
            INNER JOIN (
                SELECT strategy_id, MAX(created_at) as max_created
                FROM backtest_results
                GROUP BY strategy_id
            ) latest ON br.strategy_id = latest.strategy_id
                     AND br.created_at = latest.max_created
            ORDER BY br.total_pnl DESC
        """)
        return rows or []
    except Exception:
        return []


def _store_result(metrics: BacktestMetrics):
    """Store backtest result in backtest_results table."""
    try:
        # Ensure table exists
        engine.execute("""
            CREATE TABLE IF NOT EXISTS backtest_results (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                strategy_id     TEXT NOT NULL,
                strategy_name   TEXT,
                start_date      TEXT,
                end_date        TEXT,
                initial_capital REAL,
                final_capital   REAL,
                total_pnl       REAL,
                total_pnl_pct   REAL,
                win_rate        REAL,
                total_trades    INTEGER,
                wins            INTEGER,
                losses          INTEGER,
                open_positions  INTEGER,
                max_drawdown_pct REAL,
                sharpe_ratio    REAL,
                profit_factor   REAL,
                avg_win         REAL,
                avg_loss        REAL,
                avg_hold_hours  REAL,
                equity_curve    TEXT,
                trades_json     TEXT,
                created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        engine.execute(
            "CREATE INDEX IF NOT EXISTS idx_bt_results_strategy ON backtest_results(strategy_id)"
        )
        engine.execute(
            "CREATE INDEX IF NOT EXISTS idx_bt_results_created ON backtest_results(created_at DESC)"
        )

        engine.execute(
            """INSERT INTO backtest_results
               (strategy_id, strategy_name, start_date, end_date,
                initial_capital, final_capital, total_pnl, total_pnl_pct,
                win_rate, total_trades, wins, losses, open_positions,
                max_drawdown_pct, sharpe_ratio, profit_factor,
                avg_win, avg_loss, avg_hold_hours,
                equity_curve, trades_json)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                metrics.strategy_id, metrics.strategy_name,
                metrics.start_date, metrics.end_date,
                metrics.initial_capital, metrics.final_capital,
                metrics.total_pnl, metrics.total_pnl_pct,
                metrics.win_rate, metrics.total_trades,
                metrics.wins, metrics.losses, metrics.open_positions,
                metrics.max_drawdown_pct, metrics.sharpe_ratio,
                metrics.profit_factor, metrics.avg_win, metrics.avg_loss,
                metrics.avg_hold_hours,
                json.dumps(metrics.equity_curve),
                json.dumps(metrics.trades),
            ),
        )
        logger.info(f"Stored backtest result for {metrics.strategy_name}")
    except Exception as e:
        logger.error(f"Failed to store backtest result: {e}")


def _update_strategy(metrics: BacktestMetrics):
    """Update strategy table with latest backtest metrics."""
    try:
        engine.execute(
            """UPDATE strategies SET
                backtest_pnl = ?, backtest_win_rate = ?,
                backtest_sharpe = ?, backtest_max_dd = ?,
                backtest_trades = ?, updated_at = ?
               WHERE id = ?""",
            (
                metrics.total_pnl, metrics.win_rate,
                metrics.sharpe_ratio, metrics.max_drawdown_pct,
                metrics.total_trades, datetime.utcnow().isoformat(),
                metrics.strategy_id,
            ),
        )
    except Exception as e:
        logger.error(f"Failed to update strategy backtest fields: {e}")
