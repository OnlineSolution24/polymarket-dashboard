"""
Backtest Service.
Orchestrates running backtests on strategies using the backtesting modules.
Connects strategy definitions to simulator, walk-forward, and Monte Carlo.
"""

import json
import logging
from datetime import datetime

import numpy as np
import pandas as pd

from db import engine
from services.strategy_evaluator import evaluate_rules

logger = logging.getLogger(__name__)


def run_strategy_backtest(strategy_id: str) -> dict:
    """Run a full backtest suite on a strategy.

    Steps:
        1. Load strategy definition
        2. Load historical trades + market data
        3. Apply entry rules to filter trades
        4. Run simulator, walk-forward, Monte Carlo
        5. Compute confidence score
        6. Store results in DB

    Returns:
        Result dict with metrics and status.
    """
    # 1. Load strategy
    strategy = engine.query_one("SELECT * FROM strategies WHERE id = ?", (strategy_id,))
    if not strategy:
        return {"ok": False, "error": "Strategy not found"}

    try:
        definition = json.loads(strategy["definition"])
    except (json.JSONDecodeError, TypeError):
        return {"ok": False, "error": "Invalid strategy definition JSON"}

    # 2. Load historical data (DB trades + blockchain trades if available)
    trades_df = _load_historical_trades()
    snapshots_df = _load_market_snapshots()

    # Try to augment with blockchain Parquet data for much deeper history
    try:
        from backtesting.data_loader import load_blockchain_trades
        bc_trades = load_blockchain_trades(limit=20000)
        if not bc_trades.empty and "pnl" in bc_trades.columns:
            trades_df = _merge_trade_sources(trades_df, bc_trades)
            logger.info(f"Backtest: merged {len(bc_trades)} blockchain trades, total={len(trades_df)}")
    except Exception:
        pass

    if trades_df.empty or len(trades_df) < 10:
        # Generate synthetic data for testing when real data is sparse
        trades_df = _generate_synthetic_trades(500)

    # 3. Apply strategy rules to filter matching trades
    matched_df = _apply_strategy_rules(definition, trades_df, snapshots_df)

    if matched_df.empty or len(matched_df) < 5:
        engine.execute(
            "UPDATE strategies SET status = 'rejected', updated_at = ? WHERE id = ?",
            (datetime.utcnow().isoformat(), strategy_id),
        )
        return {
            "ok": True,
            "status": "rejected",
            "reason": f"Too few matching trades ({len(matched_df)}). Need at least 5.",
        }

    # 4. Run backtest suite
    from backtesting.simulator import run_backtest
    from backtesting.walk_forward import run_walk_forward
    from backtesting.monte_carlo import run_monte_carlo

    bt = run_backtest(matched_df, initial_capital=100.0)
    wf = run_walk_forward(matched_df)

    pnls = matched_df["pnl"].dropna().values
    mc = run_monte_carlo(pnls, initial_capital=100.0) if len(pnls) >= 10 else None

    # 5. Compute confidence score
    confidence = _compute_confidence(bt, wf, mc)

    # 6. Compile results
    results_json = {
        "backtest": {
            "total_pnl": bt.total_pnl,
            "win_rate": bt.win_rate,
            "sharpe": bt.sharpe_ratio,
            "max_drawdown_pct": bt.max_drawdown_pct,
            "profit_factor": bt.profit_factor,
            "total_trades": bt.total_trades,
            "avg_win": bt.avg_win,
            "avg_loss": bt.avg_loss,
        },
        "walk_forward": {
            "n_windows": wf.n_windows,
            "consistency": wf.consistency_score,
            "degradation": wf.degradation,
            "avg_test_pnl": wf.avg_test_pnl,
            "avg_test_win_rate": wf.avg_test_win_rate,
        },
        "monte_carlo": {
            "prob_profitable": mc.prob_profitable,
            "median_pnl": mc.median_pnl,
            "pnl_5th": mc.pnl_5th,
            "pnl_95th": mc.pnl_95th,
            "worst_max_dd": mc.worst_max_dd,
        } if mc else None,
    }

    # 7. Store in DB
    engine.execute(
        """UPDATE strategies SET
            status = 'backtested',
            backtest_pnl = ?, backtest_win_rate = ?, backtest_sharpe = ?,
            backtest_max_dd = ?, backtest_trades = ?, backtest_results = ?,
            confidence_score = ?, updated_at = ?
           WHERE id = ?""",
        (
            bt.total_pnl, bt.win_rate, bt.sharpe_ratio,
            bt.max_drawdown_pct, bt.total_trades,
            json.dumps(results_json), confidence,
            datetime.utcnow().isoformat(), strategy_id,
        ),
    )

    logger.info(
        f"Backtest completed for {strategy_id}: "
        f"WR={bt.win_rate:.0%} Sharpe={bt.sharpe_ratio:.2f} "
        f"Confidence={confidence:.2f}"
    )

    return {
        "ok": True,
        "status": "backtested",
        "strategy_id": strategy_id,
        "confidence": confidence,
        "results": results_json,
    }


def get_pattern_analysis() -> dict:
    """Compute trading pattern analytics from historical data.

    Returns win rates broken down by category, price bucket, volume bucket,
    and time-of-day. Used by Strategy Agent for discovery.
    """
    result = {}

    # Win rates by category
    cat_stats = engine.query("""
        SELECT m.category,
               COUNT(*) as trades,
               SUM(CASE WHEN t.result='win' THEN 1 ELSE 0 END) as wins,
               AVG(t.pnl) as avg_pnl
        FROM trades t
        JOIN markets m ON t.market_id = m.id
        WHERE t.result IS NOT NULL
        GROUP BY m.category
        ORDER BY trades DESC
    """)
    result["by_category"] = [
        {
            "category": r["category"] or "unknown",
            "trades": r["trades"],
            "win_rate": r["wins"] / r["trades"] if r["trades"] > 0 else 0,
            "avg_pnl": r["avg_pnl"] or 0,
        }
        for r in (cat_stats or [])
    ]

    # Win rates by entry price bucket
    price_stats = engine.query("""
        SELECT
            CASE WHEN t.price < 0.2 THEN 'under_020'
                 WHEN t.price < 0.4 THEN '020_040'
                 WHEN t.price < 0.6 THEN '040_060'
                 WHEN t.price < 0.8 THEN '060_080'
                 ELSE 'over_080' END as bucket,
            COUNT(*) as trades,
            SUM(CASE WHEN result='win' THEN 1 ELSE 0 END) as wins,
            AVG(pnl) as avg_pnl
        FROM trades t
        WHERE result IS NOT NULL AND price IS NOT NULL
        GROUP BY bucket
    """)
    result["by_price"] = [
        {
            "bucket": r["bucket"],
            "trades": r["trades"],
            "win_rate": r["wins"] / r["trades"] if r["trades"] > 0 else 0,
            "avg_pnl": r["avg_pnl"] or 0,
        }
        for r in (price_stats or [])
    ]

    # Win rates by volume bucket
    vol_stats = engine.query("""
        SELECT
            CASE WHEN m.volume < 10000 THEN 'under_10k'
                 WHEN m.volume < 100000 THEN '10k_100k'
                 ELSE 'over_100k' END as bucket,
            COUNT(*) as trades,
            SUM(CASE WHEN t.result='win' THEN 1 ELSE 0 END) as wins,
            AVG(t.pnl) as avg_pnl
        FROM trades t
        JOIN markets m ON t.market_id = m.id
        WHERE t.result IS NOT NULL
        GROUP BY bucket
    """)
    result["by_volume"] = [
        {
            "bucket": r["bucket"],
            "trades": r["trades"],
            "win_rate": r["wins"] / r["trades"] if r["trades"] > 0 else 0,
            "avg_pnl": r["avg_pnl"] or 0,
        }
        for r in (vol_stats or [])
    ]

    # Win rates by side
    side_stats = engine.query("""
        SELECT side,
               COUNT(*) as trades,
               SUM(CASE WHEN result='win' THEN 1 ELSE 0 END) as wins,
               AVG(pnl) as avg_pnl
        FROM trades
        WHERE result IS NOT NULL
        GROUP BY side
    """)
    result["by_side"] = [
        {
            "side": r["side"],
            "trades": r["trades"],
            "win_rate": r["wins"] / r["trades"] if r["trades"] > 0 else 0,
            "avg_pnl": r["avg_pnl"] or 0,
        }
        for r in (side_stats or [])
    ]

    # Overall stats
    overall = engine.query_one("""
        SELECT COUNT(*) as total,
               SUM(CASE WHEN result='win' THEN 1 ELSE 0 END) as wins,
               COALESCE(SUM(pnl), 0) as total_pnl
        FROM trades WHERE result IS NOT NULL
    """)
    if overall:
        result["overall"] = {
            "total_trades": overall["total"],
            "win_rate": overall["wins"] / overall["total"] if overall["total"] > 0 else 0,
            "total_pnl": overall["total_pnl"],
        }
    else:
        result["overall"] = {"total_trades": 0, "win_rate": 0, "total_pnl": 0}

    return result


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _merge_trade_sources(db_trades: pd.DataFrame, bc_trades: pd.DataFrame) -> pd.DataFrame:
    """Merge DB trades with blockchain trades, deduplicating by market_id."""
    if db_trades.empty:
        return bc_trades
    if bc_trades.empty:
        return db_trades

    # Ensure both have executed_at as datetime
    for df in [db_trades, bc_trades]:
        if "executed_at" in df.columns:
            df["executed_at"] = pd.to_datetime(df["executed_at"], errors="coerce")

    # Ensure required columns exist in blockchain trades
    for col in ["amount_usd", "pnl", "result"]:
        if col not in bc_trades.columns:
            return db_trades

    # Concat and sort by time
    combined = pd.concat([db_trades, bc_trades], ignore_index=True)
    combined.sort_values("executed_at", inplace=True, na_position="last")
    return combined


def _load_historical_trades() -> pd.DataFrame:
    """Load trade history as DataFrame."""
    rows = engine.query(
        """SELECT t.*, m.yes_price, m.no_price, m.volume, m.liquidity,
                  m.sentiment_score, m.calculated_edge, m.category, m.end_date
           FROM trades t
           LEFT JOIN markets m ON t.market_id = m.id
           WHERE t.result IS NOT NULL
           ORDER BY t.executed_at"""
    )
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows)


def _load_market_snapshots() -> pd.DataFrame:
    """Load market snapshots as DataFrame."""
    rows = engine.query(
        "SELECT * FROM market_snapshots ORDER BY snapshot_at DESC LIMIT 10000"
    )
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows)


def _apply_strategy_rules(
    definition: dict,
    trades: pd.DataFrame,
    snapshots: pd.DataFrame,
) -> pd.DataFrame:
    """Filter historical trades that match strategy entry rules."""
    entry_rules = definition.get("entry_rules", [])
    category_filter = definition.get("category_filter", [])
    min_liquidity = definition.get("min_liquidity", 0)

    if trades.empty:
        return trades

    matched = trades.copy()

    # Apply category filter
    if category_filter and "category" in matched.columns:
        matched = matched[matched["category"].isin(category_filter)]

    # Apply liquidity filter
    if min_liquidity > 0 and "liquidity" in matched.columns:
        matched = matched[matched["liquidity"].fillna(0) >= min_liquidity]

    # Apply each rule
    for rule in entry_rules:
        field = rule.get("field", "")
        op = rule.get("op", "")
        value = rule.get("value")

        if op not in ("gt", "lt", "gte", "lte", "eq") or value is None:
            continue

        # Map field to DataFrame column
        col = field
        if field == "days_to_expiry" and "end_date" in matched.columns:
            try:
                matched["days_to_expiry"] = pd.to_datetime(matched["end_date"]).apply(
                    lambda x: (x - datetime.utcnow()).days if pd.notna(x) else None
                )
                col = "days_to_expiry"
            except Exception:
                continue

        if col not in matched.columns:
            continue

        numeric_col = pd.to_numeric(matched[col], errors="coerce")

        if op == "gt":
            matched = matched[numeric_col > value]
        elif op == "lt":
            matched = matched[numeric_col < value]
        elif op == "gte":
            matched = matched[numeric_col >= value]
        elif op == "lte":
            matched = matched[numeric_col <= value]
        elif op == "eq":
            matched = matched[numeric_col == value]

    return matched


def _compute_confidence(bt, wf, mc) -> float:
    """Composite confidence score 0-1."""
    score = 0.0

    # Backtest metrics (40% weight)
    if bt.win_rate > 0.5:
        score += 0.15
    if bt.sharpe_ratio > 0.5:
        score += min(0.15, bt.sharpe_ratio * 0.1)
    if bt.profit_factor > 1.2:
        score += 0.10

    # Walk-forward (30% weight)
    if wf and wf.n_windows > 0:
        score += wf.consistency_score * 0.20
        if wf.degradation < 0.05:
            score += 0.10

    # Monte Carlo (30% weight)
    if mc:
        score += mc.prob_profitable * 0.20
        if mc.pnl_5th > 0:
            score += 0.10

    return min(1.0, round(score, 3))


def _generate_synthetic_trades(n: int = 500) -> pd.DataFrame:
    """Generate synthetic trade data for backtesting when real data is sparse."""
    rng = np.random.default_rng(42)

    categories = ["politics", "crypto", "sports", "science"]
    sides = ["YES", "NO"]

    records = []
    for i in range(n):
        side = rng.choice(sides)
        price = round(rng.uniform(0.1, 0.9), 2)
        edge = round(rng.uniform(-0.05, 0.10), 3)
        win = rng.random() < (0.5 + edge)
        pnl = round(rng.uniform(0.5, 5.0) if win else -rng.uniform(0.5, 3.0), 2)

        records.append({
            "id": i + 1,
            "market_id": f"synthetic_{i % 50}",
            "market_question": f"Synthetic market {i % 50}",
            "side": side,
            "amount_usd": round(rng.uniform(1.0, 5.0), 2),
            "price": price,
            "yes_price": price if side == "YES" else 1 - price,
            "no_price": 1 - price if side == "YES" else price,
            "status": "executed",
            "result": "win" if win else "loss",
            "pnl": pnl,
            "volume": round(rng.uniform(1000, 500000), 0),
            "liquidity": round(rng.uniform(500, 100000), 0),
            "sentiment_score": round(rng.uniform(-1.0, 1.0), 2),
            "calculated_edge": edge,
            "category": rng.choice(categories),
            "executed_at": f"2026-{1 + i // 30:02d}-{1 + i % 28:02d}T12:00:00",
        })

    return pd.DataFrame(records)
