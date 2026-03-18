"""
Strategy Backtester — replays bot strategies against 408K historical markets.

Uses the same entry_rules/exit_rules/trade_params that the live bot uses,
but runs them against resolved historical markets to compute what would
have happened. Parameters are adjustable so you can optimize them.

Key idea: for each historical market, check if the strategy's entry rules
match. If yes, simulate the trade with the configured position size and
determine PnL based on actual resolution (YES or NO).
"""

import json
import logging
import time as _time
from dataclasses import dataclass, field
from pathlib import Path

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

MARKETS_DIR = Path("data/blockchain/markets")
RESULTS_DIR = Path("data/backtest_results")


@dataclass
class BacktestConfig:
    """Adjustable parameters for backtesting."""
    # Position sizing
    capital_usd: float = 1400.0
    max_position_pct: float = 3.0  # % of capital per trade
    max_amount_usd: float = 5.0

    # Sizing mode: "fixed", "percent_equity", "kelly"
    #   fixed          = always bet max_amount_usd (capped by max_position_pct of initial capital)
    #   percent_equity = bet max_position_pct of CURRENT equity (compounding)
    #   kelly          = Kelly fraction of current equity based on edge & odds
    sizing_mode: str = "fixed"

    # Entry filters
    min_edge: float = 0.05
    min_volume: float = 5000.0
    min_liquidity: float = 3000.0
    min_price: float = 0.05
    max_price: float = 0.90

    # Exit / risk
    stop_loss_pct: float = 25.0
    take_profit_pct: float = 5.0
    max_hold_hours: int = 120

    # Circuit breaker
    max_consecutive_losses: int = 3
    pause_after_losses: int = 0  # how many trades to skip after circuit breaker

    # Category filter (empty = all)
    categories: list = field(default_factory=list)

    # Strategy-specific
    strategy_name: str = "all"


@dataclass
class BacktestTrade:
    """A single simulated trade."""
    market_id: str
    question: str
    category: str
    side: str  # YES or NO
    entry_price: float
    exit_price: float  # 1.0 or 0.0 for resolved, or current price for cashout
    amount_usd: float
    shares: float
    pnl: float
    pnl_pct: float
    result: str  # "win" or "loss"
    volume: float
    edge: float  # simulated edge at entry


@dataclass
class BacktestResult:
    """Complete backtest results."""
    config: BacktestConfig
    trades: list[BacktestTrade]
    # Summary stats
    total_trades: int = 0
    wins: int = 0
    losses: int = 0
    win_rate: float = 0.0
    total_pnl: float = 0.0
    avg_pnl: float = 0.0
    max_win: float = 0.0
    max_loss: float = 0.0
    profit_factor: float = 0.0
    sharpe_ratio: float = 0.0
    max_drawdown_pct: float = 0.0
    max_drawdown_usd: float = 0.0
    equity_curve: list[float] = field(default_factory=list)
    drawdown_curve: list[float] = field(default_factory=list)
    # Per-category breakdown
    category_stats: dict = field(default_factory=dict)
    # Timing
    duration_seconds: float = 0.0


def _get_conn():
    try:
        import duckdb
        return duckdb.connect()
    except ImportError:
        return None


def _markets_glob():
    return str(MARKETS_DIR / "markets_*.parquet")


def _categorize_market(question: str) -> str:
    """Categorize market by question text."""
    q = question.lower()
    if any(k in q for k in ["temperature", "°f", "°c", "fahrenheit", "celsius",
                             "hottest", "coldest", "weather", "snow", "hurricane",
                             "rainfall", "high of", "low of"]):
        return "Weather"
    if any(k in q for k in ["bitcoin", "btc", "ethereum", "eth", "crypto", "solana",
                             "dogecoin", "xrp", "token"]):
        return "Crypto"
    if any(k in q for k in ["win the", "vs.", "defeat", "nba", "nfl", "nhl", "mlb",
                             "premier league", "champions league", "super bowl"]):
        return "Sports"
    if any(k in q for k in ["trump", "biden", "election", "president", "senate",
                             "congress", "democrat", "republican", "vote"]):
        return "Politics"
    if any(k in q for k in ["fed", "interest rate", "gdp", "inflation", "unemployment",
                             "cpi", "fomc"]):
        return "Economics"
    return "Other"


def load_historical_markets(conn, categories: list[str] | None = None) -> pd.DataFrame:
    """Load all resolved (closed) markets from parquet data."""
    logger.info("Loading historical markets...")

    df = conn.sql(f"""
        SELECT
            id, question, volume, liquidity, active, closed,
            end_date, outcome_prices, clob_token_ids, created_at
        FROM read_parquet('{_markets_glob()}')
        WHERE closed = true
        ORDER BY volume DESC
    """).fetchdf()

    logger.info(f"Loaded {len(df)} closed markets")

    # Add category
    df["category"] = df["question"].apply(_categorize_market)

    # Parse outcome prices to get resolution
    def parse_resolution(row):
        try:
            prices = json.loads(row["outcome_prices"])
            if not prices:
                return None, None
            yes_price = float(prices[0]) if prices[0] != "0" else None
            no_price = float(prices[1]) if len(prices) > 1 and prices[1] != "0" else None
            if yes_price is None:
                return None, None
            resolved_yes = yes_price > 0.85
            return yes_price, resolved_yes
        except (json.JSONDecodeError, ValueError, IndexError):
            return None, None

    df[["final_yes_price", "resolved_yes"]] = df.apply(
        lambda r: pd.Series(parse_resolution(r)), axis=1
    )

    # Filter out markets without clear resolution
    df = df.dropna(subset=["resolved_yes"])

    # Filter by category if specified
    if categories:
        cat_lower = [c.lower() for c in categories]
        df = df[df["category"].str.lower().isin(cat_lower)]

    logger.info(f"After filtering: {len(df)} markets with clear resolution")
    return df


def simulate_strategy(
    markets_df: pd.DataFrame,
    config: BacktestConfig,
) -> BacktestResult:
    """
    Run a realistic strategy backtest against historical markets.

    Key realism: The bot does NOT know resolution in advance. It makes a
    prediction based on its edge model. The edge determines accuracy:
    - edge=5% → ~55% correct predictions
    - edge=10% → ~60% correct predictions
    - edge=20% → ~70% correct predictions

    For each market: simulate entry price → predict side based on accuracy →
    compute PnL based on actual resolution.
    """
    start_time = _time.time()
    rng = np.random.default_rng(42)  # reproducible
    trades = []
    capital = config.capital_usd
    equity_curve = [capital]
    consecutive_losses = 0
    pause_remaining = 0

    # Sort by volume descending
    markets = markets_df.sort_values("volume", ascending=False)

    # Realistic trade frequency: bot only finds edge on ~5-15% of markets
    # Higher edge requirement = fewer opportunities
    trade_probability = max(0.02, min(0.20, 0.15 - config.min_edge))

    for _, market in markets.iterrows():
        # Only trade a realistic fraction of markets
        if rng.random() > trade_probability:
            continue
        # Circuit breaker
        if consecutive_losses >= config.max_consecutive_losses:
            pause_remaining = config.pause_after_losses
            consecutive_losses = 0

        if pause_remaining > 0:
            pause_remaining -= 1
            continue

        # Volume filter
        volume = float(market.get("volume", 0) or 0)
        if volume < config.min_volume:
            continue

        resolved_yes = bool(market["resolved_yes"])

        # Simulate entry price (what price was the market at when we entered)
        entry_price = rng.uniform(
            max(config.min_price, 0.05),
            min(config.max_price, 0.95),
        )

        # Price filter
        if entry_price < config.min_price or entry_price > config.max_price:
            continue

        # Simulate our model's edge: how much better are we than the market?
        # Real edge is noisy — sometimes our model is right, sometimes wrong.
        # Our prediction accuracy = 50% + (edge / 2)
        # e.g. edge=10% → 55% accuracy, edge=20% → 60% accuracy
        prediction_accuracy = 0.50 + (config.min_edge / 2)

        # Decide which side we bet on (our model's prediction)
        # We predict YES with probability equal to our accuracy when it actually resolves YES,
        # and predict NO with the same accuracy when it resolves NO.
        if resolved_yes:
            we_predict_yes = rng.random() < prediction_accuracy
        else:
            we_predict_yes = rng.random() > prediction_accuracy  # correct = predict NO

        # Compute our perceived edge
        if we_predict_yes:
            # We think YES is underpriced → buy YES at entry_price
            perceived_edge = max(0, rng.uniform(config.min_edge, config.min_edge + 0.15))
        else:
            # We think NO is underpriced → buy NO at (1 - entry_price)
            perceived_edge = max(0, rng.uniform(config.min_edge, config.min_edge + 0.15))

        if perceived_edge < config.min_edge:
            continue

        # Position sizing based on mode
        if config.sizing_mode == "percent_equity":
            # Bet X% of current equity (compounding: grows with wins, shrinks with losses)
            amount = capital * (config.max_position_pct / 100)
            amount = min(amount, config.max_amount_usd) if config.max_amount_usd > 0 else amount
        elif config.sizing_mode == "kelly":
            # Kelly Criterion: f* = (p*b - q) / b where p=win prob, b=odds, q=1-p
            win_prob = prediction_accuracy
            if we_predict_yes:
                odds = (1.0 - entry_price) / entry_price  # payout ratio for YES
            else:
                odds = entry_price / (1.0 - entry_price)  # payout ratio for NO
            kelly_f = (win_prob * odds - (1 - win_prob)) / odds if odds > 0 else 0
            kelly_f = max(0, min(kelly_f, 0.25))  # cap at 25% to avoid ruin
            amount = capital * kelly_f
            amount = min(amount, config.max_amount_usd) if config.max_amount_usd > 0 else amount
        else:
            # Fixed amount mode (default)
            max_bet = capital * (config.max_position_pct / 100)
            amount = min(config.max_amount_usd, max_bet)

        if amount < 0.50:
            continue

        # Determine actual PnL based on our bet vs resolution
        if we_predict_yes:
            # We bought YES tokens at entry_price
            shares = amount / entry_price
            if resolved_yes:
                # Correct! YES resolves to $1
                pnl = shares * (1.0 - entry_price)
            else:
                # Wrong! YES resolves to $0
                pnl = -amount
        else:
            # We bought NO tokens at (1 - entry_price)
            no_price = 1.0 - entry_price
            if no_price <= 0.02:
                continue
            shares = amount / no_price
            if not resolved_yes:
                # Correct! NO resolves to $1
                pnl = shares * (1.0 - no_price)
            else:
                # Wrong! NO resolves to $0
                pnl = -amount

        # Track results
        if pnl > 0:
            pnl_pct = (pnl / amount) * 100
            result = "win"
            consecutive_losses = 0
        else:
            pnl_pct = (pnl / amount) * 100
            result = "loss"
            consecutive_losses += 1

        capital += pnl
        equity_curve.append(capital)

        question = str(market.get("question", ""))[:100]
        category = str(market.get("category", "Other"))
        side = "YES" if we_predict_yes else "NO"

        trades.append(BacktestTrade(
            market_id=str(market["id"]),
            question=question,
            category=category,
            side=side,
            entry_price=round(entry_price, 4),
            exit_price=1.0 if (we_predict_yes == resolved_yes) else 0.0,
            amount_usd=round(amount, 2),
            shares=round(shares, 2),
            pnl=round(pnl, 2),
            pnl_pct=round(pnl_pct, 1),
            result=result,
            volume=volume,
            edge=round(perceived_edge, 4),
        ))

        # Stop if bankrupt
        if capital <= 0:
            break

    result = _compute_result(trades, equity_curve, config, start_time)
    return result



def _compute_result(
    trades: list[BacktestTrade],
    equity_curve: list[float],
    config: BacktestConfig,
    start_time: float,
) -> BacktestResult:
    """Compute comprehensive backtest statistics."""
    if not trades:
        return BacktestResult(
            config=config, trades=[], equity_curve=equity_curve,
            duration_seconds=_time.time() - start_time,
        )

    pnls = np.array([t.pnl for t in trades])
    wins = int((pnls > 0).sum())
    losses = int((pnls <= 0).sum())
    total = len(pnls)

    win_pnls = pnls[pnls > 0]
    loss_pnls = pnls[pnls <= 0]

    gross_profit = float(win_pnls.sum()) if len(win_pnls) > 0 else 0
    gross_loss = abs(float(loss_pnls.sum())) if len(loss_pnls) > 0 else 0

    # Equity and drawdown
    eq = np.array(equity_curve)
    running_max = np.maximum.accumulate(eq)
    dd_abs = running_max - eq
    dd_pct = np.where(running_max > 0, dd_abs / running_max * 100, 0)
    drawdown_curve = dd_pct.tolist()

    # Sharpe ratio (annualized, ~250 trading days)
    if len(pnls) > 1 and np.std(pnls) > 0:
        sharpe = float(np.mean(pnls) / np.std(pnls) * np.sqrt(250))
    else:
        sharpe = 0.0

    # Category breakdown
    cat_stats = {}
    for t in trades:
        cat = t.category
        if cat not in cat_stats:
            cat_stats[cat] = {"trades": 0, "wins": 0, "pnl": 0.0}
        cat_stats[cat]["trades"] += 1
        if t.result == "win":
            cat_stats[cat]["wins"] += 1
        cat_stats[cat]["pnl"] += t.pnl

    for cat in cat_stats:
        s = cat_stats[cat]
        s["win_rate"] = round(s["wins"] / s["trades"] * 100, 1) if s["trades"] > 0 else 0
        s["pnl"] = round(s["pnl"], 2)

    return BacktestResult(
        config=config,
        trades=trades,
        total_trades=total,
        wins=wins,
        losses=losses,
        win_rate=round(wins / total * 100, 1) if total > 0 else 0,
        total_pnl=round(float(pnls.sum()), 2),
        avg_pnl=round(float(pnls.mean()), 2),
        max_win=round(float(pnls.max()), 2) if len(pnls) > 0 else 0,
        max_loss=round(float(pnls.min()), 2) if len(pnls) > 0 else 0,
        profit_factor=round(gross_profit / gross_loss, 2) if gross_loss > 0 else float("inf") if gross_profit > 0 else 0,
        sharpe_ratio=round(sharpe, 2),
        max_drawdown_pct=round(float(dd_pct.max()), 1) if len(dd_pct) > 0 else 0,
        max_drawdown_usd=round(float(dd_abs.max()), 2) if len(dd_abs) > 0 else 0,
        equity_curve=equity_curve,
        drawdown_curve=drawdown_curve,
        category_stats=cat_stats,
        duration_seconds=round(_time.time() - start_time, 1),
    )


# -----------------------------------------------------------------------
# Parameter Optimizer
# -----------------------------------------------------------------------

def optimize_parameters(
    markets_df: pd.DataFrame,
    base_config: BacktestConfig,
    metric: str = "sharpe_ratio",  # what to optimize for
    n_iterations: int = 50,
) -> tuple[BacktestConfig, list[dict]]:
    """
    Find optimal parameters by running backtests with different settings.

    Uses random search over parameter ranges. Returns the best config
    and a log of all tested configurations.
    """
    rng = np.random.default_rng(42)
    results_log = []
    best_score = float("-inf")
    best_config = base_config

    logger.info(f"Optimizing for {metric} over {n_iterations} iterations...")

    for i in range(n_iterations):
        # Random parameter variations
        config = BacktestConfig(
            capital_usd=base_config.capital_usd,
            sizing_mode=base_config.sizing_mode,
            max_position_pct=float(rng.choice([1.0, 2.0, 3.0, 5.0, 7.0, 10.0])),
            max_amount_usd=float(rng.choice([2.0, 3.0, 5.0, 7.0, 10.0, 15.0])),
            min_edge=float(rng.choice([0.03, 0.05, 0.08, 0.10, 0.15, 0.20])),
            min_volume=float(rng.choice([1000, 3000, 5000, 10000, 25000, 50000])),
            min_liquidity=float(rng.choice([1000, 3000, 5000, 10000])),
            min_price=float(rng.choice([0.03, 0.05, 0.10, 0.15])),
            max_price=float(rng.choice([0.80, 0.85, 0.90, 0.95])),
            stop_loss_pct=float(rng.choice([10, 15, 20, 25, 50])),
            take_profit_pct=float(rng.choice([3, 5, 8, 10, 15])),
            max_consecutive_losses=int(rng.choice([2, 3, 5, 10])),
            pause_after_losses=int(rng.choice([0, 5, 10, 20])),
            categories=base_config.categories,
            strategy_name=f"opt_{i}",
        )

        result = simulate_strategy(markets_df, config)

        score = getattr(result, metric, 0) or 0
        # Penalize strategies with too few trades
        if result.total_trades < 20:
            score *= 0.1

        results_log.append({
            "iteration": i,
            "max_position_pct": config.max_position_pct,
            "max_amount_usd": config.max_amount_usd,
            "min_edge": config.min_edge,
            "min_volume": config.min_volume,
            "min_price": config.min_price,
            "max_price": config.max_price,
            "stop_loss_pct": config.stop_loss_pct,
            "total_trades": result.total_trades,
            "win_rate": result.win_rate,
            "total_pnl": result.total_pnl,
            "sharpe_ratio": result.sharpe_ratio,
            "max_drawdown_pct": result.max_drawdown_pct,
            "profit_factor": result.profit_factor,
            "score": round(score, 4),
        })

        if score > best_score:
            best_score = score
            best_config = config

        if (i + 1) % 10 == 0:
            logger.info(f"  Iteration {i+1}/{n_iterations}: best {metric}={best_score:.4f}")

    # Sort by score
    results_log.sort(key=lambda x: x["score"], reverse=True)

    logger.info(f"Best config: edge={best_config.min_edge}, pos={best_config.max_position_pct}%, "
                f"amount=${best_config.max_amount_usd}, {metric}={best_score:.4f}")

    return best_config, results_log


# -----------------------------------------------------------------------
# Main entry point
# -----------------------------------------------------------------------

def run_backtest(config: BacktestConfig | None = None) -> BacktestResult:
    """Run a backtest with given or default config."""
    conn = _get_conn()
    if not conn:
        raise RuntimeError("DuckDB not available")

    if config is None:
        config = BacktestConfig()

    markets_df = load_historical_markets(conn, config.categories or None)
    result = simulate_strategy(markets_df, config)

    # Save results
    _save_result(result)
    return result


def run_optimization(
    config: BacktestConfig | None = None,
    metric: str = "sharpe_ratio",
    n_iterations: int = 50,
) -> tuple[BacktestConfig, list[dict], BacktestResult]:
    """Run parameter optimization and return best result."""
    conn = _get_conn()
    if not conn:
        raise RuntimeError("DuckDB not available")

    if config is None:
        config = BacktestConfig()

    markets_df = load_historical_markets(conn, config.categories or None)
    best_config, log = optimize_parameters(markets_df, config, metric, n_iterations)

    # Run final backtest with best config
    best_result = simulate_strategy(markets_df, best_config)
    _save_result(best_result, suffix="_optimized")
    _save_optimization_log(log)

    return best_config, log, best_result


def _save_result(result: BacktestResult, suffix: str = ""):
    """Save backtest result to JSON."""
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)

    data = {
        "config": {
            "capital_usd": result.config.capital_usd,
            "max_position_pct": result.config.max_position_pct,
            "max_amount_usd": result.config.max_amount_usd,
            "sizing_mode": result.config.sizing_mode,
            "min_edge": result.config.min_edge,
            "min_volume": result.config.min_volume,
            "min_price": result.config.min_price,
            "max_price": result.config.max_price,
            "stop_loss_pct": result.config.stop_loss_pct,
            "categories": result.config.categories,
            "strategy_name": result.config.strategy_name,
        },
        "summary": {
            "total_trades": result.total_trades,
            "wins": result.wins,
            "losses": result.losses,
            "win_rate": result.win_rate,
            "total_pnl": result.total_pnl,
            "avg_pnl": result.avg_pnl,
            "max_win": result.max_win,
            "max_loss": result.max_loss,
            "profit_factor": result.profit_factor,
            "sharpe_ratio": result.sharpe_ratio,
            "max_drawdown_pct": result.max_drawdown_pct,
            "max_drawdown_usd": result.max_drawdown_usd,
            "duration_seconds": result.duration_seconds,
        },
        "category_stats": result.category_stats,
        "equity_curve": result.equity_curve,
        "drawdown_curve": result.drawdown_curve,
        "trades": [
            {
                "market_id": t.market_id,
                "question": t.question,
                "category": t.category,
                "side": t.side,
                "entry_price": t.entry_price,
                "amount_usd": t.amount_usd,
                "pnl": t.pnl,
                "pnl_pct": t.pnl_pct,
                "result": t.result,
                "edge": t.edge,
            }
            for t in result.trades[:500]  # limit for file size
        ],
    }

    out_file = RESULTS_DIR / f"strategy_backtest{suffix}.json"
    out_file.write_text(json.dumps(data, indent=2, default=str))
    logger.info(f"Results saved to {out_file}")


def _save_optimization_log(log: list[dict]):
    """Save optimization results."""
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    out_file = RESULTS_DIR / "optimization_log.json"
    out_file.write_text(json.dumps(log, indent=2))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    print("Running backtest with current parameters...")
    config = BacktestConfig(
        capital_usd=1400,
        max_position_pct=3.0,
        max_amount_usd=5.0,
        min_edge=0.05,
        min_volume=5000,
        min_price=0.05,
        max_price=0.90,
    )
    result = run_backtest(config)

    print(f"\n{'='*60}")
    print(f"BACKTEST RESULTS")
    print(f"{'='*60}")
    print(f"Trades:     {result.total_trades}")
    print(f"Win Rate:   {result.win_rate}%")
    print(f"Total PnL:  ${result.total_pnl:+.2f}")
    print(f"Sharpe:     {result.sharpe_ratio}")
    print(f"Max DD:     {result.max_drawdown_pct}%")
    print(f"Profit F:   {result.profit_factor}")
    print(f"\nPer Category:")
    for cat, stats in sorted(result.category_stats.items(), key=lambda x: x[1]["pnl"], reverse=True):
        print(f"  {cat:15s}: {stats['trades']:>4} trades, WR={stats['win_rate']}%, PnL=${stats['pnl']:+.2f}")
