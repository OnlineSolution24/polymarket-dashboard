"""
Kelly Sizing Backtest — Uses REAL 388M trade data to compare sizing strategies.

Tests: Fixed $5 vs Quarter-Kelly vs Half-Kelly on Volume Flow + Contrarian Whale edges.
Uses actual market outcomes from Parquet resolution data, not simulations.

Run via: docker exec polymarket-bot python3 backtesting/kelly_backtest.py
"""

import sys
sys.path.insert(0, "/app")

import random
from dataclasses import dataclass

TRADES_GLOB = "data/blockchain/trades/trades_*.parquet"
TOKEN_MAP = "data/blockchain/token_to_market.parquet"
RESOLUTIONS = "data/blockchain/resolutions.parquet"


@dataclass
class BacktestResult:
    strategy: str
    sizing: str
    start_capital: float
    final_capital: float
    total_trades: int
    wins: int
    losses: int
    win_rate: float
    total_pnl: float
    max_drawdown_pct: float
    sharpe_approx: float
    avg_bet: float


def get_conn():
    import duckdb
    conn = duckdb.connect()
    conn.execute("SET temp_directory = '/tmp/duckdb_temp'")
    conn.execute("SET memory_limit = '4GB'")
    return conn


def load_volume_flow_markets(conn) -> list[dict]:
    """Load per-market volume flow data from real Parquet trades."""
    print("Loading Volume Flow markets from Parquet...")

    results = conn.execute(f"""
        WITH market_ranges AS (
            SELECT
                tm.condition_id,
                MIN(t.block_number) as first_block,
                MAX(t.block_number) as last_block,
                (MAX(t.block_number) - MIN(t.block_number)) / 2 + MIN(t.block_number) as mid_block
            FROM read_parquet('{TRADES_GLOB}') t
            JOIN read_parquet('{TOKEN_MAP}') tm ON t.taker_asset_id = tm.token_id
            WHERE t.maker_asset_id = '0' AND t.maker_amount > 0
            GROUP BY tm.condition_id
            HAVING COUNT(*) >= 20 AND MAX(t.block_number) > MIN(t.block_number)
        ),
        early_flows AS (
            SELECT
                tm.condition_id,
                SUM(CASE WHEN tm.is_winner THEN t.maker_amount ELSE 0 END) as winning_vol,
                SUM(CASE WHEN NOT tm.is_winner THEN t.maker_amount ELSE 0 END) as losing_vol,
                MIN(t.block_number) as first_block
            FROM read_parquet('{TRADES_GLOB}') t
            JOIN read_parquet('{TOKEN_MAP}') tm ON t.taker_asset_id = tm.token_id
            JOIN market_ranges mr ON tm.condition_id = mr.condition_id
            WHERE t.maker_asset_id = '0'
              AND t.maker_amount > 0
              AND t.block_number <= mr.mid_block
            GROUP BY tm.condition_id
            HAVING SUM(t.maker_amount) > 0
        )
        SELECT
            condition_id,
            winning_vol,
            losing_vol,
            first_block,
            GREATEST(winning_vol, losing_vol) * 1.0
                / NULLIF(LEAST(winning_vol, losing_vol), 0) as flow_ratio,
            CASE WHEN winning_vol > losing_vol THEN 1 ELSE 0 END as dominant_won
        FROM early_flows
        WHERE winning_vol > 0 AND losing_vol > 0
        ORDER BY first_block
    """).fetchall()

    markets = []
    for row in results:
        cid, wvol, lvol, fb, ratio, won = row
        if ratio is None or ratio < 1.3:
            continue
        markets.append({
            "condition_id": cid,
            "flow_ratio": float(ratio),
            "dominant_won": bool(won),
            "first_block": fb,
        })

    print(f"  Loaded {len(markets)} markets with flow_ratio >= 1.3")
    return markets


def load_contrarian_whale_markets(conn) -> list[dict]:
    """Load per-market whale flow dominance from real Parquet trades."""
    print("Loading Contrarian Whale markets from Parquet...")

    results = conn.execute(f"""
        WITH market_flows AS (
            SELECT
                tm.condition_id,
                tm.outcome,
                tm.is_winner,
                SUM(CASE WHEN t.maker_amount / 1e6 < 50 THEN t.maker_amount / 1e6 ELSE 0 END) as retail_vol,
                SUM(CASE WHEN t.maker_amount / 1e6 >= 500 THEN t.maker_amount / 1e6 ELSE 0 END) as whale_vol,
                MIN(t.block_number) as first_block
            FROM read_parquet('{TRADES_GLOB}') t
            JOIN read_parquet('{TOKEN_MAP}') tm ON t.taker_asset_id = tm.token_id
            WHERE t.maker_asset_id = '0'
              AND t.maker_amount > 0 AND t.taker_amount > 0
            GROUP BY tm.condition_id, tm.outcome, tm.is_winner
        )
        SELECT
            condition_id,
            outcome,
            is_winner,
            retail_vol,
            whale_vol,
            first_block,
            CASE
                WHEN whale_vol > 0 AND (retail_vol = 0 OR whale_vol > retail_vol * 5) THEN 'whale_only'
                WHEN whale_vol > retail_vol * 2 AND retail_vol > 0 THEN 'whale_dominant'
                ELSE 'other'
            END as flow_type
        FROM market_flows
        WHERE whale_vol > 0
        ORDER BY first_block
    """).fetchall()

    markets = []
    for row in results:
        cid, outcome, is_winner, rvol, wvol, fb, flow_type = row
        if flow_type == "other":
            continue
        markets.append({
            "condition_id": cid,
            "flow_type": flow_type,
            "is_winner": bool(is_winner),
            "first_block": fb,
        })

    print(f"  Loaded {len(markets)} whale-dominated outcomes")
    return markets


def get_flow_tier(ratio: float) -> str:
    if ratio >= 5.0:
        return "extreme"
    elif ratio >= 2.0:
        return "strong"
    elif ratio >= 1.3:
        return "mild"
    return "none"


# Historical hit rates from Discovery (388M trades)
FLOW_HIT_RATES = {
    "extreme": 0.954,
    "strong": 0.773,
    "mild": 0.623,
}

WHALE_HIT_RATES = {
    "whale_only": 0.955,
    "whale_dominant": 0.700,
}


def kelly_fraction(hit_rate: float) -> float:
    """Kelly criterion for binary bets (win = +bet, lose = -bet)."""
    return max(2 * hit_rate - 1, 0)


def simulate_equity(markets: list[dict], sizing: str, start_capital: float,
                     hit_rate_fn, max_position_pct: float = 0.20) -> BacktestResult:
    """Simulate equity curve for a list of markets with a given sizing strategy."""
    capital = start_capital
    peak = start_capital
    max_dd = 0.0
    wins = 0
    losses = 0
    returns = []
    total_bet = 0.0

    for m in markets:
        hit_rate = hit_rate_fn(m)
        if hit_rate is None:
            continue

        won = m.get("dominant_won", m.get("is_winner", False))

        # Calculate bet size
        if sizing == "fixed_5":
            bet = 5.0
        elif sizing == "fixed_10":
            bet = 10.0
        elif sizing == "quarter_kelly":
            kf = kelly_fraction(hit_rate)
            bet = capital * kf / 4
        elif sizing == "half_kelly":
            kf = kelly_fraction(hit_rate)
            bet = capital * kf / 2
        else:
            bet = 5.0

        # Cap at max position size and absolute max
        bet = min(bet, capital * max_position_pct)
        bet = min(bet, 200.0)  # absolute max $200 per trade (realistic)
        bet = min(bet, capital - 1.0)  # keep at least $1
        bet = max(bet, 1.0)  # minimum $1

        if capital < 2.0:
            break  # bankrupt

        # Safety: skip if NaN/inf
        if bet != bet or capital != capital:
            break

        total_bet += bet

        if won:
            capital += bet
            wins += 1
            prev = capital - bet
            returns.append(bet / prev if prev > 0 else 0)
        else:
            capital -= bet
            losses += 1
            prev = capital + bet
            returns.append(-bet / prev if prev > 0 else 0)

        # Track drawdown
        if capital > peak:
            peak = capital
        dd = (peak - capital) / peak if peak > 0 else 0
        if dd > max_dd:
            max_dd = dd

    total_trades = wins + losses
    avg_bet = total_bet / total_trades if total_trades > 0 else 0

    # Approximate Sharpe (mean return / std return)
    if returns and len(returns) > 1:
        mean_r = sum(returns) / len(returns)
        var_r = sum((r - mean_r) ** 2 for r in returns) / len(returns)
        std_r = var_r ** 0.5
        sharpe = (mean_r / std_r) if std_r > 0 else 0
    else:
        sharpe = 0

    return BacktestResult(
        strategy="",
        sizing=sizing,
        start_capital=start_capital,
        final_capital=round(capital, 2),
        total_trades=total_trades,
        wins=wins,
        losses=losses,
        win_rate=wins / total_trades if total_trades > 0 else 0,
        total_pnl=round(capital - start_capital, 2),
        max_drawdown_pct=round(max_dd * 100, 1),
        sharpe_approx=round(sharpe, 3),
        avg_bet=round(avg_bet, 2),
    )


def walk_forward(markets: list[dict], sizing: str, start_capital: float,
                 hit_rate_fn, n_windows: int = 5) -> list[BacktestResult]:
    """Walk-forward test: split markets into n sequential windows."""
    window_size = len(markets) // n_windows
    results = []
    for i in range(n_windows):
        start = i * window_size
        end = start + window_size if i < n_windows - 1 else len(markets)
        window = markets[start:end]
        r = simulate_equity(window, sizing, start_capital, hit_rate_fn)
        r.strategy = f"Window {i+1}"
        results.append(r)
    return results


def monte_carlo(markets: list[dict], sizing: str, start_capital: float,
                hit_rate_fn, n_simulations: int = 1000) -> dict:
    """Monte Carlo: shuffle trade order 1000x, measure outcome distribution."""
    final_capitals = []
    max_drawdowns = []

    for _ in range(n_simulations):
        shuffled = markets.copy()
        random.shuffle(shuffled)
        r = simulate_equity(shuffled, sizing, start_capital, hit_rate_fn)
        final_capitals.append(r.final_capital)
        max_drawdowns.append(r.max_drawdown_pct)

    final_capitals.sort()
    max_drawdowns.sort()

    return {
        "median_capital": final_capitals[n_simulations // 2],
        "p5_capital": final_capitals[int(n_simulations * 0.05)],
        "p95_capital": final_capitals[int(n_simulations * 0.95)],
        "median_dd": max_drawdowns[n_simulations // 2],
        "p95_dd": max_drawdowns[int(n_simulations * 0.95)],
        "prob_profitable": sum(1 for c in final_capitals if c > start_capital) / n_simulations,
    }


def print_result(r: BacktestResult):
    print(f"  {r.sizing:20s} | Trades: {r.total_trades:>7,} | "
          f"Win: {r.win_rate:5.1%} | PnL: ${r.total_pnl:>12,.2f} | "
          f"Final: ${r.final_capital:>12,.2f} | "
          f"MaxDD: {r.max_drawdown_pct:5.1f}% | "
          f"Sharpe: {r.sharpe_approx:6.3f} | "
          f"AvgBet: ${r.avg_bet:>7.2f}")


def main():
    conn = get_conn()
    START_CAPITAL = 1000.0
    SIZINGS = ["fixed_5", "fixed_10", "quarter_kelly", "half_kelly"]

    # ─── VOLUME FLOW BACKTEST ─────────────────────────────────────────────
    vf_markets = load_volume_flow_markets(conn)

    def vf_hit_rate(m):
        tier = get_flow_tier(m["flow_ratio"])
        return FLOW_HIT_RATES.get(tier)

    print(f"\n{'='*120}")
    print(f"VOLUME FLOW BACKTEST — {len(vf_markets):,} markets, start capital ${START_CAPITAL:,.0f}")
    print(f"{'='*120}")

    # Tier breakdown
    tiers = {}
    for m in vf_markets:
        t = get_flow_tier(m["flow_ratio"])
        tiers[t] = tiers.get(t, 0) + 1
    for t, c in sorted(tiers.items()):
        hr = FLOW_HIT_RATES.get(t, 0)
        kf = kelly_fraction(hr)
        print(f"  {t:10s}: {c:>6,} markets | hit_rate: {hr:.1%} | kelly: {kf:.1%}")

    print(f"\n--- Main Results ---")
    for sizing in SIZINGS:
        r = simulate_equity(vf_markets, sizing, START_CAPITAL, vf_hit_rate)
        r.strategy = "Volume Flow"
        print_result(r)

    # Walk-Forward
    print(f"\n--- Walk-Forward (Quarter-Kelly, 5 windows) ---")
    wf = walk_forward(vf_markets, "quarter_kelly", START_CAPITAL, vf_hit_rate)
    for r in wf:
        print_result(r)

    # Monte Carlo
    print(f"\n--- Monte Carlo (Quarter-Kelly, 1000 shuffles) ---")
    mc = monte_carlo(vf_markets, "quarter_kelly", START_CAPITAL, vf_hit_rate, 1000)
    print(f"  Median final capital: ${mc['median_capital']:,.2f}")
    print(f"  5th percentile:       ${mc['p5_capital']:,.2f}")
    print(f"  95th percentile:      ${mc['p95_capital']:,.2f}")
    print(f"  Median max drawdown:  {mc['median_dd']:.1f}%")
    print(f"  95th pctile drawdown: {mc['p95_dd']:.1f}%")
    print(f"  Prob profitable:      {mc['prob_profitable']:.1%}")

    # ─── CONTRARIAN WHALE BACKTEST ────────────────────────────────────────
    cw_markets = load_contrarian_whale_markets(conn)

    def cw_hit_rate(m):
        return WHALE_HIT_RATES.get(m["flow_type"])

    print(f"\n{'='*120}")
    print(f"CONTRARIAN WHALE BACKTEST — {len(cw_markets):,} outcomes, start capital ${START_CAPITAL:,.0f}")
    print(f"{'='*120}")

    # Type breakdown
    types = {}
    for m in cw_markets:
        ft = m["flow_type"]
        types[ft] = types.get(ft, 0) + 1
    for ft, c in sorted(types.items()):
        hr = WHALE_HIT_RATES.get(ft, 0)
        kf = kelly_fraction(hr)
        print(f"  {ft:18s}: {c:>6,} outcomes | hit_rate: {hr:.1%} | kelly: {kf:.1%}")

    print(f"\n--- Main Results ---")
    for sizing in SIZINGS:
        r = simulate_equity(cw_markets, sizing, START_CAPITAL, cw_hit_rate)
        r.strategy = "Contrarian Whale"
        print_result(r)

    # Walk-Forward
    print(f"\n--- Walk-Forward (Quarter-Kelly, 5 windows) ---")
    wf = walk_forward(cw_markets, "quarter_kelly", START_CAPITAL, cw_hit_rate)
    for r in wf:
        print_result(r)

    # Monte Carlo
    print(f"\n--- Monte Carlo (Quarter-Kelly, 1000 shuffles) ---")
    mc = monte_carlo(cw_markets, "quarter_kelly", START_CAPITAL, cw_hit_rate, 1000)
    print(f"  Median final capital: ${mc['median_capital']:,.2f}")
    print(f"  5th percentile:       ${mc['p5_capital']:,.2f}")
    print(f"  95th percentile:      ${mc['p95_capital']:,.2f}")
    print(f"  Median max drawdown:  {mc['median_dd']:.1f}%")
    print(f"  95th pctile drawdown: {mc['p95_dd']:.1f}%")
    print(f"  Prob profitable:      {mc['prob_profitable']:.1%}")

    conn.close()
    print(f"\n{'='*120}")
    print("BACKTEST COMPLETE")
    print(f"{'='*120}")


if __name__ == "__main__":
    main()
