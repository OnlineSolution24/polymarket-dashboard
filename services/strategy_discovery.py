"""
Strategy Discovery Engine — Mines 288M+ blockchain trades for statistical
patterns that predict market outcomes. Discovered patterns are stored as
strategies in the DB for backtesting and live trading.

Uses DuckDB to join:
  trades (Parquet) × token_to_market (Parquet) × resolutions (Parquet)

Data model:
  - maker_asset_id = '0' means maker is paying USDC (buying tokens)
  - taker_asset_id = '0' means taker is paying USDC (selling tokens)
  - Price per token = USDC amount / token amount
  - Amounts are in raw units (USDC has 6 decimals, tokens have 6 decimals)
"""

import json
import logging
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger(__name__)

TRADES_GLOB = "data/blockchain/trades/trades_*.parquet"
TOKEN_MAP = "data/blockchain/token_to_market.parquet"
RESOLUTIONS = "data/blockchain/resolutions.parquet"


@dataclass
class DiscoveredPattern:
    """A statistically significant trading pattern."""
    pattern_id: str
    name: str
    description: str
    dimension: str
    filters: dict
    sample_size: int
    hit_rate: float
    avg_price: float
    expected_edge: float
    entry_rules: list
    trade_params: dict


def _get_conn():
    """Get a DuckDB connection with memory limits for large queries."""
    import duckdb
    conn = duckdb.connect()
    conn.execute("SET temp_directory = '/tmp/duckdb_temp'")
    conn.execute("SET memory_limit = '4GB'")
    return conn


def _check_data_files() -> bool:
    """Verify all required Parquet files exist."""
    for path in [TOKEN_MAP, RESOLUTIONS]:
        if not Path(path).exists():
            logger.error(f"Missing required file: {path}")
            return False
    trade_files = list(Path("data/blockchain/trades").glob("trades_*.parquet"))
    if not trade_files:
        logger.error("No trade Parquet files found")
        return False
    return True


# ---------------------------------------------------------------------------
# Common SQL fragments
# ---------------------------------------------------------------------------
# When maker_asset_id = '0', maker pays USDC to buy tokens from taker.
# The token being traded is the taker_asset_id (outcome token).
# Price = maker_amount / taker_amount (USDC per token).
#
# When maker_asset_id != '0' and taker_asset_id matches a token,
# the maker is selling tokens. We still join on taker_asset_id = token_id
# but the price interpretation differs.
#
# For simplicity and correctness, we focus on "buy" trades where
# maker_asset_id = '0' (USDC side), which is ~288M of our trades.

BUY_TRADES_SQL = """
    SELECT
        t.block_number,
        t.maker,
        t.taker,
        t.maker_amount,   -- USDC amount (raw, /1e6 for dollars)
        t.taker_amount,   -- Token amount (raw, /1e6 for tokens)
        t.fee,
        tm.condition_id,
        tm.outcome,
        tm.is_winner,
        t.maker_amount * 1.0 / NULLIF(t.taker_amount, 0) as price_per_token,
        t.maker_amount / 1e6 as usdc_amount
    FROM read_parquet('{trades}') t
    JOIN read_parquet('{token_map}') tm ON t.taker_asset_id = tm.token_id
    WHERE t.maker_asset_id = '0'
      AND t.maker_amount > 0
      AND t.taker_amount > 0
""".format(trades=TRADES_GLOB, token_map=TOKEN_MAP)


def mine_price_bucket_patterns(conn) -> list[DiscoveredPattern]:
    """Pattern A: At which price levels do outcome tokens get bought correctly?

    If you buy a token at 20 cents and it wins, you get $1 back = 5x return.
    The question is: at each price level, what fraction of bought tokens
    end up being winners?
    """
    logger.info("Mining price bucket patterns...")

    results = conn.execute(f"""
        SELECT
            price_bucket,
            COUNT(*) as total_trades,
            SUM(CASE WHEN is_winner THEN 1 ELSE 0 END) as winning_trades,
            AVG(price_per_token) as avg_price
        FROM (
            SELECT
                is_winner,
                price_per_token,
                CASE
                    WHEN price_per_token < 0.10 THEN 'longshot'
                    WHEN price_per_token < 0.20 THEN 'deep_value'
                    WHEN price_per_token < 0.35 THEN 'value'
                    WHEN price_per_token < 0.50 THEN 'mid'
                    WHEN price_per_token < 0.65 THEN 'lean_fav'
                    WHEN price_per_token < 0.80 THEN 'favorite'
                    WHEN price_per_token < 0.92 THEN 'heavy_fav'
                    ELSE 'near_certain'
                END as price_bucket
            FROM ({BUY_TRADES_SQL}) buys
            WHERE price_per_token > 0.01 AND price_per_token < 0.99
        ) sub
        GROUP BY price_bucket
        HAVING COUNT(*) >= 1000
        ORDER BY avg_price
    """).fetchall()

    patterns = []
    for row in results:
        bucket, total, wins, avg_price = row
        hit_rate = wins / total if total > 0 else 0

        # Edge = EV - 1 = hit_rate * (1/avg_price) - 1
        # e.g. hit_rate=0.30 at avg_price=0.15 → EV = 0.30/0.15 = 2.0 → edge = +100%
        # e.g. hit_rate=0.50 at avg_price=0.50 → EV = 1.0 → edge = 0% (fair)
        ev_per_dollar = hit_rate / avg_price if avg_price > 0 else 0
        expected_edge = ev_per_dollar - 1.0

        price_ranges = {
            "longshot": (0.01, 0.10),
            "deep_value": (0.10, 0.20),
            "value": (0.20, 0.35),
            "mid": (0.35, 0.50),
            "lean_fav": (0.50, 0.65),
            "favorite": (0.65, 0.80),
            "heavy_fav": (0.80, 0.92),
            "near_certain": (0.92, 0.99),
        }
        lo, hi = price_ranges.get(bucket, (0, 1))

        logger.info(
            f"  {bucket} ({lo:.0%}-{hi:.0%}): hit_rate={hit_rate:.1%}, "
            f"avg_price={avg_price:.3f}, EV={ev_per_dollar:.2f}, "
            f"edge={expected_edge:+.1%} ({total:,} trades)"
        )

        if expected_edge < 0.02:
            continue

        pattern = DiscoveredPattern(
            pattern_id=f"price_{bucket}",
            name=f"Price Bucket: {bucket}",
            description=(
                f"Buy tokens at {lo:.0%}-{hi:.0%}. "
                f"Hit rate: {hit_rate:.1%}, avg price: {avg_price:.2f}, "
                f"EV per $1: ${ev_per_dollar:.2f}, edge: {expected_edge:+.1%} ({total:,} trades)"
            ),
            dimension="price_bucket",
            filters={"price_min": lo, "price_max": hi},
            sample_size=total,
            hit_rate=hit_rate,
            avg_price=avg_price,
            expected_edge=expected_edge,
            entry_rules=[
                {"field": "yes_price", "op": "gte", "value": lo},
                {"field": "yes_price", "op": "lte", "value": hi},
            ],
            trade_params={
                "side": "YES",
                "sizing_method": "fixed_amount",
                "fixed_amount_usd": 2.0,
                "min_edge": 0.03,
            },
        )
        patterns.append(pattern)

    return patterns


def mine_volume_flow_patterns(conn) -> list[DiscoveredPattern]:
    """Pattern B: Does early volume (first 50% of trades) predict the winner?

    Only counts volume from the FIRST HALF of a market's trading life.
    This avoids the data-leak of counting volume after the outcome is known.
    """
    logger.info("Mining early volume flow patterns...")

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
            HAVING COUNT(*) >= 20
        ),
        early_flows AS (
            SELECT
                tm.condition_id,
                SUM(CASE WHEN tm.is_winner THEN t.maker_amount ELSE 0 END) as winning_vol,
                SUM(CASE WHEN NOT tm.is_winner THEN t.maker_amount ELSE 0 END) as losing_vol,
                COUNT(*) as trade_count
            FROM read_parquet('{TRADES_GLOB}') t
            JOIN read_parquet('{TOKEN_MAP}') tm ON t.taker_asset_id = tm.token_id
            JOIN market_ranges mr ON tm.condition_id = mr.condition_id
            WHERE t.maker_asset_id = '0'
              AND t.maker_amount > 0
              AND t.block_number <= mr.mid_block  -- ONLY first half of trading
            GROUP BY tm.condition_id
            HAVING COUNT(*) >= 5
        )
        SELECT
            flow_bucket,
            COUNT(*) as total_markets,
            SUM(CASE WHEN winning_vol > losing_vol THEN 1 ELSE 0 END) as dominant_won,
            AVG(CASE WHEN winning_vol > losing_vol
                THEN winning_vol * 1.0 / NULLIF(losing_vol, 0)
                ELSE losing_vol * 1.0 / NULLIF(winning_vol, 0)
            END) as avg_ratio
        FROM (
            SELECT
                condition_id,
                winning_vol,
                losing_vol,
                CASE
                    WHEN GREATEST(winning_vol, losing_vol) / NULLIF(LEAST(winning_vol, losing_vol), 0) > 5.0 THEN 'extreme'
                    WHEN GREATEST(winning_vol, losing_vol) / NULLIF(LEAST(winning_vol, losing_vol), 0) > 2.0 THEN 'strong'
                    WHEN GREATEST(winning_vol, losing_vol) / NULLIF(LEAST(winning_vol, losing_vol), 0) > 1.3 THEN 'mild'
                    ELSE 'balanced'
                END as flow_bucket
            FROM early_flows
        ) bucketed
        GROUP BY flow_bucket
        HAVING COUNT(*) >= 100
    """).fetchall()

    patterns = []
    for row in results:
        bucket, total, dominant_won, avg_ratio = row
        hit_rate = dominant_won / total if total > 0 else 0

        logger.info(
            f"  early_{bucket}: {hit_rate:.1%} correct "
            f"(avg ratio {avg_ratio:.1f}x, {total:,} markets)"
        )

        if hit_rate <= 0.52:
            continue

        pattern = DiscoveredPattern(
            pattern_id=f"early_flow_{bucket}",
            name=f"Early Volume Flow: {bucket}",
            description=(
                f"Markets where early trading (first half) shows {bucket} "
                f"volume imbalance. Dominant side wins {hit_rate:.1%} "
                f"({total:,} markets)"
            ),
            dimension="early_volume_flow",
            filters={"flow_bucket": bucket, "avg_ratio": avg_ratio},
            sample_size=total,
            hit_rate=hit_rate,
            avg_price=0.5,
            expected_edge=hit_rate - 0.5,
            entry_rules=[
                {"field": "volume", "op": "gte", "value": 50000},
            ],
            trade_params={
                "side": "YES",
                "sizing_method": "fixed_amount",
                "fixed_amount_usd": 2.0,
                "min_edge": 0.03,
            },
        )
        patterns.append(pattern)

    return patterns


def mine_trade_size_patterns(conn) -> list[DiscoveredPattern]:
    """Pattern E: Do larger traders pick winners more often?"""
    logger.info("Mining trade size patterns...")

    results = conn.execute(f"""
        SELECT
            size_bucket,
            COUNT(*) as total_trades,
            SUM(CASE WHEN is_winner THEN 1 ELSE 0 END) as winning_trades,
            AVG(usdc_amount) as avg_trade_usd,
            AVG(price_per_token) as avg_price
        FROM (
            SELECT
                is_winner,
                usdc_amount,
                price_per_token,
                CASE
                    WHEN usdc_amount < 5 THEN 'micro'
                    WHEN usdc_amount < 50 THEN 'small'
                    WHEN usdc_amount < 500 THEN 'medium'
                    WHEN usdc_amount < 5000 THEN 'large'
                    ELSE 'whale'
                END as size_bucket
            FROM ({BUY_TRADES_SQL}) buys
            WHERE price_per_token > 0.01 AND price_per_token < 0.99
        ) sub
        GROUP BY size_bucket
        HAVING COUNT(*) >= 1000
        ORDER BY avg_trade_usd
    """).fetchall()

    patterns = []
    for row in results:
        bucket, total, wins, avg_usd, avg_price = row
        hit_rate = wins / total if total > 0 else 0

        # Edge relative to the price they paid
        ev_per_dollar = hit_rate / avg_price if avg_price > 0 else 0
        expected_edge = ev_per_dollar - 1.0

        logger.info(
            f"  {bucket} (avg ${avg_usd:.0f}): hit_rate={hit_rate:.1%}, "
            f"avg_price={avg_price:.3f}, edge={expected_edge:+.1%} ({total:,} trades)"
        )

        if expected_edge < 0.02:
            continue

        pattern = DiscoveredPattern(
            pattern_id=f"size_{bucket}",
            name=f"Trade Size: {bucket}",
            description=(
                f"Trades sized as {bucket} (avg ${avg_usd:.0f}). "
                f"Hit rate: {hit_rate:.1%}, edge: {expected_edge:+.1%} ({total:,} trades)"
            ),
            dimension="trade_size",
            filters={"size_bucket": bucket, "avg_usd": avg_usd},
            sample_size=total,
            hit_rate=hit_rate,
            avg_price=avg_price,
            expected_edge=expected_edge,
            entry_rules=[],
            trade_params={
                "side": "YES",
                "sizing_method": "fixed_amount",
                "fixed_amount_usd": 2.0,
                "min_edge": 0.03,
            },
        )
        patterns.append(pattern)

    return patterns


def mine_maker_taker_patterns(conn) -> list[DiscoveredPattern]:
    """Pattern F: Do takers (market orders) pick winners better than makers?"""
    logger.info("Mining maker vs taker patterns...")

    # In buy trades (maker_asset_id = '0'):
    # - The 'taker' address is the one filling the order (market order = informed)
    # - The 'maker' address placed the limit order (passive)
    # We check: do taker-address wallets have better hit rates than maker-address wallets?

    # Simpler approach: compare hit rates for trades where our token side
    # matches the winning outcome. Since all these are "buy" trades
    # (maker pays USDC, taker provides tokens), the maker is BUYING
    # and the taker is SELLING.
    #
    # Actually in CTF exchange OrderFilled:
    # - maker = the address whose order was resting
    # - taker = the address who filled (crossed the spread)
    #
    # Both are "involved" but the taker is the active participant.
    # We just report the overall buy-side hit rate for context.

    results = conn.execute(f"""
        SELECT
            'buy_trades' as category,
            COUNT(*) as total_trades,
            SUM(CASE WHEN is_winner THEN 1 ELSE 0 END) as winning_trades,
            AVG(price_per_token) as avg_price
        FROM ({BUY_TRADES_SQL}) buys
        WHERE price_per_token > 0.01 AND price_per_token < 0.99
    """).fetchall()

    patterns = []
    for row in results:
        category, total, wins, avg_price = row
        hit_rate = wins / total if total > 0 else 0
        ev = hit_rate / avg_price if avg_price > 0 else 0

        logger.info(
            f"  Overall buy trades: hit_rate={hit_rate:.1%}, "
            f"avg_price={avg_price:.3f}, EV={ev:.3f} ({total:,} trades)"
        )

    return patterns


def run_discovery(min_edge: float = 0.03, min_sample: int = 1000) -> dict:
    """Run the full pattern discovery pipeline."""
    if not _check_data_files():
        return {"ok": False, "error": "Missing data files"}

    try:
        conn = _get_conn()
    except ImportError:
        return {"ok": False, "error": "duckdb not installed"}

    logger.info("Starting Strategy Discovery...")
    logger.info(f"Thresholds: min_edge={min_edge:.1%}, min_sample={min_sample:,}")

    all_patterns = []

    miners = [
        mine_price_bucket_patterns,
        mine_volume_flow_patterns,
        mine_trade_size_patterns,
        mine_maker_taker_patterns,
    ]

    for miner in miners:
        try:
            patterns = miner(conn)
            all_patterns.extend(patterns)
        except Exception as e:
            logger.error(f"Error in {miner.__name__}: {e}")
            import traceback
            traceback.print_exc()

    conn.close()

    qualified = [
        p for p in all_patterns
        if p.expected_edge >= min_edge and p.sample_size >= min_sample
    ]
    qualified.sort(key=lambda p: p.expected_edge, reverse=True)

    logger.info(
        f"Discovery complete: {len(all_patterns)} patterns found, "
        f"{len(qualified)} qualify (edge >= {min_edge:.1%}, sample >= {min_sample:,})"
    )

    for p in qualified:
        logger.info(f"  >> {p.name}: {p.expected_edge:+.1%} edge, {p.sample_size:,} trades")

    return {
        "ok": True,
        "total_patterns": len(all_patterns),
        "qualified_patterns": len(qualified),
        "patterns": [asdict(p) for p in qualified],
        "all_patterns": [asdict(p) for p in all_patterns],
        "discovery_time": datetime.now(timezone.utc).isoformat(),
    }


def save_patterns_as_strategies(patterns: list[dict]) -> list[str]:
    """Save discovered patterns as strategy entries in the DB."""
    try:
        from db import engine
    except ImportError:
        logger.error("Cannot import db engine")
        return []

    created_ids = []

    for pattern in patterns:
        strategy_id = f"disc_{pattern['pattern_id']}_{uuid.uuid4().hex[:6]}"

        definition = json.dumps({
            "entry_rules": pattern.get("entry_rules", []),
            "exit_rules": [],
            "trade_params": pattern.get("trade_params", {}),
            "discovery_metadata": {
                "dimension": pattern.get("dimension"),
                "sample_size": pattern.get("sample_size"),
                "hit_rate": pattern.get("hit_rate"),
                "expected_edge": pattern.get("expected_edge"),
                "filters": pattern.get("filters"),
            },
        })

        try:
            engine.execute(
                """INSERT INTO strategies
                   (id, name, description, definition, status, discovered_by, created_at, updated_at)
                   VALUES (?, ?, ?, ?, 'pending_backtest', 'strategy_discovery', datetime('now'), datetime('now'))""",
                (strategy_id, pattern["name"], pattern["description"], definition),
            )
            created_ids.append(strategy_id)
            logger.info(f"Created strategy: {strategy_id} ({pattern['name']})")
        except Exception as e:
            logger.error(f"Error saving strategy {strategy_id}: {e}")

    return created_ids
