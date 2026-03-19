"""
Strategy Discovery Engine — Mines 288M+ blockchain trades for statistical
patterns that predict market outcomes. Discovered patterns are stored as
strategies in the DB for backtesting and live trading.

Uses DuckDB to join:
  trades (Parquet) × token_to_market (Parquet) × resolutions (Parquet)
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
    dimension: str          # e.g. "price_bucket", "volume_flow", "category"
    filters: dict           # raw filter values
    sample_size: int
    hit_rate: float         # fraction of trades on winning side
    avg_price: float        # average entry price
    expected_edge: float    # hit_rate * (1/avg_price) - 1
    entry_rules: list       # compatible with strategy_evaluator
    trade_params: dict      # side, sizing, etc.


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
    # Check at least some trade files exist
    trade_files = list(Path("data/blockchain/trades").glob("trades_*.parquet"))
    if not trade_files:
        logger.error("No trade Parquet files found")
        return False
    return True


def mine_price_bucket_patterns(conn) -> list[DiscoveredPattern]:
    """Pattern A: Which price ranges have edge when buying the winning side?

    Checks: At different price levels, how often does a trade end up on the
    winning side? If hit_rate × payout > 1, there's edge.
    """
    logger.info("Mining price bucket patterns...")

    results = conn.execute("""
        SELECT
            price_bucket,
            COUNT(*) as total_trades,
            SUM(CASE WHEN is_winner THEN 1 ELSE 0 END) as winning_trades,
            AVG(price_per_token) as avg_price
        FROM (
            SELECT
                tm.is_winner,
                t.maker_amount * 1.0 / NULLIF(t.taker_amount, 0) as price_per_token,
                CASE
                    WHEN t.maker_amount * 1.0 / NULLIF(t.taker_amount, 0) < 0.15 THEN 'deep_value'
                    WHEN t.maker_amount * 1.0 / NULLIF(t.taker_amount, 0) < 0.30 THEN 'value'
                    WHEN t.maker_amount * 1.0 / NULLIF(t.taker_amount, 0) < 0.50 THEN 'mid'
                    WHEN t.maker_amount * 1.0 / NULLIF(t.taker_amount, 0) < 0.70 THEN 'favorite'
                    WHEN t.maker_amount * 1.0 / NULLIF(t.taker_amount, 0) < 0.85 THEN 'heavy_fav'
                    ELSE 'near_certain'
                END as price_bucket
            FROM read_parquet('{}') t
            JOIN read_parquet('{}') tm ON t.taker_asset_id = tm.token_id
            WHERE t.maker_amount > 0 AND t.taker_amount > 0
              AND t.maker_amount * 1.0 / t.taker_amount > 0.01
              AND t.maker_amount * 1.0 / t.taker_amount < 0.99
        ) sub
        GROUP BY price_bucket
        HAVING COUNT(*) >= 1000
        ORDER BY price_bucket
    """.format(TRADES_GLOB, TOKEN_MAP)).fetchall()

    patterns = []
    for row in results:
        bucket, total, wins, avg_price = row
        hit_rate = wins / total if total > 0 else 0
        # Expected edge: if you buy at avg_price and win with hit_rate
        # Payout is 1.0 per token, cost is avg_price
        # EV = hit_rate * (1.0 / avg_price) - 1.0 ... but that's per-token
        # Better: EV = hit_rate * 1.0 - avg_price (profit per $1 of tokens)
        # Simplified: edge = hit_rate - avg_price (how much better than random)
        expected_edge = hit_rate - avg_price

        if expected_edge < 0.02:
            continue  # skip patterns with <2% edge

        # Map bucket to price range
        price_ranges = {
            "deep_value": (0.01, 0.15),
            "value": (0.15, 0.30),
            "mid": (0.30, 0.50),
            "favorite": (0.50, 0.70),
            "heavy_fav": (0.70, 0.85),
            "near_certain": (0.85, 0.99),
        }
        lo, hi = price_ranges.get(bucket, (0, 1))

        pattern = DiscoveredPattern(
            pattern_id=f"price_{bucket}",
            name=f"Price Bucket: {bucket}",
            description=(
                f"Buy tokens in the {bucket} range ({lo:.0%}-{hi:.0%}). "
                f"Hit rate: {hit_rate:.1%}, avg price: {avg_price:.2f}, "
                f"edge: {expected_edge:.1%} over {total:,} trades"
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
        logger.info(f"  {bucket}: {hit_rate:.1%} hit rate, {expected_edge:.1%} edge ({total:,} trades)")

    return patterns


def mine_volume_flow_patterns(conn) -> list[DiscoveredPattern]:
    """Pattern B: Does buy/sell volume imbalance predict the winner?

    For each resolved market, compute the ratio of volume on the winning side
    vs losing side. Then check: if you follow the volume, what's the edge?
    """
    logger.info("Mining volume flow patterns...")

    results = conn.execute("""
        SELECT
            flow_bucket,
            COUNT(*) as total_markets,
            SUM(CASE WHEN dominant_side_won THEN 1 ELSE 0 END) as correct,
            AVG(flow_ratio) as avg_flow_ratio
        FROM (
            SELECT
                condition_id,
                winning_side_vol,
                losing_side_vol,
                winning_side_vol / NULLIF(losing_side_vol, 0) as flow_ratio,
                winning_side_vol > losing_side_vol as dominant_side_won,
                CASE
                    WHEN winning_side_vol / NULLIF(losing_side_vol, 0) > 5.0 THEN 'extreme_flow'
                    WHEN winning_side_vol / NULLIF(losing_side_vol, 0) > 2.0 THEN 'strong_flow'
                    WHEN winning_side_vol / NULLIF(losing_side_vol, 0) > 1.2 THEN 'mild_flow'
                    ELSE 'balanced'
                END as flow_bucket
            FROM (
                SELECT
                    tm.condition_id,
                    SUM(CASE WHEN tm.is_winner THEN t.maker_amount ELSE 0 END) as winning_side_vol,
                    SUM(CASE WHEN NOT tm.is_winner THEN t.maker_amount ELSE 0 END) as losing_side_vol
                FROM read_parquet('{}') t
                JOIN read_parquet('{}') tm ON t.taker_asset_id = tm.token_id
                WHERE t.maker_amount > 0
                GROUP BY tm.condition_id
                HAVING COUNT(*) >= 10  -- markets with enough trades
            ) market_flows
        ) bucketed
        GROUP BY flow_bucket
        HAVING COUNT(*) >= 100
    """.format(TRADES_GLOB, TOKEN_MAP)).fetchall()

    patterns = []
    for row in results:
        bucket, total, correct, avg_ratio = row
        hit_rate = correct / total if total > 0 else 0

        if hit_rate <= 0.52:
            continue  # Need meaningful prediction

        pattern = DiscoveredPattern(
            pattern_id=f"flow_{bucket}",
            name=f"Volume Flow: {bucket}",
            description=(
                f"Markets with {bucket} volume imbalance. "
                f"Dominant-side wins {hit_rate:.1%} of the time "
                f"(avg flow ratio: {avg_ratio:.1f}x) over {total:,} markets"
            ),
            dimension="volume_flow",
            filters={"flow_bucket": bucket, "avg_flow_ratio": avg_ratio},
            sample_size=total,
            hit_rate=hit_rate,
            avg_price=0.5,  # mid-market assumption
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
        logger.info(f"  {bucket}: {hit_rate:.1%} correct ({total:,} markets, avg ratio {avg_ratio:.1f}x)")

    return patterns


def mine_trade_size_patterns(conn) -> list[DiscoveredPattern]:
    """Pattern E: Do larger traders have better hit rates?

    Hypothesis: Big traders (>$100 per trade) are more informed.
    """
    logger.info("Mining trade size patterns...")

    results = conn.execute("""
        SELECT
            size_bucket,
            COUNT(*) as total_trades,
            SUM(CASE WHEN is_winner THEN 1 ELSE 0 END) as winning_trades,
            AVG(trade_usd) as avg_trade_usd
        FROM (
            SELECT
                tm.is_winner,
                t.maker_amount / 1e6 as trade_usd,
                CASE
                    WHEN t.maker_amount / 1e6 < 5 THEN 'micro'
                    WHEN t.maker_amount / 1e6 < 50 THEN 'small'
                    WHEN t.maker_amount / 1e6 < 500 THEN 'medium'
                    WHEN t.maker_amount / 1e6 < 5000 THEN 'large'
                    ELSE 'whale'
                END as size_bucket
            FROM read_parquet('{}') t
            JOIN read_parquet('{}') tm ON t.taker_asset_id = tm.token_id
            WHERE t.maker_amount > 0 AND t.taker_amount > 0
        ) sub
        GROUP BY size_bucket
        HAVING COUNT(*) >= 1000
        ORDER BY avg_trade_usd
    """.format(TRADES_GLOB, TOKEN_MAP)).fetchall()

    patterns = []
    for row in results:
        bucket, total, wins, avg_usd = row
        hit_rate = wins / total if total > 0 else 0
        edge = hit_rate - 0.5  # vs random

        if edge < 0.02:
            continue

        pattern = DiscoveredPattern(
            pattern_id=f"size_{bucket}",
            name=f"Trade Size: {bucket}",
            description=(
                f"Trades in {bucket} range (avg ${avg_usd:.0f}). "
                f"Hit rate: {hit_rate:.1%}, edge vs random: {edge:.1%} ({total:,} trades)"
            ),
            dimension="trade_size",
            filters={"size_bucket": bucket, "avg_usd": avg_usd},
            sample_size=total,
            hit_rate=hit_rate,
            avg_price=0.5,
            expected_edge=edge,
            entry_rules=[],
            trade_params={
                "side": "YES",
                "sizing_method": "fixed_amount",
                "fixed_amount_usd": 2.0,
                "min_edge": 0.03,
            },
        )
        patterns.append(pattern)
        logger.info(f"  {bucket} (avg ${avg_usd:.0f}): {hit_rate:.1%} hit rate, {edge:.1%} edge ({total:,} trades)")

    return patterns


def mine_maker_taker_patterns(conn) -> list[DiscoveredPattern]:
    """Pattern F: Are taker trades (crossing the spread) more informed?

    Takers pay fees and accept worse prices — they typically have information.
    """
    logger.info("Mining maker vs taker patterns...")

    # Check if the trade data has 'contract' field to distinguish roles
    # In our data, 'maker' is the address, not the role in the specific trade
    # The maker_asset_id side has the maker (limit order placer)
    # The taker_asset_id side has the taker (market order filler)

    # We compare: do takers pick winners more often than makers?
    results = conn.execute("""
        SELECT
            role,
            COUNT(*) as total_trades,
            SUM(CASE WHEN is_winner THEN 1 ELSE 0 END) as winning_trades
        FROM (
            -- Taker perspective: taker buys the taker_asset_id token
            SELECT 'taker' as role, tm.is_winner
            FROM read_parquet('{}') t
            JOIN read_parquet('{}') tm ON t.taker_asset_id = tm.token_id
            WHERE t.maker_amount > 0 AND t.taker_amount > 0

            UNION ALL

            -- Maker perspective: maker holds the maker_asset_id token
            SELECT 'maker' as role, tm.is_winner
            FROM read_parquet('{}') t
            JOIN read_parquet('{}') tm ON t.maker_asset_id = tm.token_id
            WHERE t.maker_amount > 0 AND t.taker_amount > 0
        ) combined
        GROUP BY role
    """.format(TRADES_GLOB, TOKEN_MAP, TRADES_GLOB, TOKEN_MAP)).fetchall()

    patterns = []
    for row in results:
        role, total, wins = row
        hit_rate = wins / total if total > 0 else 0
        edge = hit_rate - 0.5

        logger.info(f"  {role}: {hit_rate:.1%} hit rate ({total:,} trades)")

        if edge < 0.01:
            continue

        pattern = DiscoveredPattern(
            pattern_id=f"role_{role}",
            name=f"Role: {role} advantage",
            description=(
                f"{role.title()} trades have {hit_rate:.1%} hit rate "
                f"(edge: {edge:.1%} over {total:,} trades)"
            ),
            dimension="maker_taker",
            filters={"role": role},
            sample_size=total,
            hit_rate=hit_rate,
            avg_price=0.5,
            expected_edge=edge,
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


def run_discovery(min_edge: float = 0.03, min_sample: int = 1000) -> dict:
    """Run the full pattern discovery pipeline.

    Returns dict with discovered patterns and stats.
    """
    if not _check_data_files():
        return {"ok": False, "error": "Missing data files"}

    try:
        conn = _get_conn()
    except ImportError:
        return {"ok": False, "error": "duckdb not installed"}

    logger.info("Starting Strategy Discovery...")
    logger.info(f"Thresholds: min_edge={min_edge:.1%}, min_sample={min_sample:,}")

    all_patterns = []

    # Run each mining dimension
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

    conn.close()

    # Filter by thresholds
    qualified = [
        p for p in all_patterns
        if p.expected_edge >= min_edge and p.sample_size >= min_sample
    ]

    # Sort by expected edge
    qualified.sort(key=lambda p: p.expected_edge, reverse=True)

    logger.info(
        f"Discovery complete: {len(all_patterns)} patterns found, "
        f"{len(qualified)} qualify (edge >= {min_edge:.1%}, sample >= {min_sample:,})"
    )

    for p in qualified:
        logger.info(f"  ✓ {p.name}: {p.expected_edge:.1%} edge, {p.sample_size:,} trades")

    return {
        "ok": True,
        "total_patterns": len(all_patterns),
        "qualified_patterns": len(qualified),
        "patterns": [asdict(p) for p in qualified],
        "all_patterns": [asdict(p) for p in all_patterns],
        "discovery_time": datetime.now(timezone.utc).isoformat(),
    }


def save_patterns_as_strategies(patterns: list[dict]) -> list[str]:
    """Save discovered patterns as strategy entries in the DB.

    Args:
        patterns: List of pattern dicts (from run_discovery()["patterns"])

    Returns:
        List of created strategy IDs.
    """
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
