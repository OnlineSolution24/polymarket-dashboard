"""
Historical Analytics — DuckDB queries on Parquet blockchain trade data.

Provides deep wallet analysis and market flow data using the full
historical trade dataset collected by the blockchain indexer.

All queries run on Parquet files via DuckDB — no separate database needed.

Performance strategy:
- Pre-aggregated wallet stats file (rebuilt periodically in background)
- Alpha Scanner reads from the summary file (instant) instead of scanning 38K files
- Heavy DuckDB queries only run during background refresh
"""

import json
import logging
import time as _time
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger(__name__)

TRADES_DIR = Path("data/blockchain/trades")
WALLET_STATS_FILE = Path("data/blockchain/wallet_stats.json")

# -----------------------------------------------------------------------
# Connection pool — reuse DuckDB connection
# -----------------------------------------------------------------------
_duckdb_conn = None


def _has_data() -> bool:
    """Check if blockchain trade data is available."""
    return TRADES_DIR.exists() and any(TRADES_DIR.glob("trades_*.parquet"))


def _get_duckdb():
    """Get DuckDB module (lazy import)."""
    try:
        import duckdb
        return duckdb
    except ImportError:
        logger.warning("DuckDB not installed — historical analytics unavailable")
        return None


def _get_conn():
    """Get a persistent DuckDB connection."""
    global _duckdb_conn
    duckdb = _get_duckdb()
    if not duckdb:
        return None
    if _duckdb_conn is None:
        _duckdb_conn = duckdb.connect()
    return _duckdb_conn


def _trades_glob() -> str:
    """Get the glob pattern for all trade Parquet files."""
    return str(TRADES_DIR / "trades_*.parquet")


# -----------------------------------------------------------------------
# Time-based cache for expensive queries
# -----------------------------------------------------------------------
_cache: dict[str, tuple[float, object]] = {}
CACHE_TTL = 3600  # 1 hour


def _cached(key: str):
    if key in _cache:
        ts, val = _cache[key]
        if _time.time() - ts < CACHE_TTL:
            return val
    return None


def _set_cache(key: str, val: object):
    _cache[key] = (_time.time(), val)


# -----------------------------------------------------------------------
# Pre-aggregated wallet stats (instant lookups for Alpha Scanner)
# -----------------------------------------------------------------------

def _load_wallet_stats() -> dict[str, dict]:
    """Load pre-aggregated wallet stats from JSON file."""
    cached = _cached("wallet_stats_file")
    if cached is not None:
        return cached

    if not WALLET_STATS_FILE.exists():
        return {}

    try:
        data = json.loads(WALLET_STATS_FILE.read_text())
        _set_cache("wallet_stats_file", data)
        return data
    except Exception as e:
        logger.error(f"Error loading wallet stats: {e}")
        return {}


def rebuild_wallet_stats() -> int:
    """
    Rebuild the pre-aggregated wallet stats file from Parquet data.

    Splits into two memory-efficient queries (maker-side + taker-side)
    to avoid OOM on 382M+ trades. Uses DuckDB temp directory for
    disk spilling when RAM is insufficient.

    Returns number of wallets processed.
    """
    conn = _get_conn()
    if not conn or not _has_data():
        return 0

    logger.info("Rebuilding wallet stats from blockchain data...")
    start = _time.time()

    try:
        # Enable disk spilling for large datasets
        conn.execute("SET temp_directory = '/tmp/duckdb_temp'")
        conn.execute("SET memory_limit = '4GB'")

        # Step 1: Maker-side stats (no UNION ALL, half the data)
        logger.info("Step 1/3: Aggregating maker-side stats...")
        conn.execute(f"""
            CREATE OR REPLACE TEMP TABLE maker_stats AS
            SELECT
                LOWER(maker) as wallet,
                COUNT(*) as trade_count,
                SUM(CASE WHEN maker_asset_id = '0' THEN maker_amount / 1e6 ELSE 0 END) as usdc_spent,
                SUM(CASE WHEN taker_asset_id = '0' THEN taker_amount / 1e6 ELSE 0 END) as usdc_received,
                SUM(fee / 1e6) as fees
            FROM read_parquet('{_trades_glob()}')
            GROUP BY LOWER(maker)
        """)

        # Step 2: Taker-side stats
        logger.info("Step 2/3: Aggregating taker-side stats...")
        conn.execute(f"""
            CREATE OR REPLACE TEMP TABLE taker_stats AS
            SELECT
                LOWER(taker) as wallet,
                COUNT(*) as trade_count,
                SUM(CASE WHEN taker_asset_id = '0' THEN taker_amount / 1e6 ELSE 0 END) as usdc_spent,
                SUM(CASE WHEN maker_asset_id = '0' THEN maker_amount / 1e6 ELSE 0 END) as usdc_received,
                0.0 as fees
            FROM read_parquet('{_trades_glob()}')
            GROUP BY LOWER(taker)
        """)

        # Step 3: Merge maker + taker stats
        logger.info("Step 3/3: Merging results...")
        df = conn.sql("""
            SELECT
                wallet,
                SUM(trade_count) as total_trades,
                ROUND(SUM(usdc_spent), 2) as total_bought_usdc,
                ROUND(SUM(usdc_received), 2) as total_sold_usdc,
                ROUND(SUM(fees), 2) as total_fees_usdc,
                ROUND(SUM(usdc_received) - SUM(usdc_spent), 2) as net_flow_usdc
            FROM (
                SELECT * FROM maker_stats
                UNION ALL
                SELECT * FROM taker_stats
            )
            GROUP BY wallet
            HAVING SUM(trade_count) >= 10
            ORDER BY SUM(trade_count) DESC
        """).fetchdf()

        # Clean up temp tables
        conn.execute("DROP TABLE IF EXISTS maker_stats")
        conn.execute("DROP TABLE IF EXISTS taker_stats")

        # Build dict (skip win rate for now — too expensive for 382M trades)
        stats = {}
        for _, row in df.iterrows():
            addr = row["wallet"]
            stats[addr] = {
                "total_trades": int(row["total_trades"] or 0),
                "total_bought_usdc": float(row["total_bought_usdc"] or 0),
                "total_sold_usdc": float(row["total_sold_usdc"] or 0),
                "total_fees_usdc": float(row["total_fees_usdc"] or 0),
                "net_flow_usdc": float(row["net_flow_usdc"] or 0),
                "unique_markets": 0,
                "round_trips": 0,
                "estimated_win_rate": 0.0,
            }

        # Save to file
        WALLET_STATS_FILE.parent.mkdir(parents=True, exist_ok=True)
        WALLET_STATS_FILE.write_text(json.dumps(stats))

        # Update in-memory cache
        _set_cache("wallet_stats_file", stats)

        duration = _time.time() - start
        logger.info(f"Wallet stats rebuilt: {len(stats)} wallets in {duration:.1f}s")
        return len(stats)

    except Exception as e:
        logger.error(f"Error rebuilding wallet stats: {e}")
        return 0


def batch_enrich_wallets(addresses: list[str]) -> dict[str, dict]:
    """
    Enrich multiple wallets using pre-aggregated stats (instant).

    Falls back to direct DuckDB query if stats file doesn't exist.
    """
    if not addresses:
        return {}

    # Try pre-aggregated stats first (instant)
    all_stats = _load_wallet_stats()
    if all_stats:
        result = {}
        for addr in addresses:
            data = all_stats.get(addr.lower())
            if data:
                result[addr.lower()] = data
        return result

    # Fallback: direct query (slow, first time only)
    return _batch_enrich_wallets_direct(addresses)


def _batch_enrich_wallets_direct(addresses: list[str]) -> dict[str, dict]:
    """Direct DuckDB batch query (used as fallback before stats file exists)."""
    conn = _get_conn()
    if not conn or not _has_data():
        return {}

    cache_key = "batch_enrich_direct"
    cached = _cached(cache_key)
    if cached is not None:
        # Filter to requested addresses
        return {a.lower(): cached[a.lower()] for a in addresses if a.lower() in cached}

    try:
        addr_list = ", ".join(f"'{a.lower()}'" for a in addresses)

        df = conn.sql(f"""
            WITH relevant AS (
                SELECT *
                FROM read_parquet('{_trades_glob()}')
                WHERE LOWER(maker) IN ({addr_list})
                   OR LOWER(taker) IN ({addr_list})
            ),
            per_wallet AS (
                SELECT
                    wallet,
                    COUNT(*) as total_trades,
                    SUM(usdc_spent) as total_bought,
                    SUM(usdc_received) as total_sold,
                    SUM(fee_usdc) as total_fees,
                    COUNT(DISTINCT market_id) as unique_markets
                FROM (
                    SELECT
                        LOWER(maker) as wallet,
                        CASE WHEN maker_asset_id = '0' THEN maker_amount / 1e6 ELSE 0 END as usdc_spent,
                        CASE WHEN taker_asset_id = '0' THEN taker_amount / 1e6 ELSE 0 END as usdc_received,
                        fee / 1e6 as fee_usdc,
                        CASE WHEN maker_asset_id != '0' THEN maker_asset_id ELSE taker_asset_id END as market_id
                    FROM relevant
                    WHERE LOWER(maker) IN ({addr_list})

                    UNION ALL

                    SELECT
                        LOWER(taker) as wallet,
                        CASE WHEN taker_asset_id = '0' THEN taker_amount / 1e6 ELSE 0 END as usdc_spent,
                        CASE WHEN maker_asset_id = '0' THEN maker_amount / 1e6 ELSE 0 END as usdc_received,
                        0 as fee_usdc,
                        CASE WHEN taker_asset_id != '0' THEN taker_asset_id ELSE maker_asset_id END as market_id
                    FROM relevant
                    WHERE LOWER(taker) IN ({addr_list})
                )
                GROUP BY wallet
            )
            SELECT * FROM per_wallet
        """).fetchdf()

        result = {}
        for _, row in df.iterrows():
            addr = row["wallet"]
            bought = float(row["total_bought"] or 0)
            sold = float(row["total_sold"] or 0)
            fees = float(row["total_fees"] or 0)
            result[addr] = {
                "total_trades": int(row["total_trades"] or 0),
                "total_bought_usdc": round(bought, 2),
                "total_sold_usdc": round(sold, 2),
                "total_fees_usdc": round(fees, 2),
                "net_flow_usdc": round(sold - bought, 2),
                "unique_markets": int(row["unique_markets"] or 0),
            }

        _set_cache(cache_key, result)
        return result

    except Exception as e:
        logger.error(f"DuckDB error (batch_enrich_wallets_direct): {e}")
        return {}


def batch_wallet_win_rates(addresses: list[str]) -> dict[str, dict]:
    """
    Batch win rate using pre-aggregated stats (instant).
    """
    if not addresses:
        return {}

    all_stats = _load_wallet_stats()
    if all_stats:
        result = {}
        for addr in addresses:
            data = all_stats.get(addr.lower())
            if data and data.get("round_trips", 0) > 0:
                result[addr.lower()] = {
                    "estimated_win_rate": data["estimated_win_rate"],
                    "total_round_trips": data["round_trips"],
                    "total_trades": data["total_trades"],
                }
        return result

    # Fallback: no stats file yet
    return {}


# -----------------------------------------------------------------------
# Individual wallet queries (kept for single-wallet detail views)
# -----------------------------------------------------------------------

def get_wallet_trade_count(address: str) -> int:
    """Count total trades for a wallet address."""
    # Try pre-aggregated first
    stats = _load_wallet_stats()
    if stats:
        data = stats.get(address.lower())
        if data:
            return data.get("total_trades", 0)

    conn = _get_conn()
    if not conn or not _has_data():
        return 0

    try:
        result = conn.sql(f"""
            SELECT COUNT(*) as cnt
            FROM read_parquet('{_trades_glob()}')
            WHERE LOWER(maker) = LOWER('{address}')
               OR LOWER(taker) = LOWER('{address}')
        """).fetchone()
        return result[0] if result else 0
    except Exception as e:
        logger.error(f"DuckDB error (wallet_trade_count): {e}")
        return 0


def get_wallet_full_history(address: str, limit: int = 1000) -> list[dict]:
    """Get full trade history for a wallet address."""
    conn = _get_conn()
    if not conn or not _has_data():
        return []

    try:
        df = conn.sql(f"""
            SELECT
                block_number,
                transaction_hash,
                CASE WHEN LOWER(maker) = LOWER('{address}') THEN 'MAKER' ELSE 'TAKER' END as role,
                maker_asset_id,
                taker_asset_id,
                maker_amount,
                taker_amount,
                fee,
                contract
            FROM read_parquet('{_trades_glob()}')
            WHERE LOWER(maker) = LOWER('{address}')
               OR LOWER(taker) = LOWER('{address}')
            ORDER BY block_number DESC
            LIMIT {limit}
        """).fetchdf()
        return df.to_dict("records")
    except Exception as e:
        logger.error(f"DuckDB error (wallet_full_history): {e}")
        return []


def get_wallet_pnl_estimate(address: str) -> dict:
    """Estimate wallet PnL (uses pre-aggregated stats if available)."""
    result = batch_enrich_wallets([address])
    return result.get(address.lower(), {})


def get_market_trade_flow(asset_id: str, hours: int = 24) -> dict:
    """Get maker/taker flow for a specific market."""
    conn = _get_conn()
    if not conn or not _has_data():
        return {}

    try:
        result = conn.sql(f"""
            SELECT
                COUNT(*) as total_trades,
                SUM(CASE WHEN maker_asset_id = '0' THEN maker_amount / 1e6 ELSE 0 END) as buy_volume,
                SUM(CASE WHEN taker_asset_id = '0' THEN taker_amount / 1e6 ELSE 0 END) as sell_volume,
                COUNT(DISTINCT maker) as unique_makers,
                COUNT(DISTINCT taker) as unique_takers
            FROM read_parquet('{_trades_glob()}')
            WHERE maker_asset_id = '{asset_id}' OR taker_asset_id = '{asset_id}'
        """).fetchone()

        if not result:
            return {}

        buy_vol = result[1] or 0
        sell_vol = result[2] or 0

        return {
            "total_trades": result[0] or 0,
            "buy_volume_usdc": round(buy_vol, 2),
            "sell_volume_usdc": round(sell_vol, 2),
            "net_flow_usdc": round(buy_vol - sell_vol, 2),
            "unique_makers": result[3] or 0,
            "unique_takers": result[4] or 0,
        }
    except Exception as e:
        logger.error(f"DuckDB error (market_trade_flow): {e}")
        return {}


def get_top_wallets_by_volume(limit: int = 100) -> list[dict]:
    """Find top wallets by total trading volume."""
    cache_key = f"top_wallets_{limit}"
    cached = _cached(cache_key)
    if cached is not None:
        return cached

    # Try pre-aggregated stats (instant)
    stats = _load_wallet_stats()
    if stats:
        sorted_wallets = sorted(
            stats.items(),
            key=lambda x: x[1].get("total_bought_usdc", 0),
            reverse=True,
        )[:limit]
        result = [
            {
                "address": addr,
                "total_volume_usdc": d.get("total_bought_usdc", 0),
                "trade_count": d.get("total_trades", 0),
                "maker_pct": 0,  # not tracked in summary
            }
            for addr, d in sorted_wallets
        ]
        _set_cache(cache_key, result)
        return result

    conn = _get_conn()
    if not conn or not _has_data():
        return []

    try:
        df = conn.sql(f"""
            WITH wallet_stats AS (
                SELECT
                    address,
                    SUM(volume_usdc) as total_volume,
                    SUM(trade_count) as total_trades,
                    SUM(CASE WHEN role = 'MAKER' THEN trade_count ELSE 0 END) as maker_trades
                FROM (
                    SELECT
                        LOWER(maker) as address,
                        'MAKER' as role,
                        COUNT(*) as trade_count,
                        SUM(CASE WHEN maker_asset_id = '0' THEN maker_amount / 1e6
                                 ELSE taker_amount / 1e6 END) as volume_usdc
                    FROM read_parquet('{_trades_glob()}')
                    GROUP BY LOWER(maker)

                    UNION ALL

                    SELECT
                        LOWER(taker) as address,
                        'TAKER' as role,
                        COUNT(*) as trade_count,
                        SUM(CASE WHEN taker_asset_id = '0' THEN taker_amount / 1e6
                                 ELSE maker_amount / 1e6 END) as volume_usdc
                    FROM read_parquet('{_trades_glob()}')
                    GROUP BY LOWER(taker)
                )
                GROUP BY address
            )
            SELECT
                address,
                ROUND(total_volume, 2) as total_volume_usdc,
                total_trades as trade_count,
                ROUND(maker_trades * 100.0 / NULLIF(total_trades, 0), 1) as maker_pct
            FROM wallet_stats
            ORDER BY total_volume DESC
            LIMIT {limit}
        """).fetchdf()
        result = df.to_dict("records")
        _set_cache(cache_key, result)
        return result
    except Exception as e:
        logger.error(f"DuckDB error (top_wallets_by_volume): {e}")
        return []


def get_wallet_win_rate_historical(address: str) -> dict:
    """Estimate historical win rate (uses pre-aggregated stats if available)."""
    result = batch_wallet_win_rates([address])
    return result.get(address.lower(), {})


def get_data_summary() -> dict:
    """Get a summary of available blockchain data."""
    cached = _cached("data_summary")
    if cached is not None:
        return cached

    conn = _get_conn()
    if not conn or not _has_data():
        return {"available": False}

    try:
        result = conn.sql(f"""
            SELECT
                COUNT(*) as total_trades,
                MIN(block_number) as min_block,
                MAX(block_number) as max_block,
                COUNT(DISTINCT maker) as unique_makers,
                COUNT(DISTINCT taker) as unique_takers
            FROM read_parquet('{_trades_glob()}')
        """).fetchone()

        if not result:
            return {"available": False}

        data = {
            "available": True,
            "total_trades": result[0] or 0,
            "min_block": result[1],
            "max_block": result[2],
            "unique_makers": result[3] or 0,
            "unique_takers": result[4] or 0,
        }
        _set_cache("data_summary", data)
        return data
    except Exception as e:
        logger.error(f"DuckDB error (data_summary): {e}")
        return {"available": False}
