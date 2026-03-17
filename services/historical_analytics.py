"""
Historical Analytics — DuckDB queries on Parquet blockchain trade data.

Provides deep wallet analysis and market flow data using the full
historical trade dataset collected by the blockchain indexer.

All queries run on Parquet files via DuckDB — no separate database needed.
"""

import logging
from datetime import datetime, timezone
from pathlib import Path
from functools import lru_cache

logger = logging.getLogger(__name__)

TRADES_DIR = Path("data/blockchain/trades")


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


def _trades_glob() -> str:
    """Get the glob pattern for all trade Parquet files."""
    return str(TRADES_DIR / "trades_*.parquet")


def get_wallet_trade_count(address: str) -> int:
    """Count total trades for a wallet address (as maker or taker)."""
    duckdb = _get_duckdb()
    if not duckdb or not _has_data():
        return 0

    try:
        result = duckdb.sql(f"""
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
    """
    Get full trade history for a wallet address.

    Returns list of trades with: block_number, transaction_hash, side,
    maker_amount, taker_amount, fee, contract, maker_asset_id, taker_asset_id.
    """
    duckdb = _get_duckdb()
    if not duckdb or not _has_data():
        return []

    try:
        df = duckdb.sql(f"""
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
    """
    Estimate wallet PnL from historical blockchain trades.

    Calculates total USDC spent (as buyer) vs received (as seller),
    plus fees paid. This is an approximation since we don't track
    settlement payouts on-chain here.

    Returns: {total_bought_usdc, total_sold_usdc, total_fees, net_flow,
              total_trades, unique_markets}
    """
    duckdb = _get_duckdb()
    if not duckdb or not _has_data():
        return {}

    try:
        result = duckdb.sql(f"""
            SELECT
                COUNT(*) as total_trades,

                -- When wallet is maker and maker_asset_id = '0' → paying USDC (buying)
                SUM(CASE
                    WHEN LOWER(maker) = LOWER('{address}') AND maker_asset_id = '0'
                    THEN maker_amount / 1e6
                    ELSE 0
                END) as usdc_spent_as_maker,

                -- When wallet is taker and taker_asset_id = '0' → paying USDC (buying)
                SUM(CASE
                    WHEN LOWER(taker) = LOWER('{address}') AND taker_asset_id = '0'
                    THEN taker_amount / 1e6
                    ELSE 0
                END) as usdc_spent_as_taker,

                -- When wallet is maker and taker_asset_id = '0' → receiving USDC (selling)
                SUM(CASE
                    WHEN LOWER(maker) = LOWER('{address}') AND taker_asset_id = '0'
                    THEN taker_amount / 1e6
                    ELSE 0
                END) as usdc_received_as_maker,

                -- When wallet is taker and maker_asset_id = '0' → receiving USDC (selling)
                SUM(CASE
                    WHEN LOWER(taker) = LOWER('{address}') AND maker_asset_id = '0'
                    THEN maker_amount / 1e6
                    ELSE 0
                END) as usdc_received_as_taker,

                SUM(fee / 1e6) as total_fees,

                COUNT(DISTINCT
                    CASE WHEN maker_asset_id != '0' THEN maker_asset_id
                         ELSE taker_asset_id END
                ) as unique_markets

            FROM read_parquet('{_trades_glob()}')
            WHERE LOWER(maker) = LOWER('{address}')
               OR LOWER(taker) = LOWER('{address}')
        """).fetchone()

        if not result:
            return {}

        total_spent = (result[1] or 0) + (result[2] or 0)
        total_received = (result[3] or 0) + (result[4] or 0)
        total_fees = result[5] or 0

        return {
            "total_trades": result[0] or 0,
            "total_bought_usdc": round(total_spent, 2),
            "total_sold_usdc": round(total_received, 2),
            "total_fees_usdc": round(total_fees, 2),
            "net_flow_usdc": round(total_received - total_spent, 2),
            "unique_markets": result[6] or 0,
        }
    except Exception as e:
        logger.error(f"DuckDB error (wallet_pnl_estimate): {e}")
        return {}


def get_market_trade_flow(asset_id: str, hours: int = 24) -> dict:
    """
    Get maker/taker flow for a specific market (by asset_id / token_id).

    Returns: {total_trades, buy_volume_usdc, sell_volume_usdc, net_flow,
              unique_makers, unique_takers}
    """
    duckdb = _get_duckdb()
    if not duckdb or not _has_data():
        return {}

    try:
        result = duckdb.sql(f"""
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
    """
    Find top wallets by total trading volume (USDC).

    Returns list of: {address, total_volume_usdc, trade_count, as_maker_pct}
    """
    duckdb = _get_duckdb()
    if not duckdb or not _has_data():
        return []

    try:
        df = duckdb.sql(f"""
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
        return df.to_dict("records")
    except Exception as e:
        logger.error(f"DuckDB error (top_wallets_by_volume): {e}")
        return []


def get_wallet_win_rate_historical(address: str) -> dict:
    """
    Estimate historical win rate for a wallet based on trade patterns.

    Analyzes buy/sell patterns: if a wallet bought and later sold at higher
    price (more USDC received per token), that's a win.

    Returns: {estimated_win_rate, total_round_trips, avg_hold_blocks}
    """
    duckdb = _get_duckdb()
    if not duckdb or not _has_data():
        return {}

    try:
        # Get all trades for this wallet grouped by asset
        df = duckdb.sql(f"""
            SELECT
                CASE WHEN maker_asset_id != '0' THEN maker_asset_id ELSE taker_asset_id END as asset_id,
                block_number,
                CASE
                    WHEN (LOWER(maker) = LOWER('{address}') AND maker_asset_id = '0')
                      OR (LOWER(taker) = LOWER('{address}') AND taker_asset_id = '0')
                    THEN 'BUY'
                    ELSE 'SELL'
                END as action,
                CASE
                    WHEN maker_asset_id = '0' THEN maker_amount * 1.0 / NULLIF(taker_amount, 0)
                    ELSE taker_amount * 1.0 / NULLIF(maker_amount, 0)
                END as price_per_token
            FROM read_parquet('{_trades_glob()}')
            WHERE LOWER(maker) = LOWER('{address}')
               OR LOWER(taker) = LOWER('{address}')
            ORDER BY asset_id, block_number
        """).fetchdf()

        if df.empty:
            return {}

        # Simple win rate: for each asset, compare avg buy price vs avg sell price
        wins = 0
        total = 0
        for asset_id, group in df.groupby("asset_id"):
            buys = group[group["action"] == "BUY"]
            sells = group[group["action"] == "SELL"]
            if buys.empty or sells.empty:
                continue

            avg_buy = buys["price_per_token"].mean()
            avg_sell = sells["price_per_token"].mean()
            if avg_buy > 0:
                total += 1
                if avg_sell > avg_buy:
                    wins += 1

        win_rate = (wins / total * 100) if total > 0 else 0

        return {
            "estimated_win_rate": round(win_rate, 1),
            "total_round_trips": total,
            "total_trades": len(df),
        }
    except Exception as e:
        logger.error(f"DuckDB error (wallet_win_rate): {e}")
        return {}


def get_data_summary() -> dict:
    """Get a summary of available blockchain data."""
    duckdb = _get_duckdb()
    if not duckdb or not _has_data():
        return {"available": False}

    try:
        result = duckdb.sql(f"""
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

        return {
            "available": True,
            "total_trades": result[0] or 0,
            "min_block": result[1],
            "max_block": result[2],
            "unique_makers": result[3] or 0,
            "unique_takers": result[4] or 0,
        }
    except Exception as e:
        logger.error(f"DuckDB error (data_summary): {e}")
        return {"available": False}
