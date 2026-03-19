"""
Blockchain Indexer — Fetches Polymarket trades directly from the Polygon
blockchain and stores them as Parquet files for historical analysis.

Uses the prediction-market-analysis repo's indexer pattern:
- Reads OrderFilled events from CTF Exchange contracts on Polygon
- Stores trades in chunked Parquet files
- Cursor-based resume for incremental updates

Requires: POLYGON_RPC env var (e.g. Alchemy or QuickNode endpoint)
"""

import logging
import os
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

# Polymarket CTF Exchange contracts on Polygon
CTF_EXCHANGE = "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E"
NEGRISK_CTF_EXCHANGE = "0xC5d563A36AE78145C45a50134d48A1215220f80a"

# OrderFilled event topic hash
ORDER_FILLED_TOPIC = "0xd0a08e8c493f9c94f29311604c9de1b4e8c8d4c06bd0c789af57f2d65bfec0f6"

# Polymarket CTF Exchange deployed at this block
POLYMARKET_START_BLOCK = 33605403

# Default data directory
DATA_DIR = Path("data/blockchain/trades")
CURSOR_FILE = Path("data/blockchain/.block_cursor")

# OrderFilled ABI for event decoding
ORDER_FILLED_ABI = {
    "anonymous": False,
    "inputs": [
        {"indexed": True, "name": "orderHash", "type": "bytes32"},
        {"indexed": True, "name": "maker", "type": "address"},
        {"indexed": True, "name": "taker", "type": "address"},
        {"indexed": False, "name": "makerAssetId", "type": "uint256"},
        {"indexed": False, "name": "takerAssetId", "type": "uint256"},
        {"indexed": False, "name": "makerAmountFilled", "type": "uint256"},
        {"indexed": False, "name": "takerAmountFilled", "type": "uint256"},
        {"indexed": False, "name": "fee", "type": "uint256"},
    ],
    "name": "OrderFilled",
    "type": "event",
}

BATCH_SIZE = 10_000  # trades per Parquet file
CHUNK_SIZE = 2000    # blocks per RPC query (dRPC supports up to 3000)


@dataclass
class BlockchainTrade:
    """A single trade decoded from the Polygon blockchain."""
    block_number: int
    transaction_hash: str
    log_index: int
    order_hash: str
    maker: str
    taker: str
    maker_asset_id: str  # stored as string (uint256 too large)
    taker_asset_id: str
    maker_amount: int     # 6 decimals (USDC)
    taker_amount: int
    fee: int
    contract: str         # "CTF" or "NegRisk"
    fetched_at: str


def get_polygon_rpc() -> str:
    """Get Polygon RPC URL from environment."""
    rpc = os.getenv("POLYGON_RPC", "")
    if not rpc:
        raise ValueError(
            "POLYGON_RPC env var not set. "
            "Get a free endpoint at https://www.alchemy.com/ or https://www.quicknode.com/"
        )
    return rpc


def get_data_stats() -> dict:
    """Get stats about the collected blockchain data."""
    if not DATA_DIR.exists():
        return {"total_files": 0, "cursor_block": None, "data_dir": str(DATA_DIR)}

    parquet_files = list(DATA_DIR.glob("trades_*.parquet"))
    cursor_block = None
    if CURSOR_FILE.exists():
        try:
            cursor_block = int(CURSOR_FILE.read_text().strip())
        except (ValueError, TypeError):
            pass

    return {
        "total_files": len(parquet_files),
        "cursor_block": cursor_block,
        "data_dir": str(DATA_DIR),
    }


def run_incremental(max_chunks: int = 50000) -> dict:
    """
    Run an incremental blockchain index — fetch only new blocks since last cursor.

    Args:
        max_chunks: Maximum number of block-chunks to process (prevents runaway).

    Returns:
        Dict with stats: trades_fetched, blocks_processed, from_block, to_block
    """
    try:
        from web3 import Web3
        from web3.middleware import ExtraDataToPOAMiddleware
        import pandas as pd
    except ImportError as e:
        logger.error(f"Missing dependency for blockchain indexer: {e}")
        return {"ok": False, "error": f"Missing dependency: {e}"}

    rpc_url = get_polygon_rpc()
    w3 = Web3(Web3.HTTPProvider(rpc_url, request_kwargs={"timeout": 30}))
    w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)

    if not w3.is_connected():
        logger.error("Cannot connect to Polygon RPC")
        return {"ok": False, "error": "Polygon RPC connection failed"}

    # Setup directories
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    CURSOR_FILE.parent.mkdir(parents=True, exist_ok=True)

    # Determine start block (from cursor or start)
    current_block = w3.eth.block_number
    from_block = POLYMARKET_START_BLOCK

    if CURSOR_FILE.exists():
        try:
            from_block = int(CURSOR_FILE.read_text().strip())
            logger.info(f"Resuming from block {from_block}")
        except (ValueError, TypeError):
            pass

    to_block = current_block
    total_blocks = to_block - from_block

    if total_blocks <= 0:
        logger.info("Blockchain index already up to date")
        return {"ok": True, "trades_fetched": 0, "blocks_processed": 0,
                "from_block": from_block, "to_block": to_block}

    logger.info(f"Blockchain indexer: blocks {from_block} → {to_block} ({total_blocks:,} blocks)")

    # Create contract instances for decoding
    ctf = w3.eth.contract(
        address=Web3.to_checksum_address(CTF_EXCHANGE), abi=[ORDER_FILLED_ABI]
    )
    negrisk = w3.eth.contract(
        address=Web3.to_checksum_address(NEGRISK_CTF_EXCHANGE), abi=[ORDER_FILLED_ABI]
    )

    contracts = [
        ("CTF", CTF_EXCHANGE, ctf),
        ("NegRisk", NEGRISK_CTF_EXCHANGE, negrisk),
    ]

    all_trades = []
    total_saved = 0
    chunks_processed = 0
    block = from_block

    try:
        while block <= to_block and chunks_processed < max_chunks:
            chunk_end = min(block + CHUNK_SIZE - 1, to_block)
            fetched_at = datetime.now(timezone.utc).isoformat()

            for name, address, contract in contracts:
                logs = None
                for attempt in range(5):
                    try:
                        logs = w3.eth.get_logs({
                            "address": Web3.to_checksum_address(address),
                            "topics": [ORDER_FILLED_TOPIC],
                            "fromBlock": block,
                            "toBlock": chunk_end,
                        })
                        break
                    except Exception as e:
                        err_str = str(e).lower()
                        if "too large" in err_str:
                            logger.warning(f"Block range too large {block}-{chunk_end}, skipping")
                            break
                        if "429" in str(e) or "rate" in err_str or "limit" in err_str:
                            wait = 2 ** (attempt + 1)  # 2, 4, 8, 16, 32 sec
                            logger.warning(f"Rate limited (attempt {attempt+1}/5), waiting {wait}s")
                            time.sleep(wait)
                            continue
                        logger.error(f"RPC error fetching {block}-{chunk_end}: {e}")
                        time.sleep(2)
                        break

                if logs is None:
                    continue

                for log in logs:
                    try:
                        decoded = contract.events.OrderFilled().process_log(log)
                        args = decoded["args"]
                        trade = BlockchainTrade(
                            block_number=log["blockNumber"],
                            transaction_hash=log["transactionHash"].hex(),
                            log_index=log["logIndex"],
                            order_hash=args["orderHash"].hex(),
                            maker=args["maker"],
                            taker=args["taker"],
                            maker_asset_id=str(args["makerAssetId"]),
                            taker_asset_id=str(args["takerAssetId"]),
                            maker_amount=args["makerAmountFilled"],
                            taker_amount=args["takerAmountFilled"],
                            fee=args["fee"],
                            contract=name,
                            fetched_at=fetched_at,
                        )
                        all_trades.append(asdict(trade))
                    except Exception as e:
                        logger.debug(f"Error decoding log: {e}")

            # Save in batches
            while len(all_trades) >= BATCH_SIZE:
                batch = all_trades[:BATCH_SIZE]
                _save_parquet_batch(pd, batch)
                total_saved += len(batch)
                all_trades = all_trades[BATCH_SIZE:]

            # Update cursor
            CURSOR_FILE.write_text(str(chunk_end))
            block = chunk_end + 1
            chunks_processed += 1

            # Progress logging every 1000 chunks
            if chunks_processed % 1000 == 0 and chunks_processed > 0:
                pct = (block - from_block) / max(total_blocks, 1) * 100
                logger.info(
                    f"Progress: {pct:.1f}% | block {block:,} | "
                    f"{chunks_processed} chunks | {total_saved + len(all_trades)} trades"
                )

            # Rate limiting
            time.sleep(0.1)

    except KeyboardInterrupt:
        logger.info("Blockchain indexer interrupted, progress saved")
    except Exception as e:
        logger.error(f"Blockchain indexer error: {e}")

    # Save remaining trades
    if all_trades:
        try:
            _save_parquet_batch(pd, all_trades)
            total_saved += len(all_trades)
        except Exception as e:
            logger.error(f"Error saving final batch: {e}")

    logger.info(
        f"Blockchain indexer done: {total_saved} trades saved, "
        f"{chunks_processed} chunks processed"
    )

    # Rebuild pre-aggregated wallet stats after indexing
    if total_saved > 0:
        try:
            from services.historical_analytics import rebuild_wallet_stats
            n = rebuild_wallet_stats()
            logger.info(f"Wallet stats rebuilt: {n} wallets")
        except Exception as e:
            logger.warning(f"Failed to rebuild wallet stats: {e}")

    return {
        "ok": True,
        "trades_fetched": total_saved,
        "blocks_processed": chunks_processed * CHUNK_SIZE,
        "from_block": from_block,
        "to_block": block - 1,
    }


def _save_parquet_batch(pd, trades: list[dict]) -> None:
    """Save a batch of trades as a Parquet file."""
    if not trades:
        return

    # Find next chunk index
    existing = list(DATA_DIR.glob("trades_*.parquet"))
    if existing:
        indices = []
        for f in existing:
            parts = f.stem.split("_")
            if len(parts) >= 2:
                try:
                    indices.append(int(parts[1]))
                except ValueError:
                    pass
        chunk_idx = max(indices) + BATCH_SIZE if indices else 0
    else:
        chunk_idx = 0

    chunk_path = DATA_DIR / f"trades_{chunk_idx}_{chunk_idx + len(trades)}.parquet"
    df = pd.DataFrame(trades)
    df.to_parquet(chunk_path, index=False)
    logger.info(f"Saved {len(trades)} trades → {chunk_path.name}")
