"""
Build Resolution Table — Downloads all resolved Polymarket markets from the
CLOB API and stores them as Parquet files for strategy backtesting.

Creates two files:
  - data/blockchain/resolutions.parquet: condition_id → winning outcome
  - data/blockchain/token_to_market.parquet: token_id → condition_id + outcome

Usage:
  python scripts/build_resolution_table.py          # full download
  python scripts/build_resolution_table.py --update  # incremental (new only)
"""

import argparse
import json
import logging
import sys
import time
from pathlib import Path

import httpx
import pyarrow as pa
import pyarrow.parquet as pq

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

CLOB_API = "https://clob.polymarket.com"
DATA_DIR = Path("data/blockchain")

RESOLUTIONS_FILE = DATA_DIR / "resolutions.parquet"
TOKEN_MAP_FILE = DATA_DIR / "token_to_market.parquet"


def fetch_all_markets(existing_condition_ids: set[str] | None = None) -> tuple[list, list]:
    """Fetch all markets from CLOB API with cursor pagination.

    Returns (resolutions, token_map) lists of dicts.
    """
    resolutions = []
    token_map = []
    cursor = None
    total_fetched = 0
    resolved_count = 0

    client = httpx.Client(timeout=30, headers={"Accept": "application/json"})

    try:
        while True:
            params = {"limit": "500"}
            if cursor:
                # CLOB API returns "LTE=" (base64 of -1) as end-of-pagination
                if cursor in ("LTE=", ""):
                    logger.info("Reached end-of-pagination cursor, done.")
                    break
                params["next_cursor"] = cursor

            for attempt in range(5):
                try:
                    resp = client.get(f"{CLOB_API}/markets", params=params)
                    resp.raise_for_status()
                    break
                except (httpx.HTTPStatusError, httpx.ConnectError, httpx.ReadTimeout) as e:
                    wait = 2 ** (attempt + 1)
                    logger.warning(f"API error (attempt {attempt+1}/5): {e}, retrying in {wait}s")
                    time.sleep(wait)
            else:
                logger.error("Failed after 5 attempts, stopping")
                break

            data = resp.json()

            # Handle different response formats
            if isinstance(data, dict):
                markets = data.get("data", data.get("markets", []))
                cursor = data.get("next_cursor")
            elif isinstance(data, list):
                markets = data
                cursor = None
            else:
                logger.error(f"Unexpected response type: {type(data)}")
                break

            if not markets:
                logger.info("No more markets returned, done.")
                break

            for market in markets:
                condition_id = market.get("condition_id", "")
                if not condition_id:
                    continue

                # Skip already-known markets in incremental mode
                if existing_condition_ids and condition_id in existing_condition_ids:
                    continue

                tokens = market.get("tokens", [])
                if not tokens:
                    continue

                question = market.get("question", "")
                end_date = market.get("end_date_iso", "")
                description = market.get("description", "")

                # Determine if market has resolved (any token with winner=true)
                winning_token = None
                for token in tokens:
                    if token.get("winner"):
                        winning_token = token
                        break

                # Build token_to_market entries for ALL markets (resolved or not)
                for token in tokens:
                    token_map.append({
                        "token_id": token.get("token_id", ""),
                        "condition_id": condition_id,
                        "outcome": token.get("outcome", ""),
                        "is_winner": bool(token.get("winner")),
                    })

                # Only add to resolutions if market has resolved
                if winning_token:
                    resolutions.append({
                        "condition_id": condition_id,
                        "question": question[:500],  # truncate very long questions
                        "winning_token_id": winning_token.get("token_id", ""),
                        "winning_outcome": winning_token.get("outcome", ""),
                        "end_date": end_date,
                    })
                    resolved_count += 1

            total_fetched += len(markets)

            if total_fetched % 5000 < 500:
                logger.info(
                    f"Progress: {total_fetched:,} markets fetched, "
                    f"{resolved_count:,} resolved, "
                    f"{len(token_map):,} token mappings"
                )

            if not cursor:
                logger.info("No next_cursor, pagination complete.")
                break

            # Rate limiting
            time.sleep(0.2)

    finally:
        client.close()

    logger.info(
        f"Done: {total_fetched:,} total markets, "
        f"{resolved_count:,} resolved, "
        f"{len(token_map):,} token mappings"
    )
    return resolutions, token_map


def save_parquet(resolutions: list[dict], token_map: list[dict],
                 append: bool = False) -> None:
    """Save resolution and token map data as Parquet files."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    # If appending, merge with existing data
    if append:
        if RESOLUTIONS_FILE.exists():
            existing_res = pq.read_table(RESOLUTIONS_FILE).to_pylist()
            existing_ids = {r["condition_id"] for r in existing_res}
            new_res = [r for r in resolutions if r["condition_id"] not in existing_ids]
            resolutions = existing_res + new_res
            logger.info(f"Appending {len(new_res)} new resolutions to {len(existing_res)} existing")

        if TOKEN_MAP_FILE.exists():
            existing_tm = pq.read_table(TOKEN_MAP_FILE).to_pylist()
            existing_tokens = {t["token_id"] for t in existing_tm}
            new_tm = [t for t in token_map if t["token_id"] not in existing_tokens]
            token_map = existing_tm + new_tm
            logger.info(f"Appending {len(new_tm)} new token mappings to {len(existing_tm)} existing")

    # Save resolutions
    if resolutions:
        res_table = pa.table({
            "condition_id": [r["condition_id"] for r in resolutions],
            "question": [r["question"] for r in resolutions],
            "winning_token_id": [r["winning_token_id"] for r in resolutions],
            "winning_outcome": [r["winning_outcome"] for r in resolutions],
            "end_date": [r["end_date"] for r in resolutions],
        })
        pq.write_table(res_table, RESOLUTIONS_FILE)
        logger.info(f"Saved {len(resolutions):,} resolutions → {RESOLUTIONS_FILE}")

    # Save token map
    if token_map:
        tm_table = pa.table({
            "token_id": [t["token_id"] for t in token_map],
            "condition_id": [t["condition_id"] for t in token_map],
            "outcome": [t["outcome"] for t in token_map],
            "is_winner": [t["is_winner"] for t in token_map],
        })
        pq.write_table(tm_table, TOKEN_MAP_FILE)
        logger.info(f"Saved {len(token_map):,} token mappings → {TOKEN_MAP_FILE}")


def run_incremental() -> dict:
    """Run incremental update — only fetch new markets.

    Called by scheduler for daily updates.
    Returns stats dict.
    """
    existing_ids = set()
    if RESOLUTIONS_FILE.exists():
        try:
            existing = pq.read_table(RESOLUTIONS_FILE, columns=["condition_id"])
            existing_ids = set(existing.column("condition_id").to_pylist())
        except Exception:
            pass

    if TOKEN_MAP_FILE.exists():
        try:
            existing_tm = pq.read_table(TOKEN_MAP_FILE, columns=["condition_id"])
            existing_ids.update(existing_tm.column("condition_id").to_pylist())
        except Exception:
            pass

    logger.info(f"Incremental update: {len(existing_ids):,} existing markets known")

    resolutions, token_map = fetch_all_markets(existing_condition_ids=existing_ids)

    if resolutions or token_map:
        save_parquet(resolutions, token_map, append=True)

    return {
        "ok": True,
        "new_resolutions": len(resolutions),
        "new_token_mappings": len(token_map),
        "total_existing": len(existing_ids),
    }


def main():
    parser = argparse.ArgumentParser(description="Build Polymarket resolution table")
    parser.add_argument("--update", action="store_true",
                        help="Incremental update (only new markets)")
    args = parser.parse_args()

    if args.update:
        stats = run_incremental()
        logger.info(f"Incremental result: {json.dumps(stats)}")
    else:
        logger.info("Full download of all markets from CLOB API...")
        resolutions, token_map = fetch_all_markets()
        save_parquet(resolutions, token_map, append=False)

    # Print summary
    if RESOLUTIONS_FILE.exists():
        res = pq.read_table(RESOLUTIONS_FILE)
        logger.info(f"Resolutions table: {res.num_rows:,} rows")

    if TOKEN_MAP_FILE.exists():
        tm = pq.read_table(TOKEN_MAP_FILE)
        logger.info(f"Token map table: {tm.num_rows:,} rows")


if __name__ == "__main__":
    main()
