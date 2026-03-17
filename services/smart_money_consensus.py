"""
Smart Money Consensus — Aggregates positions of top-performing wallets
and finds markets where multiple high-alpha traders agree on the same side.

When 3+ top wallets hold the same position, that's a strong consensus signal.
The edge is calculated as: consensus_strength * avg_pnl_weight - market_price.
"""

import logging
import time
from datetime import datetime, timezone
from dataclasses import dataclass, field

from services.data_api_client import DataAPIClient

logger = logging.getLogger(__name__)

# Minimum number of top wallets agreeing for a consensus signal
MIN_CONSENSUS_WALLETS = 3
# Only consider wallets with positive 7d PnL
MIN_PNL_7D = 100.0
# Maximum wallets to scan for positions
MAX_WALLETS_TO_SCAN = 50
# Leaderboard categories to sample from
CONSENSUS_CATEGORIES = ["OVERALL", "CRYPTO", "POLITICS", "SPORTS"]


@dataclass
class ConsensusSignal:
    """A market where multiple top wallets agree."""
    market_id: str  # condition_id
    market_title: str
    consensus_side: str  # "YES" or "NO"
    wallet_count: int  # how many top wallets hold this side
    total_wallets_checked: int
    avg_wallet_pnl: float  # average 7d PnL of agreeing wallets
    avg_wallet_win_rate: float
    total_position_size: float  # combined $ size of all positions
    avg_entry_price: float
    current_price: float
    consensus_score: float  # 0-1, strength of consensus
    edge: float  # signed edge
    abs_edge: float
    wallet_names: list[str] = field(default_factory=list)


def scan_smart_money_consensus(
    max_wallets: int = MAX_WALLETS_TO_SCAN,
    min_consensus: int = MIN_CONSENSUS_WALLETS,
) -> list[ConsensusSignal]:
    """
    Scan top wallets from leaderboard and find markets with consensus.

    1. Fetch top wallets from leaderboard (by PnL, WEEK)
    2. For each wallet, fetch active positions
    3. Aggregate: which markets have 3+ top wallets on the same side?
    4. Calculate consensus score and edge

    Returns list of ConsensusSignal sorted by consensus_score descending.
    """
    client = DataAPIClient(timeout=20)

    try:
        # Phase 1: Collect top wallets
        wallets = _collect_top_wallets(client, max_wallets)
        if not wallets:
            logger.warning("Smart money consensus: no wallets found")
            return []

        logger.info(f"Smart money consensus: scanning {len(wallets)} wallets")

        # Phase 2: Fetch positions for each wallet
        market_positions: dict[str, list[dict]] = {}
        for i, wallet in enumerate(wallets):
            positions = client.get_user_positions(wallet["address"], limit=50)
            if not positions:
                time.sleep(0.15)
                continue

            for pos in positions:
                size = float(pos.get("size", 0) or 0)
                if size <= 0:
                    continue

                cond_id = pos.get("conditionId", pos.get("condition_id", ""))
                if not cond_id:
                    continue

                if cond_id not in market_positions:
                    market_positions[cond_id] = []

                market_positions[cond_id].append({
                    "wallet_address": wallet["address"],
                    "wallet_name": wallet.get("username", wallet["address"][:12]),
                    "wallet_pnl_7d": wallet.get("pnl_7d", 0),
                    "wallet_win_rate": wallet.get("win_rate", 0),
                    "outcome": pos.get("outcome", "Yes"),
                    "size": size,
                    "avg_price": float(pos.get("avgPrice", 0) or 0),
                    "cur_price": float(pos.get("curPrice", 0) or 0),
                    "cash_pnl": float(pos.get("cashPnl", 0) or 0),
                    "title": pos.get("title", ""),
                })

            time.sleep(0.2)

        # Phase 3: Find consensus
        signals = _find_consensus(market_positions, len(wallets), min_consensus)

        logger.info(
            f"Smart money consensus: {len(market_positions)} markets scanned, "
            f"{len(signals)} consensus signals found"
        )

        return signals

    finally:
        client.close()


def _collect_top_wallets(client: DataAPIClient, max_wallets: int) -> list[dict]:
    """Fetch top wallets from leaderboard across categories."""
    seen: dict[str, dict] = {}

    for category in CONSENSUS_CATEGORIES:
        for offset in range(0, 100, 50):
            entries = client.get_leaderboard(
                category=category,
                time_period="WEEK",
                order_by="PNL",
                limit=50,
                offset=offset,
            )
            if not entries:
                break

            for e in entries:
                addr = e.get("proxyWallet")
                if not addr:
                    continue
                pnl = float(e.get("pnl", 0) or 0)
                if pnl < MIN_PNL_7D:
                    continue

                if addr not in seen:
                    seen[addr] = {
                        "address": addr,
                        "username": e.get("userName", ""),
                        "pnl_7d": pnl,
                        "win_rate": 0,
                        "category": category,
                    }
                elif pnl > seen[addr]["pnl_7d"]:
                    seen[addr]["pnl_7d"] = pnl

            if len(entries) < 50:
                break
            time.sleep(0.15)

        if len(seen) >= max_wallets:
            break
        time.sleep(0.15)

    # Sort by PnL descending, take top N
    sorted_wallets = sorted(seen.values(), key=lambda w: w["pnl_7d"], reverse=True)
    return sorted_wallets[:max_wallets]


def _find_consensus(
    market_positions: dict[str, list[dict]],
    total_wallets: int,
    min_consensus: int,
) -> list[ConsensusSignal]:
    """Find markets where multiple wallets agree on the same side."""
    signals = []

    for cond_id, positions in market_positions.items():
        if len(positions) < min_consensus:
            continue

        # Count sides
        yes_positions = [p for p in positions if _is_yes_side(p["outcome"])]
        no_positions = [p for p in positions if not _is_yes_side(p["outcome"])]

        # Pick dominant side
        if len(yes_positions) >= len(no_positions):
            dominant = yes_positions
            side = "YES"
        else:
            dominant = no_positions
            side = "NO"

        if len(dominant) < min_consensus:
            continue

        # Calculate metrics
        avg_pnl = sum(p["wallet_pnl_7d"] for p in dominant) / len(dominant)
        avg_wr = sum(p["wallet_win_rate"] for p in dominant) / len(dominant)
        total_size = sum(p["size"] for p in dominant)
        avg_entry = (
            sum(p["avg_price"] * p["size"] for p in dominant) / total_size
            if total_size > 0 else 0
        )
        cur_price = dominant[0]["cur_price"] if dominant else 0
        title = dominant[0]["title"] if dominant else ""
        names = [p["wallet_name"] for p in dominant]

        # Consensus score: combination of wallet count, PnL quality, and position size
        wallet_ratio = len(dominant) / max(total_wallets, 1)
        pnl_factor = min(avg_pnl / 5000, 1.0)  # normalize to $5k
        consensus_score = round(
            0.50 * min(len(dominant) / 10, 1.0)  # more wallets = stronger (cap at 10)
            + 0.30 * pnl_factor                    # higher avg PnL = stronger
            + 0.20 * wallet_ratio,                 # higher % of scanned wallets = stronger
            3,
        )

        # Edge: consensus fair probability vs market price
        # If many smart wallets hold YES at avg entry < current → they expect YES
        # Fair prob estimate: weighted by wallet quality
        fair_prob = _estimate_fair_probability(dominant, side, cur_price)

        if side == "YES":
            edge = fair_prob - cur_price
        else:
            edge = (1 - fair_prob) - (1 - cur_price)

        signals.append(ConsensusSignal(
            market_id=cond_id,
            market_title=title,
            consensus_side=side,
            wallet_count=len(dominant),
            total_wallets_checked=total_wallets,
            avg_wallet_pnl=round(avg_pnl, 2),
            avg_wallet_win_rate=round(avg_wr, 1),
            total_position_size=round(total_size, 2),
            avg_entry_price=round(avg_entry, 4),
            current_price=round(cur_price, 4),
            consensus_score=consensus_score,
            edge=round(edge, 4),
            abs_edge=round(abs(edge), 4),
            wallet_names=names,
        ))

    # Sort by consensus_score descending
    signals.sort(key=lambda s: s.consensus_score, reverse=True)
    return signals


def _is_yes_side(outcome: str) -> bool:
    """Check if an outcome string represents YES side."""
    return outcome.lower() in ("yes", "y", "true", "1")


def _estimate_fair_probability(
    positions: list[dict], side: str, current_price: float
) -> float:
    """
    Estimate fair probability based on smart money positions.

    Logic: If top traders bought at avg_entry and price has moved to current,
    and they're still holding (not sold), they believe fair value is ABOVE current.
    Weight by wallet PnL quality.
    """
    if not positions:
        return current_price

    # Weighted average of entry prices (weighted by wallet PnL)
    total_weight = 0
    weighted_fair = 0

    for p in positions:
        # Weight by PnL (more profitable wallets have more weight)
        weight = max(p["wallet_pnl_7d"], 100) / 100
        entry = p["avg_price"]
        size = p["size"]

        # If they entered at `entry` and still hold, they expect the market
        # to settle favorably. Their "implied fair" is higher than entry.
        # Conservative estimate: midpoint between entry and 1.0 (settlement)
        if side == "YES":
            implied_fair = min(entry + 0.15, 0.95)  # they expect YES, so fair > entry
        else:
            implied_fair = max(entry - 0.15, 0.05)

        weighted_fair += implied_fair * weight * size
        total_weight += weight * size

    if total_weight <= 0:
        return current_price

    fair = weighted_fair / total_weight
    return max(0.01, min(0.99, fair))
