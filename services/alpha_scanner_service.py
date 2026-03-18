"""
Alpha Scanner Service — Scans Polymarket leaderboard wallets, enriches them
with positions/activity/profile data, calculates Alpha and Radar scores,
and provides filtering with presets. Supports custom preset persistence
and a copy-trading watchlist.
"""

import json
import os
import time
import logging
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass, field, asdict
from typing import Optional, Callable

from services.data_api_client import DataAPIClient

logger = logging.getLogger(__name__)

# Categories to scan across
SCAN_CATEGORIES = ["OVERALL", "POLITICS", "SPORTS", "CRYPTO", "CULTURE"]
SCAN_TIME_PERIODS = ["WEEK", "MONTH"]
SCAN_ORDER_BY = ["PNL", "VOL"]

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data")
PRESETS_FILE = os.path.join(DATA_DIR, "alpha_scanner_presets.json")
WATCHLIST_FILE = os.path.join(DATA_DIR, "alpha_scanner_watchlist.json")
COPYTRADES_FILE = os.path.join(DATA_DIR, "alpha_scanner_copytrades.json")
COPYTRADING_CONFIG_FILE = os.path.join(DATA_DIR, "alpha_scanner_copytrading_config.json")


# -----------------------------------------------------------------------
# Data classes
# -----------------------------------------------------------------------

@dataclass
class WalletData:
    address: str
    username: str = ""
    pseudonym: str = ""
    verified: bool = False
    wallet_age_days: int = 0
    pnl_7d: float = 0.0
    pnl_30d: float = 0.0
    volume: float = 0.0
    roi_7d: float = 0.0
    category: str = "OVERALL"
    active_positions: int = 0
    trades_per_day: float = 0.0
    win_rate: float = 0.0
    consistency_days: int = 0
    alpha_score: float = 0.0
    radar_score: float = 0.0
    profile_url: str = ""


@dataclass
class ScanResult:
    timestamp: datetime = field(default_factory=datetime.now)
    wallets: list[WalletData] = field(default_factory=list)
    total_scanned: int = 0
    total_passed: int = 0
    scan_duration_seconds: float = 0.0


@dataclass
class FilterConfig:
    min_pnl_7d: float = 0.0
    max_volume: float = 999999999.0
    min_trades_day: float = 1.0
    max_trades_day: float = 50.0
    max_active_pos: int = 150
    min_roi_7d: float = 0.0
    min_pnl_30d: float = 0.0
    min_volume: float = 0.0
    min_wallet_age: int = 0
    min_consistency: int = 1
    min_win_rate: float = 0.0
    verified: str = "any"  # "any", "verified", "unverified"


# -----------------------------------------------------------------------
# Built-in filter presets
# -----------------------------------------------------------------------

BUILTIN_PRESETS: dict[str, FilterConfig] = {
    "Standard": FilterConfig(),
    "Under the Radar": FilterConfig(
        max_volume=100000.0, min_pnl_7d=1000.0, min_trades_day=1.0,
        max_trades_day=20.0, min_consistency=3, min_win_rate=30.0,
    ),
    "Consistent Winners": FilterConfig(
        min_trades_day=2.0, max_trades_day=30.0, min_wallet_age=30,
        min_consistency=5, min_win_rate=40.0,
    ),
    "High Roller": FilterConfig(
        min_pnl_7d=10000.0, max_active_pos=50, min_pnl_30d=5000.0,
        min_volume=50000.0,
    ),
    "New Alpha": FilterConfig(
        max_volume=500000.0, min_pnl_7d=500.0, min_roi_7d=20.0,
        min_wallet_age=0, min_consistency=3,
    ),
}


# -----------------------------------------------------------------------
# Custom preset persistence
# -----------------------------------------------------------------------

def load_all_presets() -> dict[str, FilterConfig]:
    """Load built-in + custom presets."""
    presets = dict(BUILTIN_PRESETS)
    try:
        if os.path.exists(PRESETS_FILE):
            with open(PRESETS_FILE) as f:
                custom = json.load(f)
            for name, cfg in custom.items():
                presets[name] = FilterConfig(**cfg)
    except Exception as e:
        logger.warning(f"Failed to load custom presets: {e}")
    return presets


def save_custom_preset(name: str, config: FilterConfig) -> None:
    """Save a custom preset to disk."""
    try:
        custom = {}
        if os.path.exists(PRESETS_FILE):
            with open(PRESETS_FILE) as f:
                custom = json.load(f)
        custom[name] = asdict(config)
        os.makedirs(os.path.dirname(PRESETS_FILE), exist_ok=True)
        with open(PRESETS_FILE, "w") as f:
            json.dump(custom, f, indent=2)
    except Exception as e:
        logger.error(f"Failed to save preset '{name}': {e}")


def delete_custom_preset(name: str) -> None:
    """Delete a custom preset."""
    try:
        if not os.path.exists(PRESETS_FILE):
            return
        with open(PRESETS_FILE) as f:
            custom = json.load(f)
        custom.pop(name, None)
        with open(PRESETS_FILE, "w") as f:
            json.dump(custom, f, indent=2)
    except Exception as e:
        logger.error(f"Failed to delete preset '{name}': {e}")


# -----------------------------------------------------------------------
# Copy-trading watchlist persistence
# -----------------------------------------------------------------------

def load_watchlist() -> list[dict]:
    """Load watchlist from disk. Each entry: {address, username, added_at, note}."""
    try:
        if os.path.exists(WATCHLIST_FILE):
            with open(WATCHLIST_FILE) as f:
                return json.load(f)
    except Exception as e:
        logger.warning(f"Failed to load watchlist: {e}")
    return []


def save_watchlist(watchlist: list[dict]) -> None:
    """Save watchlist to disk."""
    try:
        os.makedirs(os.path.dirname(WATCHLIST_FILE), exist_ok=True)
        with open(WATCHLIST_FILE, "w") as f:
            json.dump(watchlist, f, indent=2)
    except Exception as e:
        logger.error(f"Failed to save watchlist: {e}")


def add_to_watchlist(address: str, username: str, note: str = "") -> None:
    """Add a wallet to the copy-trading watchlist."""
    wl = load_watchlist()
    # Don't add duplicates
    if any(w["address"] == address for w in wl):
        return
    wl.append({
        "address": address,
        "username": username,
        "added_at": datetime.now().isoformat(),
        "note": note,
    })
    save_watchlist(wl)


def remove_from_watchlist(address: str) -> None:
    """Remove a wallet from the watchlist."""
    wl = load_watchlist()
    wl = [w for w in wl if w["address"] != address]
    save_watchlist(wl)


# -----------------------------------------------------------------------
# Copy-trade market selections (per wallet, per market)
# -----------------------------------------------------------------------

def load_copy_trades() -> list[dict]:
    """Load copy trade selections.

    Each entry: {wallet_address, wallet_name, market_title, condition_id,
                 outcome, size, added_at}
    """
    try:
        if os.path.exists(COPYTRADES_FILE):
            with open(COPYTRADES_FILE) as f:
                return json.load(f)
    except Exception as e:
        logger.warning(f"Failed to load copy trades: {e}")
    return []


def save_copy_trades(trades: list[dict]) -> None:
    """Save copy trades to disk."""
    try:
        os.makedirs(os.path.dirname(COPYTRADES_FILE), exist_ok=True)
        with open(COPYTRADES_FILE, "w") as f:
            json.dump(trades, f, indent=2)
    except Exception as e:
        logger.error(f"Failed to save copy trades: {e}")


def add_copy_trade(
    wallet_address: str,
    wallet_name: str,
    market_title: str,
    condition_id: str,
    outcome: str,
    size: float = 0.0,
) -> None:
    """Add a specific market position to copy trades."""
    ct = load_copy_trades()
    # No duplicates (same wallet + condition_id)
    if any(
        t["wallet_address"] == wallet_address and t["condition_id"] == condition_id
        for t in ct
    ):
        return
    ct.append({
        "wallet_address": wallet_address,
        "wallet_name": wallet_name,
        "market_title": market_title,
        "condition_id": condition_id,
        "outcome": outcome,
        "size": size,
        "added_at": datetime.now().isoformat(),
    })
    save_copy_trades(ct)


def remove_copy_trade(wallet_address: str, condition_id: str) -> None:
    """Remove a specific market from copy trades."""
    ct = load_copy_trades()
    ct = [
        t for t in ct
        if not (t["wallet_address"] == wallet_address and t["condition_id"] == condition_id)
    ]
    save_copy_trades(ct)


def remove_all_copy_trades_for_wallet(wallet_address: str) -> None:
    """Remove all copy trades for a wallet."""
    ct = load_copy_trades()
    ct = [t for t in ct if t["wallet_address"] != wallet_address]
    save_copy_trades(ct)


# -----------------------------------------------------------------------
# Copy-trading configuration
# -----------------------------------------------------------------------

DEFAULT_COPYTRADING_CONFIG = {
    "enabled": False,
    "amount_per_trade": 1.0,
    "max_daily_trades": 10,
    "max_daily_amount": 20.0,
    "poll_interval_minutes": 5,
    "mode": "paper",  # "paper" or "live"
    "min_position_size": 0.5,  # only copy if wallet position >= $X
}


def load_copytrading_config() -> dict:
    """Load copy-trading configuration."""
    config = dict(DEFAULT_COPYTRADING_CONFIG)
    try:
        if os.path.exists(COPYTRADING_CONFIG_FILE):
            with open(COPYTRADING_CONFIG_FILE) as f:
                saved = json.load(f)
            config.update(saved)
    except Exception as e:
        logger.warning(f"Failed to load copytrading config: {e}")
    return config


def save_copytrading_config(config: dict) -> None:
    """Save copy-trading configuration."""
    try:
        os.makedirs(os.path.dirname(COPYTRADING_CONFIG_FILE), exist_ok=True)
        with open(COPYTRADING_CONFIG_FILE, "w") as f:
            json.dump(config, f, indent=2)
    except Exception as e:
        logger.error(f"Failed to save copytrading config: {e}")


# -----------------------------------------------------------------------
# Normalization helper
# -----------------------------------------------------------------------

def _normalize(value: float, min_val: float, max_val: float) -> float:
    if max_val <= min_val:
        return 0.0
    return max(0.0, min(1.0, (value - min_val) / (max_val - min_val)))


def _parse_timestamp(ts_str: str) -> Optional[datetime]:
    """Parse ISO timestamp with various fractional second formats."""
    try:
        clean = ts_str.replace("Z", "+00:00")
        if "." in clean:
            parts = clean.split(".")
            frac_and_tz = parts[1]
            for i, c in enumerate(frac_and_tz):
                if c in "+-":
                    frac = frac_and_tz[:i][:6]
                    tz = frac_and_tz[i:]
                    clean = f"{parts[0]}.{frac}{tz}"
                    break
        return datetime.fromisoformat(clean)
    except (ValueError, TypeError):
        return None


# -----------------------------------------------------------------------
# Service
# -----------------------------------------------------------------------

class AlphaScannerService:

    def __init__(self, client: Optional[DataAPIClient] = None):
        self.client = client or DataAPIClient(timeout=30)

    def scan_wallets(
        self,
        max_wallets: int = 300,
        progress_callback: Optional[Callable[[int, int, str], None]] = None,
    ) -> ScanResult:
        """Run a full scan: collect leaderboard wallets, enrich, score."""
        start_time = time.time()

        # Phase 1: Collect wallet addresses from leaderboard
        if progress_callback:
            progress_callback(0, 100, "Leaderboard abrufen...")

        raw_wallets = self._collect_leaderboard(max_wallets, progress_callback)
        unique_addresses = list(raw_wallets.keys())[:max_wallets]

        if progress_callback:
            progress_callback(20, 100, f"{len(unique_addresses)} Wallets gefunden. Anreichern...")

        # Phase 2: Load pre-aggregated blockchain stats (instant from JSON file)
        blockchain_pnl = {}
        blockchain_wr = {}
        try:
            from services.historical_analytics import (
                batch_enrich_wallets,
                batch_wallet_win_rates,
            )
            if progress_callback:
                progress_callback(22, 100, "Blockchain-Daten laden...")
            blockchain_pnl = batch_enrich_wallets(unique_addresses)
            blockchain_wr = batch_wallet_win_rates(unique_addresses)
            if blockchain_pnl:
                logger.info(f"Blockchain enrichment: {len(blockchain_pnl)} wallets loaded")
        except ImportError:
            pass
        except Exception as e:
            logger.warning(f"Blockchain enrichment failed: {e}")

        # Enrich each wallet (API data + pre-loaded blockchain data)
        enriched: list[WalletData] = []
        total = len(unique_addresses)
        for i, addr in enumerate(unique_addresses):
            lb_data = raw_wallets[addr]
            try:
                wallet = self._enrich_wallet(
                    addr, lb_data,
                    bc_pnl=blockchain_pnl.get(addr.lower()),
                    bc_wr=blockchain_wr.get(addr.lower()),
                )
                enriched.append(wallet)
            except Exception as e:
                logger.debug(f"Enrichment failed for {addr[:12]}: {e}")

            if progress_callback and (i % 5 == 0 or i == total - 1):
                pct = 25 + int(65 * (i + 1) / total)
                progress_callback(pct, 100, f"Wallet {i+1}/{total} anreichern...")

            time.sleep(0.2)

        # Phase 3: Calculate scores
        if progress_callback:
            progress_callback(92, 100, "Scores berechnen...")

        self._calculate_scores(enriched)

        # Sort by alpha_score descending
        enriched.sort(key=lambda w: w.alpha_score, reverse=True)

        duration = time.time() - start_time
        if progress_callback:
            progress_callback(100, 100, f"Fertig! {len(enriched)} Wallets in {duration:.0f}s")

        return ScanResult(
            timestamp=datetime.now(),
            wallets=enriched,
            total_scanned=len(unique_addresses),
            total_passed=len(enriched),
            scan_duration_seconds=round(duration, 1),
        )

    # ------------------------------------------------------------------
    # Phase 1: Collect wallets from leaderboard (merge WEEK + MONTH)
    # ------------------------------------------------------------------

    def _collect_leaderboard(
        self,
        max_wallets: int,
        progress_callback: Optional[Callable] = None,
    ) -> dict[str, dict]:
        """Fetch wallets from leaderboard across categories/periods.

        Merges WEEK and MONTH data per wallet so both pnl_7d and pnl_30d are filled.
        """
        wallets: dict[str, dict] = {}
        step = 0
        total_steps = len(SCAN_CATEGORIES) * len(SCAN_TIME_PERIODS) * len(SCAN_ORDER_BY)

        for category in SCAN_CATEGORIES:
            for period in SCAN_TIME_PERIODS:
                for order in SCAN_ORDER_BY:
                    for offset in range(0, 200, 50):
                        entries = self.client.get_leaderboard(
                            category=category,
                            time_period=period,
                            order_by=order,
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
                            vol = float(e.get("vol", 0) or 0)

                            if addr not in wallets:
                                wallets[addr] = {
                                    "username": e.get("userName", ""),
                                    "verified": bool(e.get("verifiedBadge")),
                                    "category": category,
                                    "pnl_7d": 0.0,
                                    "pnl_30d": 0.0,
                                    "vol_7d": 0.0,
                                    "vol_30d": 0.0,
                                }

                            # Merge data from different periods
                            w = wallets[addr]
                            if period == "WEEK":
                                if pnl > w["pnl_7d"]:
                                    w["pnl_7d"] = pnl
                                if vol > w["vol_7d"]:
                                    w["vol_7d"] = vol
                            elif period == "MONTH":
                                if pnl > w["pnl_30d"]:
                                    w["pnl_30d"] = pnl
                                if vol > w["vol_30d"]:
                                    w["vol_30d"] = vol

                        if len(entries) < 50:
                            break

                        time.sleep(0.15)

                    step += 1
                    if progress_callback:
                        pct = int(18 * step / total_steps)
                        progress_callback(pct, 100, f"Leaderboard: {category} {period} {order}...")

                    if len(wallets) >= max_wallets:
                        return wallets

                    time.sleep(0.15)

        return wallets

    # ------------------------------------------------------------------
    # Phase 2: Enrich a single wallet
    # ------------------------------------------------------------------

    def _enrich_wallet(self, address: str, lb_data: dict,
                       bc_pnl: dict | None = None,
                       bc_wr: dict | None = None) -> WalletData:
        """Fetch profile, positions, activity and build WalletData."""
        wallet = WalletData(
            address=address,
            username=lb_data.get("username", ""),
            verified=lb_data.get("verified", False),
            category=lb_data.get("category", "OVERALL"),
            profile_url=f"https://polymarket.com/profile/{address}",
        )

        # Leaderboard data (merged from WEEK + MONTH)
        wallet.pnl_7d = lb_data.get("pnl_7d", 0)
        wallet.pnl_30d = lb_data.get("pnl_30d", 0)
        vol_7d = lb_data.get("vol_7d", 0)
        vol_30d = lb_data.get("vol_30d", 0)
        wallet.volume = max(vol_7d, vol_30d)
        wallet.roi_7d = (wallet.pnl_7d / vol_7d * 100) if vol_7d > 0 else 0.0

        # Profile (Gamma API)
        profile = self.client.get_user_profile(address)
        if profile:
            wallet.pseudonym = profile.get("pseudonym", "") or ""
            wallet.username = wallet.username or profile.get("name", "") or wallet.pseudonym
            wallet.verified = wallet.verified or bool(profile.get("verifiedBadge"))
            created = profile.get("createdAt")
            if created:
                dt = _parse_timestamp(created)
                if dt:
                    wallet.wallet_age_days = (datetime.now(timezone.utc) - dt).days

        # Positions
        positions = self.client.get_user_positions(address, limit=100)
        if positions:
            active = [p for p in positions if float(p.get("size", 0) or 0) > 0]
            wallet.active_positions = len(active)
            # Win rate: all positions with cashPnl data
            with_pnl = [p for p in positions if p.get("cashPnl") is not None]
            if with_pnl:
                wins = sum(1 for p in with_pnl if float(p.get("cashPnl", 0) or 0) > 0)
                wallet.win_rate = round(wins / len(with_pnl) * 100, 1)

        # Activity (last 7 days)
        seven_days_ago = int((datetime.now(timezone.utc) - timedelta(days=7)).timestamp())
        activities = self.client.get_user_activity(address, start=seven_days_ago, limit=200)
        if activities:
            wallet.trades_per_day = round(len(activities) / 7, 2)
            trade_days: set[str] = set()
            for a in activities:
                ts = a.get("timestamp")
                if ts:
                    try:
                        day = datetime.fromtimestamp(int(ts), tz=timezone.utc).strftime("%Y-%m-%d")
                        trade_days.add(day)
                    except (ValueError, TypeError, OSError):
                        pass
            wallet.consistency_days = len(trade_days)

        # Historical blockchain data enrichment (pre-loaded via batch query)
        if bc_pnl and bc_pnl.get("total_trades", 0) > 10:
            hist_vol = bc_pnl.get("total_bought_usdc", 0)
            if hist_vol > wallet.volume:
                wallet.volume = hist_vol

            net_flow = bc_pnl.get("net_flow_usdc", 0)
            if abs(net_flow) > abs(wallet.pnl_30d):
                wallet.pnl_30d = net_flow

            total_trades = bc_pnl.get("total_trades", 0)
            if total_trades > 0 and wallet.wallet_age_days > 0:
                wallet.trades_per_day = round(
                    total_trades / max(wallet.wallet_age_days, 1), 2
                )

        if bc_wr and bc_wr.get("total_round_trips", 0) > 5:
            wallet.win_rate = bc_wr["estimated_win_rate"]

        return wallet

    # ------------------------------------------------------------------
    # Phase 3: Calculate scores
    # ------------------------------------------------------------------

    def _calculate_scores(self, wallets: list[WalletData]) -> None:
        """Calculate alpha_score and radar_score for all wallets."""
        if not wallets:
            return

        max_pnl7 = max((w.pnl_7d for w in wallets), default=1) or 1
        max_roi = max((w.roi_7d for w in wallets), default=1) or 1
        max_pnl30 = max((w.pnl_30d for w in wallets), default=1) or 1
        max_vol = max((w.volume for w in wallets), default=1) or 1
        max_pos = max((w.active_positions for w in wallets), default=1) or 1

        for w in wallets:
            # Alpha Score: weighted composite
            components = [
                0.25 * _normalize(w.roi_7d, 0, max_roi),
                0.20 * _normalize(w.pnl_7d, 0, max_pnl7),
                0.15 * _normalize(w.pnl_30d, 0, max_pnl30),
                0.15 * (1 - _normalize(w.volume, 0, max_vol)),  # less volume = under the radar
                0.10 * (1 - _normalize(w.active_positions, 0, max_pos)),
                0.15 * (w.consistency_days / 7),
            ]
            w.alpha_score = round(max(0.0, min(1.0, sum(components))), 2)

            # Radar Score: low volume = under the radar
            w.radar_score = round(
                max(0.0, min(1.0, 1 - _normalize(w.volume, 0, max_vol))),
                2,
            )

    # ------------------------------------------------------------------
    # Filtering
    # ------------------------------------------------------------------

    @staticmethod
    def filter_wallets(wallets: list[WalletData], config: FilterConfig) -> list[WalletData]:
        """Apply filter config to wallet list."""
        result = []
        for w in wallets:
            if w.pnl_7d < config.min_pnl_7d:
                continue
            if w.volume > config.max_volume:
                continue
            if w.trades_per_day < config.min_trades_day:
                continue
            if w.trades_per_day > config.max_trades_day:
                continue
            if w.active_positions > config.max_active_pos:
                continue
            if w.roi_7d < config.min_roi_7d:
                continue
            if w.pnl_30d < config.min_pnl_30d:
                continue
            if w.volume < config.min_volume:
                continue
            if w.wallet_age_days < config.min_wallet_age:
                continue
            if w.consistency_days < config.min_consistency:
                continue
            if w.win_rate < config.min_win_rate:
                continue
            if config.verified == "verified" and not w.verified:
                continue
            if config.verified == "unverified" and w.verified:
                continue
            result.append(w)
        return result
