"""
Alpha Scanner Service — Scans Polymarket leaderboard wallets, enriches them
with positions/activity/profile data, calculates Alpha and Radar scores,
and provides filtering with presets.
"""

import time
import logging
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass, field
from typing import Optional, Callable

from services.data_api_client import DataAPIClient

logger = logging.getLogger(__name__)

# Categories to scan across
SCAN_CATEGORIES = ["OVERALL", "POLITICS", "SPORTS", "CRYPTO", "CULTURE"]
SCAN_TIME_PERIODS = ["WEEK", "MONTH"]
SCAN_ORDER_BY = ["PNL", "VOL"]


# -----------------------------------------------------------------------
# Data classes
# -----------------------------------------------------------------------

@dataclass
class WalletData:
    address: str
    username: str = ""
    pseudonym: str = ""
    views: int = 0
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
    max_views: int = 20000
    min_trades_day: float = 1.0
    max_trades_day: float = 50.0
    max_active_pos: int = 150
    min_roi_7d: float = 0.0
    min_pnl_30d: float = 0.0
    min_volume: float = 0.0
    min_wallet_age: int = 7
    min_consistency: int = 3
    min_win_rate: float = 50.0
    verified: str = "any"  # "any", "verified", "unverified"
    categories: list[str] = field(default_factory=lambda: ["OVERALL"])


# -----------------------------------------------------------------------
# Filter presets
# -----------------------------------------------------------------------

FILTER_PRESETS: dict[str, FilterConfig] = {
    "Standard": FilterConfig(),
    "Under the Radar": FilterConfig(
        max_views=100, min_pnl_7d=1000.0, min_trades_day=1.0,
        max_trades_day=20.0, min_consistency=3, min_win_rate=50.0,
    ),
    "Consistent Winners": FilterConfig(
        min_trades_day=2.0, max_trades_day=30.0, min_wallet_age=30,
        min_consistency=6, min_win_rate=60.0,
    ),
    "High Roller": FilterConfig(
        min_pnl_7d=10000.0, max_active_pos=50, min_pnl_30d=5000.0,
        min_volume=50000.0,
    ),
    "New Alpha": FilterConfig(
        max_views=500, min_pnl_7d=500.0, min_roi_7d=20.0,
        min_wallet_age=0, min_consistency=3,
    ),
}


# -----------------------------------------------------------------------
# Normalization helper
# -----------------------------------------------------------------------

def _normalize(value: float, min_val: float, max_val: float) -> float:
    if max_val <= min_val:
        return 0.0
    return max(0.0, min(1.0, (value - min_val) / (max_val - min_val)))


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

        # Phase 2: Enrich each wallet
        enriched: list[WalletData] = []
        total = len(unique_addresses)
        for i, addr in enumerate(unique_addresses):
            lb_data = raw_wallets[addr]
            try:
                wallet = self._enrich_wallet(addr, lb_data)
                enriched.append(wallet)
            except Exception as e:
                logger.debug(f"Enrichment failed for {addr[:12]}: {e}")

            if progress_callback and (i % 5 == 0 or i == total - 1):
                pct = 20 + int(70 * (i + 1) / total)
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
    # Phase 1: Collect wallets from leaderboard
    # ------------------------------------------------------------------

    def _collect_leaderboard(
        self,
        max_wallets: int,
        progress_callback: Optional[Callable] = None,
    ) -> dict[str, dict]:
        """Fetch wallets from leaderboard across categories/periods. Dedup by address."""
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
                            if addr and addr not in wallets:
                                wallets[addr] = {
                                    "username": e.get("userName", ""),
                                    "pnl": float(e.get("pnl", 0) or 0),
                                    "vol": float(e.get("vol", 0) or 0),
                                    "verified": bool(e.get("verifiedBadge")),
                                    "category": category,
                                    "period": period,
                                }

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

    def _enrich_wallet(self, address: str, lb_data: dict) -> WalletData:
        """Fetch profile, positions, activity and build WalletData."""
        wallet = WalletData(
            address=address,
            username=lb_data.get("username", ""),
            verified=lb_data.get("verified", False),
            category=lb_data.get("category", "OVERALL"),
            profile_url=f"https://polymarket.com/profile/{address}",
        )

        # Leaderboard data
        pnl = lb_data.get("pnl", 0)
        vol = lb_data.get("vol", 0)
        period = lb_data.get("period", "WEEK")
        if period == "WEEK":
            wallet.pnl_7d = pnl
            wallet.volume = vol
            wallet.roi_7d = (pnl / vol * 100) if vol > 0 else 0.0
        elif period == "MONTH":
            wallet.pnl_30d = pnl
            wallet.volume = max(wallet.volume, vol)

        # Profile (Gamma API)
        profile = self.client.get_user_profile(address)
        if profile:
            wallet.pseudonym = profile.get("pseudonym", "") or ""
            wallet.username = wallet.username or profile.get("name", "") or wallet.pseudonym
            wallet.verified = wallet.verified or bool(profile.get("verifiedBadge"))
            created = profile.get("createdAt")
            if created:
                try:
                    dt = datetime.fromisoformat(created.replace("Z", "+00:00"))
                    wallet.wallet_age_days = (datetime.now(timezone.utc) - dt).days
                except (ValueError, TypeError):
                    pass

        # Positions
        positions = self.client.get_user_positions(address, limit=100)
        if positions:
            active = [p for p in positions if float(p.get("size", 0) or 0) > 0]
            wallet.active_positions = len(active)
            closed = [p for p in positions if float(p.get("size", 0) or 0) == 0 and p.get("cashPnl") is not None]
            if closed:
                wins = sum(1 for p in closed if float(p.get("cashPnl", 0) or 0) > 0)
                wallet.win_rate = round(wins / len(closed) * 100, 1)

        # Activity (last 7 days)
        seven_days_ago = int((datetime.now(timezone.utc) - timedelta(days=7)).timestamp())
        activities = self.client.get_user_activity(address, start=seven_days_ago, limit=200)
        if activities:
            wallet.trades_per_day = round(len(activities) / 7, 2)
            # Consistency: unique days with trades
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

        return wallet

    # ------------------------------------------------------------------
    # Phase 3: Calculate scores
    # ------------------------------------------------------------------

    def _calculate_scores(self, wallets: list[WalletData]) -> None:
        """Calculate alpha_score and radar_score for all wallets."""
        if not wallets:
            return

        # Collect ranges for normalization
        pnls_7d = [w.pnl_7d for w in wallets]
        rois = [w.roi_7d for w in wallets]
        pnls_30d = [w.pnl_30d for w in wallets]
        views_list = [w.views for w in wallets]
        pos_list = [w.active_positions for w in wallets]

        max_pnl7 = max(pnls_7d) if pnls_7d else 1
        max_roi = max(rois) if rois else 1
        max_pnl30 = max(pnls_30d) if pnls_30d else 1
        max_views = max(views_list) if views_list else 1
        max_pos = max(pos_list) if pos_list else 1

        for w in wallets:
            # Alpha Score
            components = [
                0.25 * _normalize(w.roi_7d, 0, max(max_roi, 1)),
                0.20 * _normalize(w.pnl_7d, 0, max(max_pnl7, 1)),
                0.15 * _normalize(w.pnl_30d, 0, max(max_pnl30, 1)),
                0.15 * (1 - _normalize(w.views, 0, max(max_views, 1))),
                0.10 * (1 - _normalize(w.active_positions, 0, max(max_pos, 1))),
                0.15 * (w.consistency_days / 7),
            ]
            w.alpha_score = round(max(0.0, min(1.0, sum(components))), 2)

            # Radar Score: low visibility + high skill
            w.radar_score = round(
                max(0.0, min(1.0, 1 - _normalize(w.views, 0, max(max_views, 1)))),
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
            if w.views > config.max_views:
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
