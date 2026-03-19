"""
Edge Sources — Compute calculated_edge for markets using free external data.

Sources:
1. Crypto: Binance public API (no auth) — compare crypto market prices vs spot
2. Cross-Platform: Manifold Markets API (free, no auth) — cross-platform probability comparison
3. Sports: ESPN/API-Football free endpoints — compare odds with market prices
4. Open-Meteo Ensemble: Probabilistic weather forecasts (multi-model)

Each source populates the `calculated_edge` field in the markets table.
The strategy evaluator then picks up markets where calculated_edge >= min_edge.
"""

import logging
import re
import math
from datetime import datetime, timezone
from typing import Optional

import httpx

logger = logging.getLogger(__name__)


# ============================================================
# 1. CRYPTO EDGE — Binance public API (no auth needed)
# ============================================================

# Map common crypto terms in market questions to Binance symbols
CRYPTO_PATTERNS = {
    "bitcoin": "BTCUSDT",
    "btc": "BTCUSDT",
    "ethereum": "ETHUSDT",
    "eth": "ETHUSDT",
    "solana": "SOLUSDT",
    "sol": "SOLUSDT",
    "xrp": "XRPUSDT",
    "ripple": "XRPUSDT",
    "dogecoin": "DOGEUSDT",
    "doge": "DOGEUSDT",
    "cardano": "ADAUSDT",
    "ada": "ADAUSDT",
    "polygon": "MATICUSDT",
    "matic": "MATICUSDT",
    "avalanche": "AVAXUSDT",
    "avax": "AVAXUSDT",
    "chainlink": "LINKUSDT",
    "link": "LINKUSDT",
}

# Threshold patterns: "Will BTC be above $100k?" etc.
THRESHOLD_RE = re.compile(
    r"(?:above|over|exceed|higher than|reach|hit|surpass|at least|more than)"
    r"\s*\$?([\d,]+\.?\d*)\s*([kKmM])?",
    re.IGNORECASE,
)
BELOW_RE = re.compile(
    r"(?:below|under|lower than|less than|drop|fall|at most)"
    r"\s*\$?([\d,]+\.?\d*)\s*([kKmM])?",
    re.IGNORECASE,
)


def compute_crypto_edges(engine) -> int:
    """Fetch Binance spot prices and compute edge for crypto markets.

    Returns number of markets updated.
    """
    # Find crypto markets
    crypto_markets = engine.query(
        "SELECT id, question, yes_price, no_price, volume FROM markets "
        "WHERE (category = 'Crypto' OR question LIKE '%Bitcoin%' OR question LIKE '%BTC%' "
        "  OR question LIKE '%Ethereum%' OR question LIKE '%ETH%' "
        "  OR question LIKE '%Solana%' OR question LIKE '%SOL%' "
        "  OR question LIKE '%XRP%' OR question LIKE '%Dogecoin%') "
        "AND accepting_orders = 1 AND yes_price > 0 AND yes_price < 1 "
        "ORDER BY volume DESC LIMIT 100"
    )

    if not crypto_markets:
        return 0

    # Fetch current prices from Binance (batch)
    prices = _fetch_binance_prices()
    if not prices:
        logger.warning("Crypto edge: failed to fetch Binance prices")
        return 0

    updated = 0
    for market in crypto_markets:
        try:
            edge = _compute_single_crypto_edge(market, prices)
            if edge is not None:
                engine.execute(
                    "UPDATE markets SET calculated_edge = ?, last_updated = datetime('now') WHERE id = ?",
                    (round(edge, 4), market["id"]),
                )
                updated += 1
        except Exception as e:
            logger.debug(f"Crypto edge error for {market['id'][:30]}: {e}")

    logger.info(f"Crypto edge: {updated}/{len(crypto_markets)} markets updated")
    return updated


def _fetch_binance_prices() -> dict:
    """Fetch current prices from Binance public API. Returns {symbol: price}."""
    try:
        resp = httpx.get(
            "https://api.binance.com/api/v3/ticker/price",
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()
        return {item["symbol"]: float(item["price"]) for item in data}
    except Exception as e:
        logger.error(f"Binance API error: {e}")
        return {}


def _compute_single_crypto_edge(market: dict, prices: dict) -> Optional[float]:
    """Compute edge for a single crypto market by comparing spot price to threshold."""
    question = market["question"]
    q_lower = question.lower()

    # Find which crypto this market is about
    symbol = None
    for keyword, sym in CRYPTO_PATTERNS.items():
        if keyword in q_lower:
            symbol = sym
            break

    if not symbol or symbol not in prices:
        return None

    current_price = prices[symbol]

    # Parse the threshold from the question
    above_match = THRESHOLD_RE.search(question)
    below_match = BELOW_RE.search(question)

    if above_match:
        threshold = _parse_number(above_match.group(1), above_match.group(2))
        if threshold <= 0:
            return None

        # "Will BTC be above $100k?" — if current price is $105k, high probability YES
        ratio = current_price / threshold

        if ratio > 1.05:
            # Already well above threshold — YES is likely
            fair_prob = min(0.95, 0.5 + (ratio - 1.0) * 2.0)
        elif ratio < 0.95:
            # Well below threshold — NO is likely
            fair_prob = max(0.05, 0.5 - (1.0 - ratio) * 2.0)
        else:
            # Close to threshold — uncertain
            fair_prob = 0.5 + (ratio - 1.0) * 1.5

        fair_prob = max(0.05, min(0.95, fair_prob))
        edge = fair_prob - market["yes_price"]
        return edge

    elif below_match:
        threshold = _parse_number(below_match.group(1), below_match.group(2))
        if threshold <= 0:
            return None

        ratio = current_price / threshold

        if ratio < 0.95:
            # Below threshold — YES is likely
            fair_prob = min(0.95, 0.5 + (1.0 - ratio) * 2.0)
        elif ratio > 1.05:
            # Above threshold — NO is likely
            fair_prob = max(0.05, 0.5 - (ratio - 1.0) * 2.0)
        else:
            fair_prob = 0.5 - (ratio - 1.0) * 1.5

        fair_prob = max(0.05, min(0.95, fair_prob))
        edge = fair_prob - market["yes_price"]
        return edge

    return None


def _parse_number(num_str: str, suffix: Optional[str]) -> float:
    """Parse number with optional k/m suffix."""
    val = float(num_str.replace(",", ""))
    if suffix:
        s = suffix.lower()
        if s == "k":
            val *= 1_000
        elif s == "m":
            val *= 1_000_000
    return val


# ============================================================
# 2. CROSS-PLATFORM EDGE — Manifold Markets (free, no auth)
# ============================================================

def compute_cross_platform_edges(engine) -> int:
    """Compare Polymarket prices with Manifold Markets probabilities.

    Returns number of markets updated.
    """
    # Get top markets from Polymarket DB
    poly_markets = engine.query(
        "SELECT id, question, yes_price, slug, category FROM markets "
        "WHERE accepting_orders = 1 AND yes_price > 0.05 AND yes_price < 0.95 "
        "AND volume > 10000 "
        "ORDER BY volume DESC LIMIT 50"
    )

    if not poly_markets:
        return 0

    updated = 0
    for market in poly_markets:
        try:
            edge = _compute_manifold_edge(market)
            if edge is not None and abs(edge) >= 0.03:
                engine.execute(
                    "UPDATE markets SET calculated_edge = ?, last_updated = datetime('now') WHERE id = ?",
                    (round(edge, 4), market["id"]),
                )
                updated += 1
        except Exception as e:
            logger.debug(f"Manifold edge error for {market['id'][:30]}: {e}")

    logger.info(f"Cross-platform edge: {updated}/{len(poly_markets)} markets updated via Manifold")
    return updated


def _compute_manifold_edge(market: dict) -> Optional[float]:
    """Search Manifold for a matching market and compare probabilities."""
    question = market["question"]

    # Search Manifold for similar markets
    try:
        resp = httpx.get(
            "https://api.manifold.markets/v0/search-markets",
            params={"term": question[:80], "limit": 3},
            timeout=10,
        )
        if resp.status_code != 200:
            return None

        results = resp.json()
        if not results:
            return None

        # Find best match — look for similar question text
        best_match = None
        best_similarity = 0

        for m in results:
            if m.get("isResolved", False):
                continue
            sim = _text_similarity(question.lower(), (m.get("question", "") or "").lower())
            if sim > best_similarity and sim > 0.3:
                best_similarity = sim
                best_match = m

        if not best_match:
            return None

        manifold_prob = best_match.get("probability")
        if manifold_prob is None:
            return None

        # Edge = Manifold probability - Polymarket price
        poly_price = market["yes_price"]
        edge = manifold_prob - poly_price

        logger.debug(
            f"Cross-platform: '{question[:50]}' | Poly={poly_price:.2f} Manifold={manifold_prob:.2f} "
            f"edge={edge:+.3f} sim={best_similarity:.2f}"
        )

        return edge

    except Exception as e:
        logger.debug(f"Manifold API error: {e}")
        return None


def _text_similarity(a: str, b: str) -> float:
    """Simple word overlap similarity (Jaccard-like)."""
    words_a = set(a.split())
    words_b = set(b.split())
    if not words_a or not words_b:
        return 0.0
    intersection = words_a & words_b
    union = words_a | words_b
    return len(intersection) / len(union)


# ============================================================
# 3. OPEN-METEO ENSEMBLE — Probabilistic weather forecasts
# ============================================================

def compute_weather_ensemble_edges(engine) -> int:
    """Use Open-Meteo ensemble models for more accurate probability estimates.

    The ensemble API returns min/max/mean from 50+ weather models,
    giving a natural probability distribution for temperature forecasts.

    Returns number of markets updated.
    """
    from services.weather_forecast import parse_weather_market

    weather_markets = engine.query(
        "SELECT id, question, yes_price, no_price FROM markets "
        "WHERE (question LIKE '%temperature%' OR question LIKE '%deg%F%' OR question LIKE '%deg%C%') "
        "AND accepting_orders = 1 AND yes_price > 0 AND yes_price < 1 "
        "ORDER BY volume DESC LIMIT 50"
    )

    if not weather_markets:
        return 0

    # Cache ensemble forecasts by (lat, lon) — avoids duplicate API calls for same city
    ensemble_cache = {}
    updated = 0
    import time

    for market in weather_markets:
        try:
            parsed = parse_weather_market(market["question"])
            if not parsed:
                continue

            lat, lon = parsed["lat"], parsed["lon"]
            cache_key = (lat, lon)

            if cache_key not in ensemble_cache:
                # Rate-limit: 5s between API calls to avoid Open-Meteo 429
                if ensemble_cache:
                    time.sleep(5)
                ensemble_cache[cache_key] = _fetch_ensemble_forecast(lat, lon)

            ensemble = ensemble_cache[cache_key]
            if not ensemble:
                continue

            target_date = parsed["target_date"]
            date_str = target_date.strftime("%Y-%m-%d")

            # Get ensemble stats for target date
            day_data = ensemble.get(date_str)
            if not day_data:
                continue

            if parsed["temp_type"] == "max":
                mean_temp = day_data["temp_max_mean"]
                spread = day_data["temp_max_spread"]
            else:
                mean_temp = day_data["temp_min_mean"]
                spread = day_data["temp_min_spread"]

            if spread <= 0:
                spread = 1.0  # fallback

            # Probability using ensemble spread as natural uncertainty
            fair_prob = _probability_in_range(
                mean_temp, parsed["temp_low_c"], parsed["temp_high_c"], spread / 2.0
            )

            # Edge calculation
            edge = fair_prob - market["yes_price"]

            engine.execute(
                "UPDATE markets SET calculated_edge = ?, last_updated = datetime('now') WHERE id = ?",
                (round(edge, 4), market["id"]),
            )
            updated += 1

        except Exception as e:
            logger.debug(f"Ensemble edge error for {market['id'][:30]}: {e}")

    logger.info(f"Weather ensemble: {updated}/{len(weather_markets)} markets updated")
    return updated


def _fetch_ensemble_forecast(lat: float, lon: float) -> Optional[dict]:
    """Fetch weather forecast from Open-Meteo.

    Tries ensemble API first (multi-model spread), falls back to regular
    forecast API on 429 rate-limit errors.

    Returns {date_str: {temp_max_mean, temp_max_spread, temp_min_mean, temp_min_spread}}
    """
    # Try ensemble API first
    result = _try_ensemble_api(lat, lon)
    if result is not None:
        return result

    # Fallback: regular forecast API (more generous rate limits)
    return _try_regular_forecast_api(lat, lon)


def _try_ensemble_api(lat: float, lon: float) -> Optional[dict]:
    """Try the ensemble API. Returns None on 429 so caller can fallback."""
    try:
        resp = httpx.get(
            "https://ensemble-api.open-meteo.com/v1/ensemble",
            params={
                "latitude": lat,
                "longitude": lon,
                "daily": "temperature_2m_max,temperature_2m_min",
                "timezone": "auto",
                "forecast_days": 7,
                "models": "icon_seamless,gfs_seamless,ecmwf_ifs025,gem_global",
            },
            timeout=15,
        )
        if resp.status_code == 429:
            logger.debug(f"Ensemble API 429 for ({lat},{lon}), falling back to regular API")
            return None  # signal fallback
        resp.raise_for_status()
        data = resp.json()

        daily = data.get("daily", {})
        dates = daily.get("time", [])
        t_max = daily.get("temperature_2m_max", [])
        t_min = daily.get("temperature_2m_min", [])

        if not dates:
            return None

        result = {}
        for i, date_str in enumerate(dates):
            max_vals = t_max[i] if isinstance(t_max[i], list) else [t_max[i]] if i < len(t_max) else []
            min_vals = t_min[i] if isinstance(t_min[i], list) else [t_min[i]] if i < len(t_min) else []
            max_vals = [v for v in max_vals if v is not None]
            min_vals = [v for v in min_vals if v is not None]

            if max_vals and min_vals:
                max_mean = sum(max_vals) / len(max_vals)
                min_mean = sum(min_vals) / len(min_vals)
                max_std = (sum((v - max_mean) ** 2 for v in max_vals) / len(max_vals)) ** 0.5 if len(max_vals) > 1 else 1.5
                min_std = (sum((v - min_mean) ** 2 for v in min_vals) / len(min_vals)) ** 0.5 if len(min_vals) > 1 else 1.5
                result[date_str] = {
                    "temp_max_mean": max_mean,
                    "temp_max_spread": max_std if max_std > 0.1 else 1.5,
                    "temp_min_mean": min_mean,
                    "temp_min_spread": min_std if min_std > 0.1 else 1.5,
                    "temp_max_members": len(max_vals),
                    "temp_min_members": len(min_vals),
                }

        return result

    except Exception as e:
        logger.debug(f"Ensemble API error for ({lat},{lon}): {e}")
        return None  # trigger fallback


def _try_regular_forecast_api(lat: float, lon: float) -> Optional[dict]:
    """Fallback: use regular Open-Meteo forecast API (single model, generous limits).

    Uses a default spread of 2.0C to account for forecast uncertainty.
    """
    try:
        resp = httpx.get(
            "https://api.open-meteo.com/v1/forecast",
            params={
                "latitude": lat,
                "longitude": lon,
                "daily": "temperature_2m_max,temperature_2m_min",
                "timezone": "auto",
                "forecast_days": 7,
            },
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()

        daily = data.get("daily", {})
        dates = daily.get("time", [])
        t_max = daily.get("temperature_2m_max", [])
        t_min = daily.get("temperature_2m_min", [])

        if not dates:
            return None

        result = {}
        DEFAULT_SPREAD = 2.0  # conservative uncertainty for single-model forecast
        for i, date_str in enumerate(dates):
            if i < len(t_max) and i < len(t_min) and t_max[i] is not None and t_min[i] is not None:
                result[date_str] = {
                    "temp_max_mean": t_max[i],
                    "temp_max_spread": DEFAULT_SPREAD,
                    "temp_min_mean": t_min[i],
                    "temp_min_spread": DEFAULT_SPREAD,
                    "temp_max_members": 1,
                    "temp_min_members": 1,
                }

        return result if result else None

    except Exception as e:
        logger.error(f"Regular forecast API error for ({lat},{lon}): {e}")
        return None


def _probability_in_range(forecast: float, low: float, high: float, std: float) -> float:
    """P(low <= actual <= high) given forecast mean and std deviation."""
    if std <= 0:
        return 1.0 if low <= forecast <= high else 0.0
    z_low = (low - forecast) / std
    z_high = (high - forecast) / std
    return _normal_cdf(z_high) - _normal_cdf(z_low)


def _normal_cdf(x: float) -> float:
    """Approximate standard normal CDF."""
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


# ============================================================
# 4. SPORTS LIVE SCORES — ESPN fallback for TheSportsDB
# ============================================================

def fetch_live_scores_espn() -> list:
    """Fetch live/recent scores from ESPN public API (no auth needed).

    Returns list of dicts compatible with the sport sniper format.
    Falls back to TheSportsDB if ESPN fails.
    """
    games = []

    # ESPN public scoreboards (no auth needed)
    espn_endpoints = {
        "nba": "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard",
        "nhl": "https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/scoreboard",
        "epl": "https://site.api.espn.com/apis/site/v2/sports/soccer/eng.1/scoreboard",
        "mlb": "https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/scoreboard",
        "nfl": "https://site.api.espn.com/apis/site/v2/sports/football/nfl/scoreboard",
        "champions_league": "https://site.api.espn.com/apis/site/v2/sports/soccer/uefa.champions/scoreboard",
        "la_liga": "https://site.api.espn.com/apis/site/v2/sports/soccer/esp.1/scoreboard",
        "bundesliga": "https://site.api.espn.com/apis/site/v2/sports/soccer/ger.1/scoreboard",
        "serie_a": "https://site.api.espn.com/apis/site/v2/sports/soccer/ita.1/scoreboard",
    }

    for league, url in espn_endpoints.items():
        try:
            resp = httpx.get(url, timeout=10)
            if resp.status_code != 200:
                continue

            data = resp.json()
            events = data.get("events", [])

            for event in events:
                competitions = event.get("competitions", [])
                for comp in competitions:
                    competitors = comp.get("competitors", [])
                    if len(competitors) < 2:
                        continue

                    home = next((c for c in competitors if c.get("homeAway") == "home"), competitors[0])
                    away = next((c for c in competitors if c.get("homeAway") == "away"), competitors[1])

                    status_obj = comp.get("status", {}) or event.get("status", {})
                    status_type = status_obj.get("type", {})
                    status_name = status_type.get("name", "")
                    status_desc = status_type.get("description", "")

                    # Map ESPN status to our format
                    if status_name == "STATUS_FINAL":
                        game_status = "FT"
                    elif status_name == "STATUS_IN_PROGRESS":
                        game_status = "LIVE"
                    elif status_name == "STATUS_SCHEDULED":
                        game_status = "Scheduled"
                    else:
                        game_status = status_desc or status_name

                    game = {
                        "home_team": home.get("team", {}).get("displayName", ""),
                        "away_team": away.get("team", {}).get("displayName", ""),
                        "home_score": home.get("score", ""),
                        "away_score": away.get("score", ""),
                        "status": game_status,
                        "sport": league.split("_")[0] if "_" in league else league,
                        "league": league,
                        "event_name": event.get("name", ""),
                        "date": event.get("date", ""),
                    }
                    games.append(game)

        except Exception as e:
            logger.warning(f"ESPN {league} error: {e}")

    logger.info(f"ESPN scores: {len(games)} games fetched")
    return games


# ============================================================
# Main orchestrator — called by scheduler
# ============================================================

def run_all_edge_sources(config) -> dict:
    """Run all edge sources and return summary.

    Called by the scheduler job.
    """
    from db import engine
    import time

    results = {}

    # 1. Crypto edges (Binance — free, no auth)
    try:
        results["crypto"] = compute_crypto_edges(engine)
    except Exception as e:
        logger.error(f"Crypto edge source failed: {e}")
        results["crypto"] = 0

    time.sleep(1)

    # 2. Cross-platform edges (Manifold — free, no auth)
    try:
        results["cross_platform"] = compute_cross_platform_edges(engine)
    except Exception as e:
        logger.error(f"Cross-platform edge source failed: {e}")
        results["cross_platform"] = 0

    time.sleep(1)

    # 3. Weather ensemble edges (Open-Meteo — free, no auth)
    try:
        results["weather_ensemble"] = compute_weather_ensemble_edges(engine)
    except Exception as e:
        logger.error(f"Weather ensemble edge source failed: {e}")
        results["weather_ensemble"] = 0

    total = sum(results.values())
    logger.info(f"Edge sources complete: {total} total updates | {results}")
    return results
