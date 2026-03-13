"""
Resolution Sniper — Buy positions when we KNOW the outcome before market settlement.

Three snipers:
1. Weather Resolution: Ultra-precise hourly forecasts for markets settling today/tomorrow
2. Economic Data: CPI, Fed decisions, jobs reports — buy when data is released
3. Sport Score: Live scores for finished games that haven't settled on Polymarket

Each sniper checks external data, calculates confidence, and creates trade suggestions
(or auto-executes in full-auto mode).
"""

import json
import logging
import re
import math
from datetime import datetime, timedelta, timezone
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

# -----------------------------------------------
# Shared helpers
# -----------------------------------------------

def _should_buy(confidence: float, market_price: float) -> bool:
    """Decide whether to buy based on confidence and current market price.

    Tier 1: confidence >= 0.95 AND price <= 0.85 -> BUY (almost certain)
    Tier 2: confidence >= 0.90 AND price <= 0.70 -> BUY (high confidence, needs better price)
    """
    if confidence >= 0.95 and market_price <= 0.85:
        return True
    if confidence >= 0.90 and market_price <= 0.70:
        return True
    return False


def _calculate_amount(confidence: float, edge: float, capital: float, max_pct: float) -> float:
    """Confidence-scaled position sizing."""
    max_amount = min(capital * max_pct, 20.0)  # hard cap $20
    # Scale by confidence * edge
    amount = round(confidence * edge * capital * 0.8, 2)
    amount = min(amount, max_amount)
    amount = max(amount, 1.0)  # minimum $1
    return amount


def _create_sniper_suggestion(engine, config, market_id: str, question: str,
                               side: str, price: float, confidence: float,
                               edge: float, data_source: str, detail: str,
                               sniper_type: str):
    """Create a trade suggestion (or auto-approve in full-auto mode) and send Telegram alert."""
    from config import load_platform_config
    from services.telegram_alerts import get_alerts

    platform_cfg = load_platform_config()
    trading_cfg = platform_cfg.get("trading", {})
    mode = trading_cfg.get("mode", "paper")
    capital = trading_cfg.get("capital_usd", 100.0)
    limits = trading_cfg.get("limits", {})
    max_pct = limits.get("max_position_pct", 5) / 100

    # Skip if we already have open position
    open_pos = engine.query_one(
        "SELECT id FROM trades WHERE market_id = ? AND status IN ('executed', 'executing') "
        "AND (result IS NULL OR result = 'open')",
        (market_id,),
    )
    if open_pos:
        logger.debug(f"Sniper: skip {market_id}, open position exists")
        return False

    # Skip if recent suggestion already exists
    existing = engine.query_one(
        "SELECT id FROM suggestions WHERE status IN ('pending', 'auto_approved') "
        "AND payload LIKE ? AND created_at > datetime('now', '-2 hours')",
        (f'%"market_id": "{market_id}"%',),
    )
    if existing:
        logger.debug(f"Sniper: skip {market_id}, recent suggestion exists")
        return False

    amount = _calculate_amount(confidence, edge, capital, max_pct)
    status = "auto_approved" if mode == "full-auto" else "pending"

    payload = {
        "market_id": market_id,
        "market_question": question,
        "side": side,
        "amount_usd": amount,
        "price": price,
        "edge": round(edge, 4),
        "confidence": round(confidence, 4),
        "data_source": data_source,
        "detail": detail,
        "strategy_name": "Resolution Sniper",
        "sniper_type": sniper_type,
    }

    engine.execute(
        """INSERT INTO suggestions (agent_id, type, title, description, payload, status, created_at)
           VALUES (?, ?, ?, ?, ?, ?, datetime('now'))""",
        (
            "resolution-sniper",
            "trade",
            f"Snipe: {side} '{question[:50]}...'",
            f"Confidence: {confidence:.0%} | Price: {price:.2f} | Edge: {edge:+.1%} | "
            f"Source: {data_source} | {detail[:100]}",
            json.dumps(payload),
            status,
        ),
    )

    # Telegram alert
    try:
        alerts = get_alerts(config)
        alerts.send(
            f"\xf0\x9f\x8e\xaf <b>Resolution Snipe</b>\n"
            f"Markt: {question[:80]}\n"
            f"Seite: {side} @ {price:.2f}\n"
            f"Confidence: {confidence:.0%} | Edge: {edge:+.1%}\n"
            f"Quelle: {data_source}\n"
            f"Detail: {detail[:120]}\n"
            f"Betrag: ${amount:.2f} | Modus: {mode}"
        )
    except Exception as e:
        logger.warning(f"Sniper telegram alert failed: {e}")

    logger.info(f"Resolution snipe created: {side} {question[:60]} conf={confidence:.0%} edge={edge:+.1%}")
    return True


# -----------------------------------------------
# 1. WEATHER RESOLUTION SNIPER
# -----------------------------------------------

def run_weather_sniper(config) -> int:
    """Enhanced weather sniper using hourly forecasts for markets settling today/tomorrow.

    Returns number of suggestions created.
    """
    from db import engine
    from services.weather_forecast import parse_weather_market

    now = datetime.utcnow()
    suggestions = 0

    # Find weather markets
    weather_markets = engine.query(
        "SELECT id, question, yes_price, no_price FROM markets "
        "WHERE (question LIKE '%temperature%' OR question LIKE '%deg%F%' OR question LIKE '%deg%C%') "
        "AND accepting_orders = 1 AND yes_price > 0 AND yes_price < 1 "
        "ORDER BY volume DESC LIMIT 200"
    )

    if not weather_markets:
        logger.debug("Weather sniper: no temperature markets found")
        return 0

    # Cache hourly forecasts by (lat, lon)
    hourly_cache = {}

    for m in weather_markets:
        try:
            parsed = parse_weather_market(m["question"])
            if not parsed:
                continue

            target_date = parsed["target_date"]
            days_ahead = (target_date - now).days

            # Only snipe markets settling TODAY or TOMORROW
            if days_ahead < 0 or days_ahead > 1:
                continue

            lat, lon = parsed["lat"], parsed["lon"]
            cache_key = (lat, lon)

            # Fetch HOURLY forecast (more precise than daily)
            if cache_key not in hourly_cache:
                hourly_cache[cache_key] = _fetch_hourly_forecast(lat, lon)

            hourly = hourly_cache[cache_key]
            if not hourly:
                continue

            # Get all hourly temps for target date
            date_str = target_date.strftime("%Y-%m-%d")
            day_temps = [
                hourly[ts] for ts in hourly
                if ts.startswith(date_str)
            ]

            if not day_temps:
                continue

            # Calculate max/min from hourly data
            if parsed["temp_type"] == "max":
                forecast_temp = max(day_temps)
            else:
                forecast_temp = min(day_temps)

            # For today/tomorrow with hourly data, uncertainty is very low
            if days_ahead == 0:
                std = 0.3  # Very precise for today
            else:
                std = 0.7  # Still quite precise for tomorrow with hourly

            # Calculate probability
            fair_prob = _probability_in_range(
                forecast_temp, parsed["temp_low_c"], parsed["temp_high_c"], std
            )

            # Determine confidence and side
            if fair_prob >= 0.90:
                confidence = fair_prob
                side = "YES"
                price = m["yes_price"]
                edge = confidence - price
            elif fair_prob <= 0.10:
                confidence = 1.0 - fair_prob
                side = "NO"
                price = m["no_price"] if m["no_price"] else 1.0 - m["yes_price"]
                edge = confidence - price
            else:
                continue  # Not confident enough

            if edge <= 0.05:
                continue  # Not enough edge

            if not _should_buy(confidence, price):
                continue

            detail = (
                f"Hourly forecast: {forecast_temp:.1f}C | "
                f"Range: {parsed['temp_low_c']:.1f}-{parsed['temp_high_c']:.1f}C | "
                f"Days ahead: {days_ahead} | City: {parsed['city'].title()}"
            )

            created = _create_sniper_suggestion(
                engine, config, m["id"], m["question"],
                side, price, confidence, edge,
                "Open-Meteo Hourly", detail, "weather"
            )
            if created:
                suggestions += 1

        except Exception as e:
            logger.error(f"Weather sniper error for {m.get('id', '?')}: {e}")

    logger.info(f"Weather sniper: {suggestions} suggestions from {len(weather_markets)} markets")
    return suggestions


def _fetch_hourly_forecast(lat: float, lon: float) -> Optional[dict]:
    """Fetch hourly forecast from Open-Meteo. Returns {datetime_str: temp_c}."""
    try:
        resp = httpx.get(
            "https://api.open-meteo.com/v1/forecast",
            params={
                "latitude": lat,
                "longitude": lon,
                "hourly": "temperature_2m,precipitation,weathercode",
                "timezone": "auto",
                "forecast_days": 3,
            },
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()

        hourly = data.get("hourly", {})
        times = hourly.get("time", [])
        temps = hourly.get("temperature_2m", [])

        result = {}
        for i, t in enumerate(times):
            if i < len(temps) and temps[i] is not None:
                result[t] = temps[i]
        return result

    except Exception as e:
        logger.error(f"Hourly forecast API error: {e}")
        return None


def _probability_in_range(forecast: float, low: float, high: float, std: float) -> float:
    """P(low <= actual <= high) given forecast and uncertainty."""
    if std <= 0:
        return 1.0 if low <= forecast <= high else 0.0
    z_low = (low - forecast) / std
    z_high = (high - forecast) / std
    return _normal_cdf(z_high) - _normal_cdf(z_low)


def _normal_cdf(x: float) -> float:
    """Approximate standard normal CDF."""
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


# -----------------------------------------------
# 2. ECONOMIC DATA SNIPER
# -----------------------------------------------

# Known economic indicators and their Polymarket keyword patterns
ECONOMIC_INDICATORS = {
    "CPI": {
        "fred_series": "CPIAUCSL",
        "keywords": ["CPI", "consumer price index", "inflation rate"],
        "market_patterns": [
            r"(?i)will.*CPI.*(?:above|below|over|under|exceed|at least)\s*(\d+\.?\d*)",
            r"(?i)(?:CPI|inflation).*(?:year.over.year|YoY|annual).*(\d+\.?\d*)",
        ],
    },
    "unemployment": {
        "fred_series": "UNRATE",
        "keywords": ["unemployment", "jobless", "jobs report", "nonfarm payroll"],
        "market_patterns": [
            r"(?i)will.*unemployment.*(?:above|below|over|under)\s*(\d+\.?\d*)",
        ],
    },
    "fed_rate": {
        "fred_series": "FEDFUNDS",
        "keywords": ["fed", "federal reserve", "interest rate", "FOMC", "rate cut", "rate hike"],
        "market_patterns": [
            r"(?i)will.*(?:fed|FOMC).*(?:cut|raise|hike|hold|lower|increase)",
            r"(?i)(?:federal funds|interest) rate.*(\d+\.?\d*)",
        ],
    },
    "GDP": {
        "fred_series": "GDP",
        "keywords": ["GDP", "gross domestic product", "economic growth"],
        "market_patterns": [
            r"(?i)will.*GDP.*(?:above|below|over|under)\s*(\d+\.?\d*)",
        ],
    },
}


def run_economic_sniper(config) -> int:
    """Check for newly released economic data and snipe related markets.

    Returns number of suggestions created.
    """
    from db import engine

    suggestions = 0
    now = datetime.now(timezone.utc)

    # Only run during US market-relevant hours (8-18 UTC)
    if now.hour < 8 or now.hour > 18:
        logger.debug("Economic sniper: outside US hours, skipping")
        return 0

    # Step 1: Find economic-related markets on Polymarket
    econ_markets = _find_economic_markets(engine)
    if not econ_markets:
        logger.debug("Economic sniper: no economic markets found")
        return 0

    # Step 2: For each market, try to get the relevant data
    for market in econ_markets:
        try:
            result = _check_economic_resolution(market)
            if not result:
                continue

            confidence = result["confidence"]
            side = result["side"]
            price = market["yes_price"] if side == "YES" else (1.0 - market["yes_price"])
            edge = confidence - price

            if edge <= 0.05:
                continue

            if not _should_buy(confidence, price):
                continue

            detail = (
                f"Indicator: {result['indicator']} | "
                f"Value: {result['value']} | "
                f"Released: {result.get('release_date', 'recent')}"
            )

            created = _create_sniper_suggestion(
                engine, config, market["id"], market["question"],
                side, price, confidence, edge,
                f"FRED/{result['indicator']}", detail, "economic"
            )
            if created:
                suggestions += 1

        except Exception as e:
            logger.error(f"Economic sniper error for {market.get('id', '?')}: {e}")

    logger.info(f"Economic sniper: {suggestions} suggestions from {len(econ_markets)} markets")
    return suggestions


def _find_economic_markets(engine) -> list:
    """Find Polymarket markets related to economic indicators."""
    # Build keyword search query
    conditions = []
    for indicator, info in ECONOMIC_INDICATORS.items():
        for kw in info["keywords"]:
            conditions.append(f"question LIKE '%{kw}%'")

    if not conditions:
        return []

    where = " OR ".join(conditions)
    markets = engine.query(
        f"SELECT id, question, yes_price, no_price, volume FROM markets "
        f"WHERE ({where}) AND accepting_orders = 1 "
        f"AND yes_price > 0 AND yes_price < 1 "
        f"ORDER BY volume DESC LIMIT 50"
    )
    return markets


def _check_economic_resolution(market: dict) -> Optional[dict]:
    """Check if economic data has been released that resolves this market.

    Uses FRED API (free with key) to get latest data.
    """
    question = market["question"].lower()

    for indicator_name, info in ECONOMIC_INDICATORS.items():
        # Check if this market matches the indicator
        matched = any(kw.lower() in question for kw in info["keywords"])
        if not matched:
            continue

        # Fetch latest data from FRED
        latest = _fetch_fred_latest(info["fred_series"])
        if not latest:
            continue

        value = latest["value"]
        release_date = latest["date"]

        # Check if data was released recently (within last 30 days)
        try:
            rel_dt = datetime.strptime(release_date, "%Y-%m-%d")
            days_old = (datetime.utcnow() - rel_dt).days
            if days_old > 30:
                continue  # Data is too old
        except Exception:
            pass

        # Try to determine if the data resolves the market
        resolution = _match_value_to_market(question, value, indicator_name)
        if resolution:
            return {
                "indicator": indicator_name,
                "value": value,
                "release_date": release_date,
                "confidence": resolution["confidence"],
                "side": resolution["side"],
            }

    return None


def _fetch_fred_latest(series_id: str) -> Optional[dict]:
    """Fetch latest observation from FRED API."""
    try:
        import os
        fred_key = os.getenv("FRED_API_KEY", "")

        if not fred_key:
            logger.debug(f"No FRED_API_KEY set, skipping FRED lookup for {series_id}")
            return None

        resp = httpx.get(
            "https://api.stlouisfed.org/fred/series/observations",
            params={
                "series_id": series_id,
                "api_key": fred_key,
                "file_type": "json",
                "sort_order": "desc",
                "limit": 1,
            },
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()
        obs = data.get("observations", [])
        if obs and obs[0]["value"] != ".":
            return {"value": float(obs[0]["value"]), "date": obs[0]["date"]}

    except Exception as e:
        logger.error(f"FRED API error for {series_id}: {e}")

    return None


def _match_value_to_market(question: str, value: float, indicator: str) -> Optional[dict]:
    """Determine if the released economic value clearly resolves the market question."""
    # Pattern: "Will CPI be above 3%?" and value is 3.2 -> YES with high confidence

    above_match = re.search(
        r"(?:above|over|exceed|higher than|at least|more than)\s*(\d+\.?\d*)\s*%?",
        question, re.IGNORECASE,
    )
    below_match = re.search(
        r"(?:below|under|lower than|less than|at most)\s*(\d+\.?\d*)\s*%?",
        question, re.IGNORECASE,
    )

    if above_match:
        threshold = float(above_match.group(1))
        if value > threshold * 1.02:  # 2% margin above -> clearly YES
            return {"side": "YES", "confidence": 0.95}
        elif value < threshold * 0.98:  # 2% margin below -> clearly NO
            return {"side": "NO", "confidence": 0.95}

    if below_match:
        threshold = float(below_match.group(1))
        if value < threshold * 0.98:
            return {"side": "YES", "confidence": 0.95}
        elif value > threshold * 1.02:
            return {"side": "NO", "confidence": 0.95}

    # For Fed decisions (cut/hike/hold), need rate comparison - skip if ambiguous
    return None


# -----------------------------------------------
# 3. SPORT SCORE SNIPER
# -----------------------------------------------

# Sports keywords to identify markets
SPORT_KEYWORDS = {
    "nba": ["NBA", "basketball", "Lakers", "Celtics", "Warriors", "Bucks", "76ers",
            "Nuggets", "Suns", "Nets", "Heat", "Knicks", "Mavericks", "Clippers",
            "Timberwolves", "Thunder", "Cavaliers", "Pacers", "Kings", "Hawks",
            "Rockets", "Grizzlies", "Magic", "Bulls", "Pelicans", "Raptors",
            "Pistons", "Hornets", "Spurs", "Trail Blazers", "Jazz", "Wizards"],
    "nhl": ["NHL", "hockey", "Bruins", "Panthers", "Rangers", "Hurricanes",
            "Stars", "Avalanche", "Oilers", "Jets", "Maple Leafs", "Lightning",
            "Capitals", "Penguins", "Devils", "Islanders", "Flyers", "Senators",
            "Canadiens", "Red Wings", "Sabres", "Blue Jackets", "Predators",
            "Wild", "Flames", "Canucks", "Kraken", "Ducks", "Sharks", "Coyotes", "Blackhawks"],
    "epl": ["Premier League", "EPL", "Arsenal", "Manchester City", "Manchester United",
            "Liverpool", "Chelsea", "Tottenham", "Newcastle", "Brighton", "Aston Villa",
            "West Ham", "Brentford", "Crystal Palace", "Wolves", "Fulham", "Everton",
            "Bournemouth", "Nottingham Forest", "Luton", "Sheffield United", "Burnley"],
    "soccer": ["Champions League", "La Liga", "Serie A", "Bundesliga", "Ligue 1",
               "Real Madrid", "Barcelona", "Bayern", "PSG", "Juventus", "Inter Milan",
               "AC Milan", "Dortmund", "Atletico"],
}


def run_sport_sniper(config) -> int:
    """Check live scores for finished games and snipe unresolved markets.

    Returns number of suggestions created.
    """
    from db import engine

    suggestions = 0

    # Find sports-related markets
    sport_markets = _find_sport_markets(engine)
    if not sport_markets:
        logger.debug("Sport sniper: no sports markets found")
        return 0

    # Get live/recent scores
    live_scores = _fetch_live_scores()
    finished_games = [g for g in live_scores if g.get("status") in ("FT", "Final", "Finished", "AOT", "AET")]

    if not finished_games:
        logger.debug(f"Sport sniper: no finished games found (total live: {len(live_scores)})")
        return 0

    logger.info(f"Sport sniper: {len(finished_games)} finished games, {len(sport_markets)} sport markets")

    for market in sport_markets:
        try:
            # Try to match market to a finished game
            match = _match_market_to_game(market["question"], finished_games)
            if not match:
                continue

            game = match["game"]
            side = match["side"]
            confidence = match["confidence"]
            price = market["yes_price"] if side == "YES" else (1.0 - market["yes_price"])
            edge = confidence - price

            if edge <= 0.05:
                continue

            if not _should_buy(confidence, price):
                continue

            score_str = (
                f"{game.get('home_team', '?')} "
                f"{game.get('home_score', '?')}-{game.get('away_score', '?')} "
                f"{game.get('away_team', '?')}"
            )
            detail = f"Final Score: {score_str} | Status: {game.get('status', '?')}"

            created = _create_sniper_suggestion(
                engine, config, market["id"], market["question"],
                side, price, confidence, edge,
                "TheSportsDB Live", detail, "sport"
            )
            if created:
                suggestions += 1

        except Exception as e:
            logger.error(f"Sport sniper error for {market.get('id', '?')}: {e}")

    logger.info(f"Sport sniper: {suggestions} suggestions from {len(sport_markets)} markets")
    return suggestions


def _find_sport_markets(engine) -> list:
    """Find Polymarket markets related to sports."""
    all_keywords = []
    for sport, keywords in SPORT_KEYWORDS.items():
        all_keywords.extend(keywords)

    # Build LIKE conditions for top keywords (limit to avoid huge query)
    conditions = [f"question LIKE '%{kw}%'" for kw in all_keywords[:50]]
    if not conditions:
        return []

    where = " OR ".join(conditions)
    markets = engine.query(
        f"SELECT id, question, yes_price, no_price, volume FROM markets "
        f"WHERE ({where}) AND accepting_orders = 1 "
        f"AND yes_price > 0 AND yes_price < 1 "
        f"ORDER BY volume DESC LIMIT 100"
    )
    return markets


def _fetch_live_scores() -> list:
    """Fetch live and recent scores from TheSportsDB (free, no key needed).

    Returns list of dicts with: home_team, away_team, home_score, away_score, status, sport
    """
    games = []

    # TheSportsDB free livescore endpoints
    endpoints = [
        "https://www.thesportsdb.com/api/v1/json/3/livescore.php?s=Soccer",
        "https://www.thesportsdb.com/api/v1/json/3/livescore.php?s=Basketball",
        "https://www.thesportsdb.com/api/v1/json/3/livescore.php?s=Ice_Hockey",
    ]

    for url in endpoints:
        try:
            resp = httpx.get(url, timeout=10)
            if resp.status_code != 200:
                continue

            data = resp.json()
            events = data.get("events") or []

            for ev in events:
                game = {
                    "home_team": ev.get("strHomeTeam", ""),
                    "away_team": ev.get("strAwayTeam", ""),
                    "home_score": ev.get("intHomeScore", ""),
                    "away_score": ev.get("intAwayScore", ""),
                    "status": ev.get("strStatus", ""),
                    "sport": ev.get("strSport", ""),
                    "league": ev.get("strLeague", ""),
                    "event_name": ev.get("strEvent", ""),
                    "date": ev.get("dateEvent", ""),
                }
                games.append(game)

        except Exception as e:
            logger.warning(f"TheSportsDB error for {url}: {e}")

    return games


def _match_market_to_game(question: str, finished_games: list) -> Optional[dict]:
    """Try to match a market question to a finished game.

    Looks for team names in the question and checks the result.
    Returns: {game, side, confidence} or None
    """
    q_lower = question.lower()

    for game in finished_games:
        home = game["home_team"]
        away = game["away_team"]

        if not home or not away:
            continue

        home_lower = home.lower()
        away_lower = away.lower()

        # Check if both teams (or at least one key team) appear in the question
        home_in = home_lower in q_lower or _team_name_match(home_lower, q_lower)
        away_in = away_lower in q_lower or _team_name_match(away_lower, q_lower)

        if not (home_in or away_in):
            continue

        # Parse scores
        try:
            h_score = int(game["home_score"])
            a_score = int(game["away_score"])
        except (ValueError, TypeError):
            continue

        # Determine winner
        if h_score > a_score:
            winner = home
            loser = away
        elif a_score > h_score:
            winner = away
            loser = home
        else:
            winner = None  # Draw
            loser = None

        # Pattern: "Will [team] win?" or "Will [team] beat [team]?"
        win_patterns = [
            r"(?i)will\s+(.+?)\s+(?:win|beat|defeat)",
            r"(?i)(.+?)\s+(?:to win|victory|wins)",
        ]

        for pattern in win_patterns:
            m = re.search(pattern, question)
            if m:
                team_mentioned = m.group(1).strip().lower()
                if winner and (winner.lower() in team_mentioned or _team_name_match(winner.lower(), team_mentioned)):
                    return {"game": game, "side": "YES", "confidence": 0.98}
                elif loser and (loser.lower() in team_mentioned or _team_name_match(loser.lower(), team_mentioned)):
                    return {"game": game, "side": "NO", "confidence": 0.98}

        # Pattern: "[team] vs [team]" -- check if the market is about the winner
        if home_in and away_in and winner:
            # If the question seems to ask about home team winning
            if home_lower in q_lower[:len(q_lower) // 2]:  # First team mentioned
                if winner == home:
                    return {"game": game, "side": "YES", "confidence": 0.95}
                else:
                    return {"game": game, "side": "NO", "confidence": 0.95}

    return None


def _team_name_match(team_name: str, text: str) -> bool:
    """Fuzzy match team names (handles abbreviations and partial names)."""
    # Try last word (e.g., "Manchester United" -> "United")
    parts = team_name.split()
    if len(parts) > 1:
        if parts[-1].lower() in text and len(parts[-1]) > 3:
            return True
    # Try first word for short names
    if parts[0].lower() in text and len(parts[0]) > 4:
        return True
    return False
