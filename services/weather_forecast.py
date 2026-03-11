"""
Weather Forecast Service for Polymarket Weather Markets.

Uses Open-Meteo API (free, no API key) to fetch forecasts and calculate
edge against Polymarket prices. Supports daily high/low temperature markets.

Typical weather market format:
  "Will the highest temperature in New York City be between 40-41°F on March 12?"
  "Will the highest temperature in Ankara be 5°C on March 7?"
"""

import re
import logging
import math
from datetime import datetime, timedelta
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

# City coordinates for Open-Meteo (lat, lon)
CITY_COORDS = {
    "new york city": (40.71, -74.01),
    "new york": (40.71, -74.01),
    "nyc": (40.71, -74.01),
    "los angeles": (34.05, -118.24),
    "chicago": (41.88, -87.63),
    "houston": (29.76, -95.37),
    "phoenix": (33.45, -112.07),
    "miami": (25.76, -80.19),
    "dallas": (32.78, -96.80),
    "san francisco": (37.77, -122.42),
    "seattle": (47.61, -122.33),
    "denver": (39.74, -104.99),
    "boston": (42.36, -71.06),
    "washington": (38.91, -77.04),
    "washington dc": (38.91, -77.04),
    "washington d.c.": (38.91, -77.04),
    "atlanta": (33.75, -84.39),
    "london": (51.51, -0.13),
    "paris": (48.86, 2.35),
    "berlin": (52.52, 13.41),
    "tokyo": (35.68, 139.69),
    "seoul": (37.57, 126.98),
    "beijing": (39.90, 116.40),
    "sydney": (-33.87, 151.21),
    "toronto": (43.65, -79.38),
    "mumbai": (19.08, 72.88),
    "delhi": (28.61, 77.21),
    "new delhi": (28.61, 77.21),
    "istanbul": (41.01, 28.98),
    "ankara": (39.93, 32.86),
    "moscow": (55.76, 37.62),
    "sao paulo": (-23.55, -46.63),
    "mexico city": (19.43, -99.13),
    "cairo": (30.04, 31.24),
    "lagos": (6.52, 3.38),
    "dubai": (25.20, 55.27),
    "singapore": (1.35, 103.82),
    "hong kong": (22.32, 114.17),
    "bangkok": (13.76, 100.50),
    "taipei": (25.03, 121.57),
    "osaka": (34.69, 135.50),
    "buenos aires": (-34.60, -58.38),
    "johannesburg": (-26.20, 28.05),
    "rome": (41.90, 12.50),
    "madrid": (40.42, -3.70),
    "lisbon": (38.72, -9.14),
    "amsterdam": (52.37, 4.90),
    "vienna": (48.21, 16.37),
    "zurich": (47.38, 8.54),
    "stockholm": (59.33, 18.07),
    "oslo": (59.91, 10.75),
    "helsinki": (60.17, 24.94),
    "warsaw": (52.23, 21.01),
    "prague": (50.08, 14.44),
    "budapest": (47.50, 19.04),
    "athens": (37.98, 23.73),
    "lucknow": (26.85, 80.95),
    "riyadh": (24.69, 46.72),
    "jakarta": (-6.21, 106.85),
    "manila": (14.60, 120.98),
    "hanoi": (21.03, 105.85),
    "kuala lumpur": (3.14, 101.69),
    "nairobi": (-1.29, 36.82),
    "lima": (-12.05, -77.04),
    "bogota": (4.71, -74.07),
    "santiago": (-33.45, -70.67),
    "vancouver": (49.28, -123.12),
    "montreal": (45.50, -73.57),
}

# Forecast uncertainty (std dev in °C) by lead time
FORECAST_UNCERTAINTY = {
    1: 1.0,   # 1 day ahead: ±1°C
    2: 1.2,   # 2 days: ±1.2°C
    3: 1.8,   # 3 days
    4: 2.2,
    5: 2.5,
    7: 3.0,
    10: 3.5,
    14: 4.0,
    16: 4.5,
}


def _get_uncertainty(days_ahead: int) -> float:
    """Get forecast uncertainty (std dev) for given lead time."""
    if days_ahead <= 0:
        return 0.5
    for threshold in sorted(FORECAST_UNCERTAINTY.keys()):
        if days_ahead <= threshold:
            return FORECAST_UNCERTAINTY[threshold]
    return 5.0


def _normal_cdf(x: float) -> float:
    """Approximate standard normal CDF."""
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def _probability_in_range(forecast: float, low: float, high: float, std: float) -> float:
    """P(low <= actual <= high) given forecast and uncertainty."""
    if std <= 0:
        return 1.0 if low <= forecast <= high else 0.0
    z_low = (low - forecast) / std
    z_high = (high - forecast) / std
    return _normal_cdf(z_high) - _normal_cdf(z_low)


def _f_to_c(f: float) -> float:
    """Fahrenheit to Celsius."""
    return (f - 32) * 5 / 9


def _c_to_f(c: float) -> float:
    """Celsius to Fahrenheit."""
    return c * 9 / 5 + 32


# Regex patterns for parsing weather market questions
_PATTERNS = [
    # "highest temperature in {city} be between {low}-{high}°F on {date}"
    re.compile(
        r"(?:highest|maximum)\s+temperature\s+in\s+(.+?)\s+be\s+between\s+"
        r"(\d+(?:\.\d+)?)\s*[-–]\s*(\d+(?:\.\d+)?)\s*°?\s*(F|C|°F|°C)\s+on\s+(.+?)[\?]?$",
        re.IGNORECASE,
    ),
    # "highest temperature in {city} be {temp}°C on {date}"
    re.compile(
        r"(?:highest|maximum)\s+temperature\s+in\s+(.+?)\s+be\s+"
        r"(\d+(?:\.\d+)?)\s*°?\s*(F|C|°F|°C)\s+(?:or\s+(?:higher|lower)\s+)?on\s+(.+?)[\?]?$",
        re.IGNORECASE,
    ),
    # "lowest temperature in {city} be between {low}-{high}°F on {date}"
    re.compile(
        r"(?:lowest|minimum)\s+temperature\s+in\s+(.+?)\s+be\s+between\s+"
        r"(\d+(?:\.\d+)?)\s*[-–]\s*(\d+(?:\.\d+)?)\s*°?\s*(F|C|°F|°C)\s+on\s+(.+?)[\?]?$",
        re.IGNORECASE,
    ),
    # "lowest temperature in {city} be {temp}°C on {date}"
    re.compile(
        r"(?:lowest|minimum)\s+temperature\s+in\s+(.+?)\s+be\s+"
        r"(\d+(?:\.\d+)?)\s*°?\s*(F|C|°F|°C)\s+(?:or\s+(?:higher|lower)\s+)?on\s+(.+?)[\?]?$",
        re.IGNORECASE,
    ),
]


def parse_weather_market(question: str) -> Optional[dict]:
    """Parse a weather market question into structured data.

    Returns dict with: city, lat, lon, temp_low_c, temp_high_c, target_date, temp_type (max/min)
    or None if unparseable.
    """
    if not question:
        return None

    temp_type = "max"
    if "lowest" in question.lower() or "minimum" in question.lower():
        temp_type = "min"

    for i, pattern in enumerate(_PATTERNS):
        m = pattern.search(question)
        if not m:
            continue

        groups = m.groups()
        city_raw = groups[0].strip().lower()

        # Look up coordinates
        coords = CITY_COORDS.get(city_raw)
        if not coords:
            # Try partial match
            for name, c in CITY_COORDS.items():
                if name in city_raw or city_raw in name:
                    coords = c
                    break
        if not coords:
            logger.debug(f"Unknown city: {city_raw}")
            return None

        # Parse temperature
        if i in (0, 2):  # range pattern
            temp_low = float(groups[1])
            temp_high = float(groups[2])
            unit = groups[3].replace("°", "")
            date_str = groups[4].strip()
        else:  # exact pattern
            temp_val = float(groups[1])
            unit = groups[2].replace("°", "")
            date_str = groups[3].strip()
            # For exact temp, use ±0.5 range
            temp_low = temp_val - 0.5
            temp_high = temp_val + 0.5
            if "or higher" in question.lower():
                temp_high = temp_val + 50  # effectively no upper limit
            elif "or lower" in question.lower():
                temp_low = temp_val - 50

        # Convert to Celsius
        if unit.upper() == "F":
            temp_low_c = _f_to_c(temp_low)
            temp_high_c = _f_to_c(temp_high)
        else:
            temp_low_c = temp_low
            temp_high_c = temp_high

        # Parse date
        target_date = _parse_date(date_str)
        if not target_date:
            return None

        return {
            "city": city_raw,
            "lat": coords[0],
            "lon": coords[1],
            "temp_low_c": temp_low_c,
            "temp_high_c": temp_high_c,
            "target_date": target_date,
            "temp_type": temp_type,
        }

    return None


def _parse_date(date_str: str) -> Optional[datetime]:
    """Parse date string like 'March 12' or 'March 12, 2026'."""
    date_str = date_str.strip().rstrip("?").strip()
    now = datetime.utcnow()

    for fmt in ("%B %d, %Y", "%B %d %Y", "%b %d, %Y", "%b %d %Y", "%B %d", "%b %d"):
        try:
            dt = datetime.strptime(date_str, fmt)
            if dt.year == 1900:  # no year specified
                dt = dt.replace(year=now.year)
                if dt < now - timedelta(days=30):
                    dt = dt.replace(year=now.year + 1)
            return dt
        except ValueError:
            continue
    return None


def fetch_forecast(lat: float, lon: float, days: int = 16) -> Optional[dict]:
    """Fetch weather forecast from Open-Meteo API.

    Returns dict mapping date string (YYYY-MM-DD) to {max_c, min_c}.
    """
    try:
        resp = httpx.get(
            "https://api.open-meteo.com/v1/forecast",
            params={
                "latitude": lat,
                "longitude": lon,
                "daily": "temperature_2m_max,temperature_2m_min",
                "timezone": "auto",
                "forecast_days": days,
            },
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()

        daily = data.get("daily", {})
        dates = daily.get("time", [])
        maxes = daily.get("temperature_2m_max", [])
        mins = daily.get("temperature_2m_min", [])

        result = {}
        for i, d in enumerate(dates):
            result[d] = {
                "max_c": maxes[i] if i < len(maxes) else None,
                "min_c": mins[i] if i < len(mins) else None,
            }
        return result

    except Exception as e:
        logger.error(f"Open-Meteo API error: {e}")
        return None


def calculate_weather_edge(market_question: str, yes_price: float) -> Optional[dict]:
    """Calculate edge for a weather market.

    Returns dict with: fair_probability, edge, forecast_temp, target_range, days_ahead
    or None if market can't be analyzed.
    """
    parsed = parse_weather_market(market_question)
    if not parsed:
        return None

    target_date = parsed["target_date"]
    days_ahead = (target_date - datetime.utcnow()).days

    # Only analyze markets 0-16 days ahead (forecast range)
    if days_ahead < 0 or days_ahead > 16:
        return None

    # Fetch forecast
    forecast = fetch_forecast(parsed["lat"], parsed["lon"])
    if not forecast:
        return None

    date_key = target_date.strftime("%Y-%m-%d")
    day_forecast = forecast.get(date_key)
    if not day_forecast:
        return None

    # Get forecast temperature
    if parsed["temp_type"] == "max":
        forecast_temp = day_forecast.get("max_c")
    else:
        forecast_temp = day_forecast.get("min_c")

    if forecast_temp is None:
        return None

    # Calculate probability
    std = _get_uncertainty(days_ahead)
    fair_prob = _probability_in_range(
        forecast_temp, parsed["temp_low_c"], parsed["temp_high_c"], std
    )

    # Edge = fair probability - market price
    edge = fair_prob - yes_price

    return {
        "fair_probability": round(fair_prob, 4),
        "edge": round(edge, 4),
        "forecast_temp_c": round(forecast_temp, 1),
        "target_low_c": round(parsed["temp_low_c"], 1),
        "target_high_c": round(parsed["temp_high_c"], 1),
        "days_ahead": days_ahead,
        "city": parsed["city"],
        "uncertainty_std": std,
    }


def analyze_weather_markets(markets: list[dict]) -> list[dict]:
    """Analyze a batch of weather markets and return those with edge.

    Each market dict should have: id, question, yes_price, no_price
    Returns list of dicts with market info + edge analysis.
    """
    results = []
    # Cache forecasts by (lat, lon) to avoid duplicate API calls
    forecast_cache = {}

    for m in markets:
        question = m.get("question", "")
        yes_price = m.get("yes_price", 0)

        if not question or not yes_price:
            continue

        parsed = parse_weather_market(question)
        if not parsed:
            continue

        target_date = parsed["target_date"]
        days_ahead = (target_date - datetime.utcnow()).days
        if days_ahead < 0 or days_ahead > 16:
            continue

        # Use cached forecast if available
        cache_key = (parsed["lat"], parsed["lon"])
        if cache_key not in forecast_cache:
            forecast_cache[cache_key] = fetch_forecast(parsed["lat"], parsed["lon"])

        forecast = forecast_cache[cache_key]
        if not forecast:
            continue

        date_key = target_date.strftime("%Y-%m-%d")
        day_forecast = forecast.get(date_key)
        if not day_forecast:
            continue

        forecast_temp = day_forecast.get("max_c") if parsed["temp_type"] == "max" else day_forecast.get("min_c")
        if forecast_temp is None:
            continue

        std = _get_uncertainty(days_ahead)
        fair_prob = _probability_in_range(
            forecast_temp, parsed["temp_low_c"], parsed["temp_high_c"], std
        )
        edge = fair_prob - yes_price

        results.append({
            "market_id": m["id"],
            "question": question,
            "yes_price": yes_price,
            "fair_probability": round(fair_prob, 4),
            "edge": round(edge, 4),
            "forecast_temp_c": round(forecast_temp, 1),
            "days_ahead": days_ahead,
            "city": parsed["city"],
            "side": "YES" if edge > 0 else "NO",
            "abs_edge": abs(round(edge, 4)),
        })

    # Sort by absolute edge descending
    results.sort(key=lambda x: x["abs_edge"], reverse=True)
    return results
