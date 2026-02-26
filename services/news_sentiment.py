"""
News sentiment analysis service.
Uses NewsAPI to fetch relevant articles and estimate sentiment scores.
"""

import logging
from datetime import datetime, timedelta
from typing import Optional

from config import AppConfig

logger = logging.getLogger(__name__)


class NewsSentimentService:
    """Fetch news and estimate sentiment for market topics."""

    def __init__(self, config: AppConfig):
        self.api_key = config.newsapi_key
        self._client = None
        if self.api_key:
            try:
                from newsapi import NewsApiClient
                self._client = NewsApiClient(api_key=self.api_key)
            except ImportError:
                logger.warning("newsapi-python not installed.")

    def get_sentiment(self, query: str, days_back: int = 3) -> dict:
        """
        Get sentiment score for a search query.
        Returns: {"score": float (-1 to 1), "article_count": int, "headlines": list[str]}
        """
        if not self._client:
            return {"score": 0.0, "article_count": 0, "headlines": []}

        try:
            from_date = (datetime.utcnow() - timedelta(days=days_back)).strftime("%Y-%m-%d")
            response = self._client.get_everything(
                q=query,
                from_param=from_date,
                language="en",
                sort_by="relevancy",
                page_size=20,
            )

            articles = response.get("articles", [])
            headlines = [a.get("title", "") for a in articles if a.get("title")]

            # Simple keyword-based sentiment (placeholder for ML sentiment)
            score = self._simple_sentiment(headlines)

            return {
                "score": score,
                "article_count": len(articles),
                "headlines": headlines[:10],
            }

        except Exception as e:
            logger.error(f"Sentiment fetch failed for '{query}': {e}")
            return {"score": 0.0, "article_count": 0, "headlines": []}

    @staticmethod
    def _simple_sentiment(headlines: list[str]) -> float:
        """
        Simple keyword-based sentiment scoring.
        Returns a value between -1.0 (very negative) and 1.0 (very positive).
        """
        positive_words = {
            "surge", "gain", "rise", "bull", "high", "record", "success",
            "win", "approve", "positive", "growth", "boost", "rally",
        }
        negative_words = {
            "crash", "fall", "drop", "bear", "low", "fail", "loss",
            "decline", "reject", "negative", "risk", "crisis", "collapse",
        }

        pos_count = 0
        neg_count = 0

        for headline in headlines:
            words = headline.lower().split()
            pos_count += sum(1 for w in words if w in positive_words)
            neg_count += sum(1 for w in words if w in negative_words)

        total = pos_count + neg_count
        if total == 0:
            return 0.0

        return round((pos_count - neg_count) / total, 2)
