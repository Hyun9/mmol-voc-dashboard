import logging
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

SOURCE_WEIGHTS = {
    "google_play": 1.0,
    "app_store": 1.0,
    "naver_blog": 0.7,
    "web_snippet": 0.5,
}

RATING_BASE = {
    1: 1.00,
    2: 0.75,
    3: 0.50,
    4: 0.25,
    5: 0.10,
}

MAX_SCORE = 2.7  # used for normalization to 0-100


def _days_old(date_str: str) -> float:
    if not date_str:
        return 365.0
    try:
        dt = datetime.fromisoformat(date_str)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        delta = datetime.now(timezone.utc) - dt
        return max(delta.days, 0)
    except Exception:
        return 365.0


def _recency_multiplier(days: float) -> float:
    if days < 14:
        return 1.8
    elif days < 30:
        return 1.5
    elif days < 90:
        return 1.2
    elif days < 180:
        return 1.0
    else:
        return 0.6


def calculate_priority_score(review: dict) -> float:
    """
    Returns priority score 0.0 - MAX_SCORE.
    Higher = more urgently needs attention.
    """
    rating = review.get("rating")
    base = RATING_BASE.get(rating, 0.50)

    days = _days_old(review.get("date"))
    recency = _recency_multiplier(days)

    source = review.get("source", "web_snippet")
    weight = SOURCE_WEIGHTS.get(source, 0.5)

    thumbs = review.get("thumbs_up", 0) or 0
    engagement_bonus = min(thumbs / 50.0, 0.4)

    confidence = review.get("category_confidence", 0.0) or 0.0
    confidence_bonus = confidence * 0.3

    raw_score = base * recency * weight + engagement_bonus + confidence_bonus
    return round(raw_score, 4)


def normalize_score(raw: float) -> int:
    """Converts raw priority score to 0-100 integer."""
    return min(100, int(round((raw / MAX_SCORE) * 100)))


def get_sentiment_label(rating) -> str:
    if rating is None:
        return "neutral"
    if rating >= 4:
        return "positive"
    elif rating == 3:
        return "neutral"
    else:
        return "negative"


def get_priority_level(normalized_score: int) -> str:
    if normalized_score >= 75:
        return "긴급"
    elif normalized_score >= 50:
        return "높음"
    elif normalized_score >= 25:
        return "중간"
    else:
        return "낮음"
