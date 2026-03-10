import time
import logging
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

APP_ID = "com.hyundaiCard.HyundaiCardMPoint"


def _normalize(r: dict) -> dict:
    date_val = r.get("at")
    if isinstance(date_val, datetime):
        if date_val.tzinfo is None:
            date_val = date_val.replace(tzinfo=timezone.utc)
        date_str = date_val.isoformat()
    else:
        date_str = str(date_val) if date_val else None

    return {
        "source": "google_play",
        "id": r.get("reviewId", ""),
        "author": r.get("userName", "익명"),
        "rating": r.get("score"),
        "title": "",
        "body": r.get("content", ""),
        "date": date_str,
        "thumbs_up": r.get("thumbsUpCount", 0),
        "reply": r.get("replyContent"),
        "link": f"https://play.google.com/store/apps/details?id={APP_ID}",
    }


def scrape_play_store(app_id: str = APP_ID, lang: str = "ko",
                      country: str = "kr", count: int = 500) -> list:
    try:
        from google_play_scraper import reviews, Sort
    except ImportError:
        logger.error("google-play-scraper not installed")
        return []

    try:
        logger.info(f"Scraping Google Play Store (count={count})...")
        result, _ = reviews(
            app_id,
            lang=lang,
            country=country,
            sort=Sort.NEWEST,
            count=count,
        )
        normalized = [_normalize(r) for r in result]
        logger.info(f"Google Play: {len(normalized)} reviews collected")
        return normalized
    except Exception as e:
        logger.error(f"Google Play scraping error: {e}")
        return []
