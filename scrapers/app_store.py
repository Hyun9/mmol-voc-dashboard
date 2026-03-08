"""
Apple App Store review scraper using the official iTunes RSS API.
Endpoint: https://itunes.apple.com/{country}/rss/customerreviews/page={page}/id={app_id}/sortby=mostrecent/json

- No auth required
- Up to 10 pages × 50 reviews = 500 reviews maximum
- Returns real, accurate App Store data
"""

import logging
import time
import requests
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

APP_ID = "369502181"
COUNTRY = "kr"
MAX_PAGES = 10
BASE_URL = "https://itunes.apple.com/{country}/rss/customerreviews/page={page}/id={app_id}/sortby=mostrecent/json"


def _parse_date(date_str: str) -> str:
    """Parse iTunes date format to ISO 8601 UTC string."""
    if not date_str:
        return None
    try:
        # Format: "2026-03-02T13:12:12-07:00"
        dt = datetime.fromisoformat(date_str)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).isoformat()
    except Exception:
        return date_str


def _normalize(entry: dict) -> dict:
    """Normalize an iTunes RSS entry dict to the common review schema."""
    def label(field):
        return (entry.get(field) or {}).get("label") or ""

    review_id = label("id")
    author = (entry.get("author") or {}).get("name", {}).get("label", "익명")
    rating_str = label("im:rating")
    title = label("title")
    body = label("content")
    date_str = label("updated")
    vote_count = label("im:voteCount")
    vote_sum = label("im:voteSum")

    try:
        rating = int(rating_str) if rating_str else None
    except ValueError:
        rating = None

    try:
        thumbs = int(vote_sum) if vote_sum else 0
    except ValueError:
        thumbs = 0

    return {
        "source": "app_store",
        "id": review_id or f"appstore_{hash(body[:50])}",
        "author": author,
        "rating": rating,
        "title": title,
        "body": body,
        "date": _parse_date(date_str),
        "thumbs_up": thumbs,
        "reply": None,
        "link": f"https://apps.apple.com/kr/app/id{APP_ID}",
    }


def _is_review_entry(entry: dict) -> bool:
    """
    App info entries have 'im:price' or no 'im:rating'.
    Skip those; only keep actual review entries.
    """
    return bool((entry.get("im:rating") or {}).get("label")) and "im:price" not in entry


def scrape_app_store(app_id: str = APP_ID, country: str = COUNTRY,
                     max_pages: int = MAX_PAGES) -> list:
    """
    Fetch App Store reviews via iTunes RSS API.
    Returns list of normalized review dicts.
    """
    all_reviews = []
    seen_ids = set()

    logger.info(f"Scraping App Store via iTunes RSS API (app_id={app_id}, max_pages={max_pages})...")

    for page in range(1, max_pages + 1):
        url = BASE_URL.format(country=country, page=page, app_id=app_id)
        try:
            resp = requests.get(
                url,
                headers={"Accept": "application/json"},
                timeout=15,
            )
            resp.raise_for_status()
            data = resp.json()

            entries = data.get("feed", {}).get("entry", [])
            if not entries:
                logger.info(f"App Store: page {page} returned no entries — stopping.")
                break

            reviews_on_page = 0
            for entry in entries:
                if not _is_review_entry(entry):
                    continue
                review = _normalize(entry)
                if review["id"] not in seen_ids and (review["body"] or review["title"]):
                    seen_ids.add(review["id"])
                    all_reviews.append(review)
                    reviews_on_page += 1

            logger.info(f"App Store page {page}: {reviews_on_page} reviews")

            if reviews_on_page == 0:
                break

            # Polite delay between pages
            time.sleep(0.5)

        except requests.HTTPError as e:
            logger.warning(f"App Store page {page} HTTP error: {e}")
            break
        except Exception as e:
            logger.error(f"App Store page {page} error: {e}")
            break

    logger.info(f"App Store total: {len(all_reviews)} reviews collected")
    return all_reviews
