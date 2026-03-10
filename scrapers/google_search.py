import os
import hashlib
import logging
import requests

logger = logging.getLogger(__name__)

QUERIES = [
    "현대카드 M몰 앱 리뷰",
    "현대카드 엠몰 후기",
    "엠포인트몰 후기",
    "M포인트몰 후기",
    "현대카드 M몰 불편",
]

CSE_ENDPOINT = "https://www.googleapis.com/customsearch/v1"


def _make_id(text: str) -> str:
    return hashlib.md5(text.encode()).hexdigest()[:16]


def _scrape_google_cse(query: str, api_key: str, cx: str, num: int = 10) -> list:
    results = []
    try:
        resp = requests.get(
            CSE_ENDPOINT,
            params={"key": api_key, "cx": cx, "q": query, "num": num},
            timeout=15,
        )
        resp.raise_for_status()
        for item in resp.json().get("items", []):
            link = item.get("link", "")
            title = item.get("title", "")
            snippet = item.get("snippet", "")
            uid = _make_id(link or title or snippet)
            results.append({
                "source": "web_snippet",
                "id": uid,
                "author": item.get("displayLink", ""),
                "rating": None,
                "title": title,
                "body": snippet,
                "date": None,
                "thumbs_up": 0,
                "reply": None,
                "link": link,
            })
    except Exception as e:
        logger.warning(f"Google CSE error for '{query}': {e}")
    return results


def scrape_web_snippets(queries: list = None, max_per_query: int = 10) -> list:
    if queries is None:
        queries = QUERIES

    api_key = os.environ.get("GOOGLE_CSE_API_KEY", "")
    cx = os.environ.get("GOOGLE_CSE_CX", "")

    if not api_key or not cx:
        logger.warning("GOOGLE_CSE_API_KEY or GOOGLE_CSE_CX not set — skipping web snippets")
        return []

    results = []
    seen = set()
    logger.info("Scraping web snippets via Google Custom Search API...")

    for query in queries:
        items = _scrape_google_cse(query, api_key, cx, num=max_per_query)
        for item in items:
            if item["id"] not in seen:
                seen.add(item["id"])
                results.append(item)

    logger.info(f"Web snippets: {len(results)} collected")
    return results
