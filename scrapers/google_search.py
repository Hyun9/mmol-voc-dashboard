import os
import re
import time
import hashlib
import logging
import requests
from datetime import datetime, timezone, timedelta
from email.utils import parsedate_to_datetime

logger = logging.getLogger(__name__)

NAVER_WEBKR_URL = "https://openapi.naver.com/v1/search/webkr.json"

QUERIES = [
    "현대카드 M몰 앱 후기",
    "현대카드 엠몰 리뷰",
    "현대카드 M몰 불편",
    "현대카드 M몰 오류",
    "현대카드 엠몰 사용후기",
    "M몰 구매 후기",
    "엠몰 상품 추천",
    "M포인트몰 후기",
    "엠포인트몰 쇼핑",
    "현대카드 M몰 구매 상품",
]


def _strip_html(text: str) -> str:
    return re.sub(r"<[^>]+>", "", text or "").strip()


def _make_id(text: str) -> str:
    return hashlib.md5(text.encode()).hexdigest()[:16]


def _parse_pub_date(date_str: str) -> str:
    """네이버 웹문서 API pubDate (RFC 2822) → ISO 8601"""
    try:
        dt = parsedate_to_datetime(date_str)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.isoformat()
    except Exception:
        return None


def scrape_web_snippets(queries: list = None, display: int = 20) -> list:
    if queries is None:
        queries = QUERIES

    client_id = os.environ.get("NAVER_CLIENT_ID", "")
    client_secret = os.environ.get("NAVER_CLIENT_SECRET", "")

    if not client_id or not client_secret or client_id.startswith("your_"):
        logger.warning("NAVER_CLIENT_ID or NAVER_CLIENT_SECRET not set — skipping web snippets")
        return []

    headers = {
        "X-Naver-Client-Id": client_id,
        "X-Naver-Client-Secret": client_secret,
    }

    results = []
    seen = set()
    logger.info("Scraping web snippets via Naver 웹문서 API...")

    for query in queries:
        try:
            params = {"query": query, "display": display, "sort": "date"}
            resp = requests.get(NAVER_WEBKR_URL, headers=headers, params=params, timeout=10)
            resp.raise_for_status()
            for item in resp.json().get("items", []):
                link = item.get("link", "")
                title = _strip_html(item.get("title", ""))
                body = _strip_html(item.get("description", ""))
                uid = _make_id(link or title)
                is_blog = "blog.naver.com" in link
                if uid not in seen and (title or body) and not is_blog:
                    seen.add(uid)
                    results.append({
                        "source": "web_snippet",
                        "id": uid,
                        "author": "",
                        "rating": None,
                        "title": title,
                        "body": body,
                        "date": _parse_pub_date(item.get("pubDate", "")),
                        "thumbs_up": 0,
                        "reply": None,
                        "link": link,
                    })
            time.sleep(0.3)
        except Exception as e:
            logger.warning(f"Naver 웹문서 API error for '{query}': {e}")

    logger.info(f"Web snippets (Naver 웹문서): {len(results)} collected")
    cutoff = datetime.now(timezone.utc) - timedelta(days=365)
    before = len(results)
    results = [r for r in results if not r.get("date") or r["date"] >= cutoff.isoformat()]
    logger.info(f"Web snippets (12mo filter): {len(results)}/{before} kept")
    return results
