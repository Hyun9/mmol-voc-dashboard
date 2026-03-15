import os
import re
import time
import hashlib
import logging
import requests
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

NAVER_API_URL = "https://openapi.naver.com/v1/search/cafearticle.json"
QUERIES = ["현대카드 M몰", "현대카드 엠몰", "현대카드M몰 후기", "현대카드엠몰 앱"]


def _strip_html(text: str) -> str:
    return re.sub(r"<[^>]+>", "", text or "").strip()


def _parse_naver_date(date_str: str) -> str:
    try:
        dt = datetime.strptime(date_str, "%Y%m%d").replace(tzinfo=timezone.utc)
        return dt.isoformat()
    except Exception:
        return date_str


def _make_id(url: str) -> str:
    return hashlib.md5(url.encode()).hexdigest()[:16]


def _normalize_api(item: dict) -> dict:
    body = _strip_html(item.get("description", ""))
    title = _strip_html(item.get("title", ""))
    link = item.get("link", "")
    return {
        "source": "naver_cafe",
        "id": _make_id(link),
        "author": item.get("cafename", ""),
        "rating": None,
        "title": title,
        "body": body,
        "date": _parse_naver_date(item.get("postdate", "")),
        "thumbs_up": 0,
        "reply": None,
        "link": link,
    }


def _scrape_via_api(client_id: str, client_secret: str,
                    queries: list = None, display: int = 20) -> list:
    if queries is None:
        queries = QUERIES
    results = []
    seen = set()
    headers = {
        "X-Naver-Client-Id": client_id,
        "X-Naver-Client-Secret": client_secret,
    }
    for query in queries:
        try:
            params = {"query": query, "display": display, "sort": "date"}
            resp = requests.get(NAVER_API_URL, headers=headers,
                                params=params, timeout=10)
            resp.raise_for_status()
            data = resp.json()
            for item in data.get("items", []):
                norm = _normalize_api(item)
                if norm["id"] not in seen:
                    seen.add(norm["id"])
                    results.append(norm)
            time.sleep(0.5)
        except Exception as e:
            logger.warning(f"Naver Cafe API error for query '{query}': {e}")
    return results


def scrape_naver_cafe(client_id: str = None, client_secret: str = None,
                      queries: list = None) -> list:
    cid = client_id or os.environ.get("NAVER_CLIENT_ID", "")
    csecret = client_secret or os.environ.get("NAVER_CLIENT_SECRET", "")

    if cid and csecret and not cid.startswith("your_"):
        logger.info("Scraping Naver Cafe via Official API...")
        results = _scrape_via_api(cid, csecret, queries)
        logger.info(f"Naver Cafe (API): {len(results)} posts collected")
        return results
    else:
        logger.warning("Naver API key not set — skipping Naver Cafe scraping.")
        return []
