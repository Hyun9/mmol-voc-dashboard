import os
import re
import time
import random
import hashlib
import logging
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timezone
from urllib.parse import quote

logger = logging.getLogger(__name__)

NAVER_API_URL = "https://openapi.naver.com/v1/search/blog.json"
QUERIES = ["현대카드 M몰", "현대카드 엠몰", "현대카드M몰 후기", "현대카드엠몰 앱"]

USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
]


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
        "source": "naver_blog",
        "id": _make_id(link),
        "author": item.get("bloggername", ""),
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
            logger.warning(f"Naver API error for query '{query}': {e}")
    return results


def _scrape_fallback(queries: list = None) -> list:
    if queries is None:
        queries = QUERIES
    results = []
    seen = set()
    for query in queries:
        try:
            url = f"https://search.naver.com/search.naver?where=blog&query={quote(query)}&sort=1"
            headers = {
                "User-Agent": random.choice(USER_AGENTS),
                "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8",
                "Accept": "text/html,application/xhtml+xml",
                "Referer": "https://www.naver.com/",
            }
            time.sleep(random.uniform(2.5, 4.5))
            resp = requests.get(url, headers=headers, timeout=15)
            soup = BeautifulSoup(resp.text, "lxml")

            for item in soup.select(".api_txt_lines.total_tit, .title_link"):
                title_el = item if item.name == "a" else item.find("a")
                if not title_el:
                    continue
                title = title_el.get_text(strip=True)
                link = title_el.get("href", "")
                desc_el = item.find_next(".api_txt_lines.dsc_txt")
                body = desc_el.get_text(strip=True) if desc_el else ""
                uid = _make_id(link or title)
                if uid not in seen and (title or body):
                    seen.add(uid)
                    results.append({
                        "source": "naver_blog",
                        "id": uid,
                        "author": "",
                        "rating": None,
                        "title": title,
                        "body": body,
                        "date": None,
                        "thumbs_up": 0,
                        "reply": None,
                        "link": link,
                    })
        except Exception as e:
            logger.warning(f"Naver fallback scrape error for '{query}': {e}")
    return results


def scrape_naver_blogs(client_id: str = None, client_secret: str = None,
                       queries: list = None) -> list:
    cid = client_id or os.environ.get("NAVER_CLIENT_ID", "")
    csecret = client_secret or os.environ.get("NAVER_CLIENT_SECRET", "")

    if cid and csecret and not cid.startswith("your_"):
        logger.info("Scraping Naver blogs via Official API...")
        results = _scrape_via_api(cid, csecret, queries)
        logger.info(f"Naver Blog (API): {len(results)} posts collected")
        return results
    else:
        logger.info("Naver API key not set — using fallback scraper...")
        results = _scrape_fallback(queries)
        logger.info(f"Naver Blog (fallback): {len(results)} posts collected")
        return results
