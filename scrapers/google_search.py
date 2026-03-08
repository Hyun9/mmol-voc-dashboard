import time
import random
import hashlib
import logging
import requests
from bs4 import BeautifulSoup
from urllib.parse import quote

logger = logging.getLogger(__name__)

QUERIES = [
    "현대카드 M몰 앱 리뷰",
    "현대카드 엠몰 후기",
    "엠포인트몰 후기",
    "M포인트몰 후기",
    "현대카드 M몰 불편",
]

USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Mobile/15E148 Safari/604.1",
]


def _make_id(text: str) -> str:
    return hashlib.md5(text.encode()).hexdigest()[:16]


def _scrape_duckduckgo(query: str, max_results: int = 5) -> list:
    url = "https://html.duckduckgo.com/html/"
    data = {"q": query, "kl": "kr-kr"}
    headers = {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept-Language": "ko-KR,ko;q=0.9",
        "Content-Type": "application/x-www-form-urlencoded",
        "Referer": "https://duckduckgo.com/",
    }

    results = []
    try:
        resp = requests.post(url, data=data, headers=headers, timeout=15)
        soup = BeautifulSoup(resp.text, "lxml")

        for result in soup.select(".result__body")[:max_results]:
            title_el = result.select_one(".result__title")
            snippet_el = result.select_one(".result__snippet")
            link_el = result.select_one(".result__url")

            title = title_el.get_text(strip=True) if title_el else ""
            snippet = snippet_el.get_text(strip=True) if snippet_el else ""
            link = link_el.get_text(strip=True) if link_el else ""

            if not (title or snippet):
                continue

            uid = _make_id(link or title or snippet)
            results.append({
                "source": "web_snippet",
                "id": uid,
                "author": "",
                "rating": None,
                "title": title,
                "body": snippet,
                "date": None,
                "thumbs_up": 0,
                "reply": None,
                "link": link,
            })
    except Exception as e:
        logger.warning(f"DuckDuckGo scrape error for '{query}': {e}")

    return results


def scrape_web_snippets(queries: list = None, max_per_query: int = 5) -> list:
    if queries is None:
        queries = QUERIES

    results = []
    seen = set()
    logger.info("Scraping web snippets via DuckDuckGo...")

    for query in queries:
        items = _scrape_duckduckgo(query, max_results=max_per_query)
        for item in items:
            if item["id"] not in seen:
                seen.add(item["id"])
                results.append(item)
        time.sleep(random.uniform(1.5, 3.0))

    logger.info(f"Web snippets: {len(results)} collected")
    return results
