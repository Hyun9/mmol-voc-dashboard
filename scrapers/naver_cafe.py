import os
import re
import time
import hashlib
import logging
import requests
from datetime import datetime, timezone, timedelta

logger = logging.getLogger(__name__)

NAVER_API_URL = "https://openapi.naver.com/v1/search/cafearticle.json"
QUERIES = [
    "M포인트몰 구매 후기",
    "엠포인트몰 내돈내산",
    "엠포인트몰 후기",
    "현대카드 M몰 후기",
    "현대카드 M포인트 구매 후기",
    "M포인트 사용 후기",
    "M몰 내돈내산",
    "현대카드포인트 쇼핑 후기",
    "M포인트몰 솔직후기",
    "현대카드몰 구매후기",
]

M_MALL_SIGNALS = [
    "M포인트몰", "엠포인트몰", "M포인트 몰",
    "M몰", "엠몰", "현대카드 M몰", "현대카드몰",
    "M포인트로", "엠포인트로", "현대카드 포인트로",
    "M포인트 사용", "엠포인트 사용",
]
PURCHASE_VERBS = ["구매", "샀", "결제", "주문", "사용했", "사용해서", "써서", "썼"]


def _is_mmall_purchase(text: str) -> bool:
    has_signal = any(s in text for s in M_MALL_SIGNALS)
    has_verb   = any(v in text for v in PURCHASE_VERBS)
    return has_signal and has_verb


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
                    queries: list = None, display: int = 100) -> list:
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
        cutoff = datetime.now(timezone.utc) - timedelta(days=365)
        before = len(results)
        results = [r for r in results if not r.get("date") or r["date"] >= cutoff.isoformat()]
        logger.info(f"Naver Cafe (12mo filter): {len(results)}/{before} kept")
        # M몰 실제 구매 확인 필터
        before = len(results)
        results = [r for r in results
                   if _is_mmall_purchase((r.get("title","") + " " + r.get("body","")))]
        logger.info(f"Naver Cafe (purchase filter): {len(results)}/{before} kept")
        return results
    else:
        logger.warning("Naver API key not set — skipping Naver Cafe scraping.")
        return []
