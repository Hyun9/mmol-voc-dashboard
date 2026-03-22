import os
import re
import time
import random
import hashlib
import logging
import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta
from urllib.parse import quote

logger = logging.getLogger(__name__)

NAVER_API_URL = "https://openapi.naver.com/v1/search/blog.json"
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

# 실제 M몰 구매 확인을 위한 컨텍스트 패턴
M_MALL_SIGNALS = [
    "M포인트몰", "엠포인트몰", "M포인트 몰",
    "M몰", "엠몰", "현대카드 M몰", "현대카드몰",
    "M포인트로", "엠포인트로", "현대카드 포인트로",
    "M포인트 사용", "엠포인트 사용",
]
PURCHASE_VERBS = ["구매", "샀", "결제", "주문", "사용했", "사용해서", "써서", "썼"]


def _is_mmall_purchase(text: str) -> bool:
    """M몰에서 실제 구매한 내용인지 확인"""
    has_signal = any(s in text for s in M_MALL_SIGNALS)
    has_verb   = any(v in text for v in PURCHASE_VERBS)
    return has_signal and has_verb

USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
]


def _strip_html(text: str) -> str:
    return re.sub(r"<[^>]+>", "", text or "").strip()


def _to_postview_url(url: str) -> str:
    """blog.naver.com/user/postid → PostView URL로 변환"""
    m = re.search(r"blog\.naver\.com/([^/?#]+)/(\d+)", url)
    if m:
        return (f"https://blog.naver.com/PostView.naver"
                f"?blogId={m.group(1)}&logNo={m.group(2)}"
                f"&redirect=Dlog&widgetTypeCall=true")
    return url


def _fetch_full_body(url: str) -> str:
    """네이버 블로그 포스트 전체 본문 크롤링 (최대 800자)"""
    try:
        fetch_url = _to_postview_url(url)
        headers = {
            "User-Agent": random.choice(USER_AGENTS),
            "Accept-Language": "ko-KR,ko;q=0.9",
            "Referer": "https://blog.naver.com/",
        }
        resp = requests.get(fetch_url, headers=headers, timeout=8)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "lxml")
        for selector in [".se-main-container", "#postViewArea",
                         ".post-content", ".se_doc_viewer", "#post-view"]:
            el = soup.select_one(selector)
            if el:
                text = re.sub(r"\s+", " ", el.get_text(separator=" ", strip=True)).strip()
                if len(text) > 50:
                    return text[:800]
    except Exception as e:
        logger.debug(f"fetch_full_body failed for {url}: {e}")
    return ""


def _enrich_with_full_body(results: list, max_workers: int = 8) -> list:
    """ThreadPoolExecutor로 전체 본문 병렬 크롤링"""
    logger.info(f"Crawling full body for {len(results)} naver blog posts...")

    def worker(r: dict) -> dict:
        body = _fetch_full_body(r.get("link", ""))
        if body:
            r["body"] = body
        return r

    enriched = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(worker, r): r for r in results}
        for future in as_completed(futures):
            try:
                enriched.append(future.result())
            except Exception:
                enriched.append(futures[future])

    ok = sum(1 for r in enriched if len(r.get("body", "")) > 100)
    logger.info(f"Body enrichment complete: {ok}/{len(enriched)} posts enriched")
    return enriched


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
        cutoff = datetime.now(timezone.utc) - timedelta(days=365)
        before = len(results)
        results = [r for r in results if r.get("date") and r["date"] >= cutoff.isoformat()]
        logger.info(f"Naver Blog (12mo filter): {len(results)}/{before} kept")
        # 제목으로 M몰 신호 사전 필터 (크롤링 전)
        before = len(results)
        title_filtered = [r for r in results if any(s in r.get("title","") for s in M_MALL_SIGNALS)]
        # 제목 필터 후 남은 게 너무 적으면 전체에서 최대 200개만 크롤링
        crawl_targets = title_filtered if len(title_filtered) >= 20 else results[:200]
        logger.info(f"Naver Blog (pre-filter): {len(crawl_targets)} posts to crawl (from {before})")
        crawl_targets = _enrich_with_full_body(crawl_targets)
        # M몰 실제 구매 확인 필터 (본문 크롤링 후 적용)
        before = len(crawl_targets)
        results = [r for r in crawl_targets
                   if _is_mmall_purchase((r.get("title","") + " " + r.get("body","")))]
        logger.info(f"Naver Blog (purchase filter): {len(results)}/{before} kept")
        return results
    else:
        logger.info("Naver API key not set — using fallback scraper...")
        results = _scrape_fallback(queries)
        logger.info(f"Naver Blog (fallback): {len(results)} posts collected")
        return results
