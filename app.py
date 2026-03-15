import json
import logging
import os
import sys
import threading
from datetime import datetime, timezone, timedelta
from pathlib import Path

from dotenv import load_dotenv
from flask import Flask, jsonify, render_template, request

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"
CACHE_FILE = DATA_DIR / "cache.json"
CACHE_META_FILE = DATA_DIR / "cache_meta.json"
CACHE_TTL = int(os.environ.get("CACHE_TTL_SECONDS", 3600))

app = Flask(__name__, template_folder="templates")

scrape_status = {
    "running": False,
    "progress": "",
    "error": None,
    "last_updated": None,
}
scrape_lock = threading.Lock()


# --- Cache helpers ---

def _is_cache_valid() -> bool:
    if not CACHE_META_FILE.exists():
        return False
    try:
        meta = json.loads(CACHE_META_FILE.read_text(encoding="utf-8"))
        updated = datetime.fromisoformat(meta["last_updated"])
        if updated.tzinfo is None:
            updated = updated.replace(tzinfo=timezone.utc)
        age = (datetime.now(timezone.utc) - updated).total_seconds()
        return age < CACHE_TTL
    except Exception:
        return False


def _read_cache() -> dict:
    return json.loads(CACHE_FILE.read_text(encoding="utf-8"))


def _write_cache(data: dict, source_counts: dict, durations: dict, errors: list):
    DATA_DIR.mkdir(exist_ok=True)
    now_str = datetime.now(timezone.utc).isoformat()

    CACHE_FILE.write_text(
        json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    meta = {
        "last_updated": now_str,
        "ttl_seconds": CACHE_TTL,
        "source_counts": source_counts,
        "scrape_durations": durations,
        "errors": errors,
    }
    CACHE_META_FILE.write_text(
        json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    scrape_status["last_updated"] = now_str
    logger.info(f"Cache written: {sum(source_counts.values())} total reviews")


# --- Scraping pipeline ---

def _run_full_scrape():
    try:
        import time
        from scrapers.play_store import scrape_play_store
        from scrapers.app_store import scrape_app_store
        from scrapers.naver import scrape_naver_blogs
        from scrapers.naver_cafe import scrape_naver_cafe
        from scrapers.google_search import scrape_web_snippets
        from processors.aggregator import build_full_dataset
    except Exception as e:
        scrape_status["error"] = f"Import error: {e}"
        scrape_status["running"] = False
        logger.error(f"_run_full_scrape import failed: {e}", exc_info=True)
        return

    with scrape_lock:
        if scrape_status["running"]:
            return
        scrape_status["running"] = True
        scrape_status["error"] = None

    all_reviews = []
    source_counts = {}
    durations = {}
    errors = []

    steps = [
        ("Google Play Store", scrape_play_store, "google_play"),
        ("App Store", scrape_app_store, "app_store"),
        ("네이버 블로그", scrape_naver_blogs, "naver_blog"),
        ("네이버 카페", scrape_naver_cafe, "naver_cafe"),
        ("웹 검색", scrape_web_snippets, "web_snippet"),
    ]

    for i, (name, fn, src_key) in enumerate(steps, 1):
        scrape_status["progress"] = f"{name} 수집 중... ({i}/{len(steps)})"
        t0 = time.time()
        try:
            results = fn()
            elapsed = round(time.time() - t0, 1)
            all_reviews.extend(results)
            source_counts[src_key] = len(results)
            durations[src_key] = elapsed
            logger.info(f"{name}: {len(results)} items in {elapsed}s")
        except Exception as e:
            elapsed = round(time.time() - t0, 1)
            source_counts[src_key] = 0
            durations[src_key] = elapsed
            errors.append(f"{name}: {str(e)}")
            logger.error(f"{name} failed: {e}")

    logger.info(f"전체 리뷰 수집 완료: {len(all_reviews)}건 (날짜 필터 없음)")

    scrape_status["progress"] = "데이터 처리 중..."
    try:
        dataset = build_full_dataset(all_reviews)
        _write_cache(dataset, source_counts, durations, errors)
    except Exception as e:
        scrape_status["error"] = str(e)
        logger.error(f"Processing failed: {e}")
    finally:
        scrape_status["running"] = False
        scrape_status["progress"] = "완료"


def _start_scrape_thread():
    t = threading.Thread(target=_run_full_scrape, daemon=True)
    t.start()


# --- Routes ---

@app.route("/")
def dashboard():
    return render_template("dashboard.html")


@app.route("/api/data")
def get_data():
    force = request.args.get("force", "false").lower() == "true"
    if force and not _is_cache_valid() and not scrape_status["running"]:
        _start_scrape_thread()
        return jsonify({"status": "refreshing", "message": "스크래핑을 시작했습니다."})

    if not CACHE_FILE.exists():
        if not scrape_status["running"]:
            _start_scrape_thread()
        return jsonify({"status": "loading", "message": "첫 번째 데이터 수집 중입니다. 잠시 후 다시 시도해 주세요."})

    data = _read_cache()
    if CACHE_META_FILE.exists():
        meta = json.loads(CACHE_META_FILE.read_text(encoding="utf-8"))
        data["cache_meta"] = meta
    return jsonify(data)


@app.route("/api/refresh", methods=["POST"])
def refresh_data():
    if scrape_status["running"]:
        return jsonify({"status": "already_running", "message": "이미 수집 중입니다."})
    _start_scrape_thread()
    return jsonify({"status": "started", "message": "데이터 수집을 시작했습니다."})


@app.route("/api/status")
def get_status():
    cache_age = None
    total_reviews = 0

    if CACHE_META_FILE.exists():
        try:
            meta = json.loads(CACHE_META_FILE.read_text(encoding="utf-8"))
            updated = datetime.fromisoformat(meta["last_updated"])
            if updated.tzinfo is None:
                updated = updated.replace(tzinfo=timezone.utc)
            cache_age = int((datetime.now(timezone.utc) - updated).total_seconds())
            total_reviews = sum(meta.get("source_counts", {}).values())
        except Exception:
            pass

    return jsonify({
        "running": scrape_status["running"],
        "progress": scrape_status["progress"],
        "last_updated": scrape_status.get("last_updated"),
        "cache_age_seconds": cache_age,
        "total_reviews": total_reviews,
        "error": scrape_status.get("error"),
    })


@app.route("/api/reviews")
def get_reviews():
    if not CACHE_FILE.exists():
        return jsonify({"reviews": [], "total": 0, "page": 1, "pages": 0})

    data = _read_cache()
    reviews = data.get("reviews", [])

    source_f = request.args.get("source")
    category_f = request.args.get("category")
    sentiment_f = request.args.get("sentiment")
    rating_f = request.args.get("rating")
    sort_f = request.args.get("sort", "priority")
    q_f = request.args.get("q", "").strip().lower()
    page = max(int(request.args.get("page", 1)), 1)
    per_page = 20

    if source_f:
        reviews = [r for r in reviews if r.get("source") == source_f]
    if category_f:
        reviews = [r for r in reviews if r.get("category") == category_f]
    if sentiment_f:
        reviews = [r for r in reviews if r.get("sentiment") == sentiment_f]
    if rating_f:
        try:
            rv = int(rating_f)
            reviews = [r for r in reviews if r.get("rating") == rv]
        except ValueError:
            pass
    if q_f:
        reviews = [
            r for r in reviews
            if q_f in (r.get("body") or "").lower()
            or q_f in (r.get("title") or "").lower()
        ]

    if sort_f == "date":
        reviews = sorted(
            reviews,
            key=lambda r: r.get("date") or "",
            reverse=True,
        )
    elif sort_f == "rating":
        reviews = sorted(
            reviews,
            key=lambda r: r.get("rating") or 0,
            reverse=True,
        )
    else:
        reviews = sorted(
            reviews,
            key=lambda r: r.get("priority_score_raw", 0),
            reverse=True,
        )

    total = len(reviews)
    pages = max(1, (total + per_page - 1) // per_page)
    page = min(page, pages)
    start = (page - 1) * per_page
    page_reviews = reviews[start: start + per_page]

    return jsonify({
        "reviews": page_reviews,
        "total": total,
        "page": page,
        "pages": pages,
    })


@app.route("/api/categories")
def get_categories():
    if not CACHE_FILE.exists():
        return jsonify({"categories": []})

    data = _read_cache()
    by_cat = data.get("summary", {}).get("by_category", {})

    cats = []
    for name, info in by_cat.items():
        cats.append({
            "name": name,
            "count": info.get("count", 0),
            "avg_priority": info.get("avg_priority", 0),
            "negative_pct": info.get("negative_pct", 0),
            "top_issues": info.get("top_issues", []),
            "priority_rank": info.get("priority_rank", 999),
        })

    cats.sort(key=lambda x: x["priority_rank"])
    return jsonify({"categories": cats})


def _fetch_url_text(url: str) -> str:
    """URL 페이지를 크롤링해서 본문 텍스트를 추출한다."""
    import requests as req
    from bs4 import BeautifulSoup

    if not url:
        return ""

    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/121.0.0.0 Safari/537.36",
        "Accept-Language": "ko-KR,ko;q=0.9",
        "Referer": "https://www.naver.com/",
    }

    try:
        # 네이버 블로그: 모바일 버전이 파싱 쉬움
        fetch_url = url
        if "blog.naver.com" in url:
            # /PostView.naver 형태로 변환
            import re as _re
            m = _re.search(r"blog\.naver\.com/([^/?#]+)/(\d+)", url)
            if m:
                fetch_url = f"https://blog.naver.com/PostView.naver?blogId={m.group(1)}&logNo={m.group(2)}&redirect=Dlog&widgetTypeCall=true"

        resp = req.get(fetch_url, headers=headers, timeout=12)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        # 불필요한 태그 제거
        for tag in soup(["script", "style", "nav", "header", "footer", "aside", "iframe"]):
            tag.decompose()

        # 네이버 블로그 본문 셀렉터 (우선순위 순)
        naver_selectors = [
            ".se-main-container",   # Smart Editor 3 (최신)
            "#postViewArea",        # 구버전
            ".post-view",
            ".post_ct",
            "#post-view",
            ".se_component_wrap",
        ]
        # 일반 사이트 셀렉터
        general_selectors = [
            "article", "main",
            ".article-body", ".post-body",
            ".content", "#content",
            ".entry-content", ".blog-content",
        ]

        all_selectors = naver_selectors + general_selectors
        for selector in all_selectors:
            el = soup.select_one(selector)
            if el:
                text = el.get_text(separator="\n", strip=True)
                if len(text) > 100:
                    return text[:4000]

        # 최후 수단: 전체 텍스트
        text = soup.get_text(separator="\n", strip=True)
        return text[:4000]

    except Exception as e:
        logger.warning(f"URL 크롤링 실패 ({url}): {e}")
        return ""



@app.route("/api/page-title", methods=["POST"])
def get_page_title():
    import re as _re
    import requests as req
    from bs4 import BeautifulSoup
    data = request.json or {}
    url = (data.get("url") or "").strip()
    if not url:
        return jsonify({"title": ""})
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
        "Accept-Language": "ko-KR,ko;q=0.9",
        "Referer": "https://www.naver.com/",
    }
    try:
        fetch_url = url
        is_naver = False
        m = _re.search(r"blog\.naver\.com/([^/?#]+)/(\d+)", url)
        if m:
            is_naver = True
            fetch_url = (f"https://blog.naver.com/PostView.naver"
                         f"?blogId={m.group(1)}&logNo={m.group(2)}&redirect=Dlog&widgetTypeCall=true")
        resp = req.get(fetch_url, headers=headers, timeout=8)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        if is_naver:
            for sel in [".se-title-text", ".pcol1", "h3.title", ".itemSubjectBoldfont"]:
                el = soup.select_one(sel)
                if el:
                    t = el.get_text(strip=True)
                    if t:
                        return jsonify({"title": t})
        og = soup.find("meta", property="og:title")
        if og and og.get("content"):
            return jsonify({"title": og["content"].strip()})
        t = soup.find("title")
        if t:
            return jsonify({"title": t.get_text(strip=True)})
    except Exception as e:
        logger.warning(f"page-title fetch 실패 ({url}): {e}")
    return jsonify({"title": ""})


@app.route("/api/summarize", methods=["POST"])
def summarize_review():
    body = request.json or {}
    url = (body.get("url") or "").strip()
    fallback_text = (body.get("text") or "").strip()

    # URL 크롤링 시도
    text = _fetch_url_text(url) if url else ""
    if not text:
        text = fallback_text
    if not text:
        return jsonify({"summary": ""})

    api_key = os.environ.get("GEMINI_API_KEY", "")
    if api_key:
        try:
            from google import genai
            client = genai.Client(api_key=api_key)
            response = client.models.generate_content(
                model="gemini-2.0-flash",
                contents=f"다음 웹페이지 내용을 한국어로 4줄 이내로 핵심만 요약해줘. 불필요한 설명 없이 요약문만 출력해:\n\n{text[:3000]}"
            )
            summary = response.text.strip()
            return jsonify({"summary": summary})
        except Exception as e:
            logger.warning(f"Gemini summarize failed: {e}")

    # Fallback: 첫 2문장 추출
    import re
    sentences = re.split(r'(?<=[.!?。\n])\s+', text.strip())
    sentences = [s.strip() for s in sentences if len(s.strip()) > 10]
    summary = " ".join(sentences[:2]) if sentences else text[:150]
    if len(summary) > 150:
        summary = summary[:150].rsplit(" ", 1)[0] + "…"
    return jsonify({"summary": summary})


@app.route("/api/trends")
def get_trends():
    if not CACHE_FILE.exists():
        return jsonify({"labels": [], "series": {}})

    data = _read_cache()
    trend = data.get("summary", {}).get("trend_data", {})
    return jsonify(trend)


if __name__ == "__main__":
    # Auto-scrape on startup if no cache
    if not CACHE_FILE.exists():
        logger.info("No cache found — starting initial scrape...")
        _start_scrape_thread()
    else:
        logger.info("Cache found — using existing data. Use /api/refresh to update.")

    port = int(os.environ.get("PORT", 5000))
    app.run(debug=True, host="0.0.0.0", port=port, threaded=True, use_reloader=False)
