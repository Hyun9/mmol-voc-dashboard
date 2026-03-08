import json
import logging
import os
import sys
import threading
from datetime import datetime, timezone
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
    import time
    from scrapers.play_store import scrape_play_store
    from scrapers.app_store import scrape_app_store
    from scrapers.naver import scrape_naver_blogs
    from scrapers.google_search import scrape_web_snippets
    from processors.aggregator import build_full_dataset

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
    app.run(debug=False, host="0.0.0.0", port=port, threaded=True)
