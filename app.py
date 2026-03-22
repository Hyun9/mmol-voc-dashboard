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
KEYWORDS_FILE = DATA_DIR / "keywords.json"
CACHE_TTL = int(os.environ.get("CACHE_TTL_SECONDS", 3600))

app = Flask(__name__, template_folder="templates", static_folder="static")

scrape_status = {
    "running": False,
    "progress": "",
    "error": None,
    "last_updated": None,
}
scrape_lock = threading.Lock()

# ── Kiwi 전역 초기화 (백그라운드, 한 번만) ─────────────────────────────────
_kiwi = None
_kiwi_ready = False

def _init_kiwi():
    global _kiwi, _kiwi_ready
    try:
        from kiwipiepy import Kiwi
        import processors.aggregator as _agg
        _kiwi = Kiwi()
        _kiwi_ready = True
        _agg._kiwi = _kiwi  # aggregator에 kiwi 인스턴스 공유
        logger.info("Kiwi 형태소 분석기 초기화 완료")
    except Exception as e:
        logger.warning(f"Kiwi 초기화 실패: {e}")

threading.Thread(target=_init_kiwi, daemon=True).start()


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

    cutoff_1y = (datetime.now(timezone.utc) - timedelta(days=365)).isoformat()

    for i, (name, fn, src_key) in enumerate(steps, 1):
        scrape_status["progress"] = f"{name} 수집 중... ({i}/{len(steps)})"
        t0 = time.time()
        try:
            results = fn()
            # 스토어 리뷰 1년 필터
            if src_key in ("app_store", "google_play"):
                before = len(results)
                results = [r for r in results if r.get("date") and r["date"] >= cutoff_1y]
                logger.info(f"{name} (1년 필터): {len(results)}/{before} kept")
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
        # 스크래핑 완료 후 백그라운드에서 키워드 미리 계산
        threading.Thread(target=_compute_keywords_bg, daemon=True).start()


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


@app.route("/api/keywords", methods=["GET"])
def get_keywords():
    if not KEYWORDS_FILE.exists():
        return jsonify({})
    return jsonify(json.loads(KEYWORDS_FILE.read_text(encoding="utf-8")))

@app.route("/api/keywords", methods=["PUT"])
def save_keywords():
    try:
        data = request.get_json()
        KEYWORDS_FILE.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 400


_kw_cache = {"data": [], "updated": None}
_KW_TTL = 86400  # 24시간


def _compute_keywords_bg():
    """스크래핑 완료 후 백그라운드에서 kiwipiepy로 키워드 계산 후 캐시."""
    global _kiwi_ready, _kiwi

    # Kiwi 준비될 때까지 대기 (최대 60초)
    import time as _time
    for _ in range(60):
        if _kiwi_ready:
            break
        _time.sleep(1)
    if not _kiwi_ready or _kiwi is None:
        logger.warning("Kiwi 미준비 — 키워드 계산 건너뜀")
        return

    STOPWORDS = {
        "현대카드","현대","카드","포인트","엠포인트","엠몰","M몰","M포인트몰",
        "엠포인트몰","포인트몰","현대카드몰","신용카드","체크카드",
        "현대M포인트몰","현대카드M","현대M","엠포인트몰",
        "네이버","쿠팡","카카오","구글","유튜브","인스타","인스타그램","페이스북",
        "트위터","틱톡","당근","중고나라","번개장터","옥션","지마켓","11번가",
        "SSG","SSG닷컴","롯데온","네이버쇼핑","스마트스토어",
        "서울","부산","인천","대구","광주","대전","울산","세종","경기","강원",
        "충북","충남","전북","전남","경북","경남","제주","강남","홍대","신촌",
        "일본","미국","중국","영국","프랑스","이탈리아","독일","해외","국내",
        "인천공항","김포공항","서울특별시","경기도",
        "블랙","화이트","그레이","핑크","블루","레드","그린","옐로우","베이지",
        "네이비","브라운","골드","실버","퍼플","오렌지","민트","카키","아이보리",
        "구매","구입","후기","사용","리뷰","내돈내산","솔직","추천","소개","정보",
        "주문","배송","결제","적립","혜택","할인","이벤트","쿠폰","포인트적립",
        "이용","선택","판매","구성","제공","운영","진행","확인","신청","등록",
        "가능","필요","안녕","최대","다양","기준","방문","공유","참고","관리",
        "준비","시작","완료","완성","활용","지원","수령","수취","반품","교환",
        "언박싱","개봉기","구매처","사용법","설명서","사용기",
        "상품","제품","제조","브랜드","가격","금액","원","비용","종류",
        "사이즈","색상","컬러","디자인","퀄리티","품질","성능","기능","스펙",
        "느낌","생각","이유","경우","방법","과정","결과","내용","정도","수준",
        "이번","최근","다음","이전","해당","관련","이후","당시","현재","기존",
        "장점","단점","장단점","비교","차이","특징","효과","장단","쇼핑몰",
        "배송비","무료배송","빠른배송","무료","유료","정가","최저","최고가",
        "홈페이지","인터넷","온라인","오프라인","스토어","매장","공식","정품",
        "완전","진짜","정말","너무","매우","아주","엄청","굉장","최고","최악",
        "블로그","포스팅","글","사진","영상","구독","댓글","이웃","소통",
        "오늘","어제","내일","이번달","지난달","작년","올해","요즘",
        "안녕하세요","감사합니다","고맙습니다",
        "고민","선물","생일","기념일","크리스마스","명절","추석","설날",
        "스탠드","바디","에어","런치","오즈","커넥트","스카이","그랜드",
    }
    PRODUCT_NOUNS = {
        "청소기","공기청정기","에어컨","냉장고","세탁기","건조기","식기세척기",
        "전기밥솥","전자레인지","오븐","커피머신","커피메이커","블렌더","믹서기",
        "이어폰","헤드폰","스피커","노트북","태블릿","스마트워치","키보드","마우스",
        "게임기","카메라","드라이기","고데기","선풍기","가습기",
        "운동화","스니커즈","부츠","샌들","슬리퍼",
        "가방","백팩","크로스백","토트백","숄더백","클러치",
        "지갑","벨트","모자","장갑","스카프","넥타이",
        "항공권","캐리어","여행가방","호텔숙박","패키지여행",
        "상품권","기프티콘","이용권","교환권","쿠폰북",
        "향수","화장품","립스틱","파운데이션","세럼","에센스","크림","마스크팩",
    }
    _M_SIGNALS = ["M포인트몰","엠포인트몰","M포인트 몰","M몰","엠몰",
                  "현대카드 M몰","현대카드몰","M포인트로","엠포인트로",
                  "현대카드 포인트로","M포인트 사용","엠포인트 사용"]
    _PURCHASE_VERBS = ["구매","샀","결제","주문","사용했","사용해서","써서","썼"]

    try:
        data = _read_cache()
        reviews = data.get("reviews", [])
        purchase_texts = [
            (r.get("title","") + " " + r.get("body","")).strip()
            for r in reviews
            if r.get("source") in ("naver_blog","naver_cafe","web_snippet")
            and (r.get("title") or r.get("body"))
            and any(s in (r.get("title","") + r.get("body","")) for s in _M_SIGNALS)
            and any(v in (r.get("title","") + r.get("body","")) for v in _PURCHASE_VERBS)
        ]
        logger.info(f"[bg] kiwipiepy 키워드 분석 시작: {len(purchase_texts)}개 문서")

        word_count: dict = {}
        for text in purchase_texts:
            tokens = _kiwi.tokenize(text)
            words_in_doc: set = set()
            for t in tokens:
                word = t.form.strip()
                if len(word) < 2 or word in STOPWORDS:
                    continue
                if t.tag == "NNP":
                    words_in_doc.add(word)
                elif t.tag == "NNG" and word in PRODUCT_NOUNS:
                    words_in_doc.add(word)
            for word in words_in_doc:
                word_count[word] = word_count.get(word, 0) + 1

        sorted_words = sorted(
            [(k, v) for k, v in word_count.items() if v >= 2],
            key=lambda x: -x[1]
        )
        keywords = [
            {"rank": i + 1, "keyword": kw, "count": cnt}
            for i, (kw, cnt) in enumerate(sorted_words[:10])
        ]
        _kw_cache["data"] = keywords
        _kw_cache["updated"] = datetime.now(timezone.utc)
        logger.info(f"[bg] 키워드 계산 완료: {len(keywords)}개")

        # VOC 카테고리 top_issues도 kiwi로 재계산 후 캐시 업데이트
        try:
            from processors.aggregator import build_full_dataset
            cached = _read_cache()
            if cached.get("reviews"):
                new_dataset = build_full_dataset(cached["reviews"])
                meta = json.loads(CACHE_META_FILE.read_text(encoding="utf-8")) if CACHE_META_FILE.exists() else {}
                _write_cache(
                    new_dataset,
                    meta.get("source_counts", {}),
                    meta.get("scrape_durations", {}),
                    meta.get("errors", []),
                )
                logger.info("[bg] VOC 카테고리 top_issues kiwi 재계산 완료")
        except Exception as e2:
            logger.warning(f"[bg] VOC 재계산 실패: {e2}")
    except Exception as e:
        logger.warning(f"[bg] 키워드 계산 실패: {e}")


@app.route("/api/popular-keywords")
def get_popular_keywords():
    """백그라운드에서 미리 계산된 키워드 캐시를 즉시 반환."""
    return jsonify({"keywords": _kw_cache["data"]})


@app.route("/api/keyword-sources")
def get_keyword_sources():
    """특정 키워드가 추출된 문서 목록(URL, 제목, 소스) 반환."""
    keyword = request.args.get("keyword", "").strip()
    if not keyword:
        return jsonify({"sources": []})

    _M_SIGNALS = ["M포인트몰","엠포인트몰","M포인트 몰","M몰","엠몰",
                  "현대카드 M몰","현대카드몰","M포인트로","엠포인트로",
                  "현대카드 포인트로","M포인트 사용","엠포인트 사용"]
    _PURCHASE_VERBS = ["구매","샀","결제","주문","사용했","사용해서","써서","썼"]

    try:
        if not _kiwi_ready or _kiwi is None:
            return jsonify({"sources": [], "error": "분석기 준비 중"})

        data = _read_cache()
        reviews = data.get("reviews", [])

        sources = []
        for r in reviews:
            if r.get("source") not in ("naver_blog", "naver_cafe", "web_snippet"):
                continue
            text = (r.get("title", "") + " " + r.get("body", ""))
            if not (any(s in text for s in _M_SIGNALS) and any(v in text for v in _PURCHASE_VERBS)):
                continue
            # 이 문서에서 keyword가 추출됐는지 확인
            tokens = _kiwi.tokenize(text)
            found = any(t.form == keyword for t in tokens if t.tag in ("NNP", "NNG"))
            if not found:
                continue
            src_label = {"naver_blog": "블로그", "naver_cafe": "카페", "web_snippet": "웹문서"}.get(r.get("source",""), "기타")
            sources.append({
                "title": r.get("title") or "(제목 없음)",
                "url":   r.get("url") or r.get("link") or "",
                "source": src_label,
                "date":  (r.get("date") or "")[:10],
            })

        # 날짜 최신순 정렬
        sources.sort(key=lambda x: x["date"], reverse=True)
        return jsonify({"keyword": keyword, "sources": sources[:30]})

    except Exception as e:
        logger.warning(f"keyword-sources failed: {e}")
        return jsonify({"sources": [], "error": str(e)})


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
        # 기존 캐시 있으면 Kiwi 준비 후 바로 키워드 계산
        threading.Thread(target=_compute_keywords_bg, daemon=True).start()

    port = int(os.environ.get("PORT", 5000))
    app.run(debug=True, host="0.0.0.0", port=port, threaded=True, use_reloader=False)
