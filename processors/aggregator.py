import hashlib
import json
import logging
from collections import Counter, defaultdict
from datetime import datetime, timezone, timedelta
from pathlib import Path

from processors.categorizer import categorize_review, get_all_categories

# 카테고리 display name → keywords.json key 매핑
_DISPLAY_TO_KEY = {
    "앱 성능/속도": "앱_성능_속도",
    "UI/UX": "UI_UX",
    "포인트/혜택": "포인트_혜택",
    "배송/주문": "배송_주문",
    "고객서비스": "고객서비스",
    "로그인/인증": "로그인_인증",
    "기타": "기타",
}


def _load_category_keywords() -> dict:
    try:
        kw_path = Path(__file__).parent.parent / "data" / "keywords.json"
        return json.loads(kw_path.read_text(encoding="utf-8"))
    except Exception:
        return {}
from processors.scorer import (
    calculate_priority_score,
    normalize_score,
    get_sentiment_label,
    get_priority_level,
)

logger = logging.getLogger(__name__)


def _make_hash(r: dict) -> str:
    body = (r.get("body") or "")[:120]
    return hashlib.md5(f"{r.get('source','')}{body}".encode()).hexdigest()


def deduplicate(reviews: list) -> list:
    seen = set()
    result = []
    for r in reviews:
        h = _make_hash(r)
        if h not in seen and (r.get("body") or r.get("title")):
            seen.add(h)
            result.append(r)
    return result


def _week_label(dt: datetime) -> str:
    monday = dt - timedelta(days=dt.weekday())
    return monday.strftime("%Y-%m-%d")


def build_trend_data(reviews: list, days: int = 90) -> dict:
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    rated = [r for r in reviews if r.get("rating") and r.get("date")]

    weekly: dict = defaultdict(lambda: defaultdict(list))

    for r in rated:
        try:
            dt = datetime.fromisoformat(r["date"])
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            if dt < cutoff:
                continue
            week = _week_label(dt)
            src = r.get("source", "other")
            weekly[week][src].append(r["rating"])
        except Exception:
            continue

    sorted_weeks = sorted(weekly.keys())
    labels = sorted_weeks

    sources = ["google_play", "app_store"]
    series: dict = {s: [] for s in sources}
    series["overall"] = []

    for week in sorted_weeks:
        all_ratings = []
        for src in sources:
            vals = weekly[week].get(src, [])
            avg = round(sum(vals) / len(vals), 2) if vals else None
            series[src].append(avg)
            all_ratings.extend(vals)
        series["overall"].append(
            round(sum(all_ratings) / len(all_ratings), 2) if all_ratings else None
        )

    return {"labels": labels, "series": series}


# app.py의 _init_kiwi()에서 초기화 완료 후 주입됨
_kiwi = None

_NOUN_STOPWORDS = {
    # 대명사/부사
    "이거", "그게", "그냥", "진짜", "너무", "정말", "아직", "계속", "매번",
    "항상", "자꾸", "빨리", "바로", "다시", "그리고", "하지만", "근데", "왜냐",
    # 조사/어미 형태 & 형용사
    "하고", "어서", "에서", "으로", "하는", "이런", "그런", "있는", "없는",
    "해서", "해도", "이게", "저도", "이건", "뭔가", "때문", "인데", "같은",
    "같아", "같이", "것같", "느낌", "생각", "경우", "부분", "내용",
    # 앱/서비스 공통
    "현대카드", "현대", "카드", "앱", "어플", "이용", "사용", "서비스",
    "회사", "제품", "고객", "문제", "기능", "화면", "앱스", "업데이트",
    "이용자", "사용자", "고객님", "방문", "후기", "리뷰",
    # 범용 명사 (의미 없음)
    "정도", "방법", "동안", "일주일", "이후", "현재", "기준", "버전",
    "이전", "관련", "추가", "개선", "수정", "삭제", "처리", "확인",
}

def _top_keywords(reviews: list, n: int = 3, allowed_keywords: list = None) -> list:
    import re
    counter: Counter = Counter()

    if _kiwi is not None:
        # kiwipiepy NNG(일반명사) + NNP(고유명사) 추출
        for r in reviews:
            text = (r.get("title") or "") + " " + (r.get("body") or "")
            try:
                tokens = _kiwi.tokenize(text)
            except Exception:
                continue
            seen = set()
            for t in tokens:
                w = t.form.strip()
                if t.tag not in ("NNG", "NNP"):
                    continue
                if len(w) < 2 or w in _NOUN_STOPWORDS or w in seen:
                    continue
                counter[w] += 1
                seen.add(w)
    else:
        # fallback: 한글 2자 이상 공백 분리 단어
        for r in reviews:
            text = (r.get("title") or "") + " " + (r.get("body") or "")
            words = re.findall(r'[가-힣]{2,6}', text)
            seen = set()
            for w in words:
                if w not in _NOUN_STOPWORDS and w not in seen:
                    counter[w] += 1
                    seen.add(w)

    return [w for w, _ in counter.most_common(n)]


def build_full_dataset(raw_reviews: list) -> dict:
    reviews = deduplicate(raw_reviews)
    logger.info(f"Processing {len(reviews)} reviews (after dedup)...")

    for r in reviews:
        text = (r.get("body") or "") + " " + (r.get("title") or "")
        cat, conf = categorize_review(text)
        r["category"] = cat
        r["category_confidence"] = conf
        raw_score = calculate_priority_score(r)
        r["priority_score_raw"] = raw_score
        r["priority_score"] = normalize_score(raw_score)
        r["priority_level"] = get_priority_level(r["priority_score"])
        r["sentiment"] = get_sentiment_label(r.get("rating"))

    reviews.sort(key=lambda x: x.get("priority_score_raw", 0), reverse=True)

    # --- Summary stats ---
    total = len(reviews)
    rated = [r for r in reviews if r.get("rating") is not None]
    avg_rating = round(sum(r["rating"] for r in rated) / len(rated), 2) if rated else 0.0

    by_source: dict = {}
    for src in ["google_play", "app_store", "naver_blog", "naver_cafe", "web_snippet"]:
        src_reviews = [r for r in reviews if r.get("source") == src]
        src_rated = [r for r in src_reviews if r.get("rating") is not None]
        by_source[src] = {
            "count": len(src_reviews),
            "avg_rating": round(
                sum(r["rating"] for r in src_rated) / len(src_rated), 2
            ) if src_rated else None,
        }

    sentiment_dist = Counter(r["sentiment"] for r in reviews)
    rating_dist = Counter(str(r["rating"]) for r in rated)

    all_categories = get_all_categories()
    cat_keywords_map = _load_category_keywords()
    by_category: dict = {}
    for cat in all_categories:
        cat_reviews = [r for r in reviews if r.get("category") == cat]
        if not cat_reviews:
            by_category[cat] = {
                "count": 0, "avg_priority": 0, "negative_pct": 0,
                "top_issues": [], "priority_rank": 999,
            }
            continue
        neg = [r for r in cat_reviews if r.get("sentiment") == "negative"]
        cat_key = _DISPLAY_TO_KEY.get(cat, "기타")
        allowed = cat_keywords_map.get(cat_key) or None
        by_category[cat] = {
            "count": len(cat_reviews),
            "avg_priority": round(
                sum(r["priority_score"] for r in cat_reviews) / len(cat_reviews), 1
            ),
            "negative_pct": round(len(neg) / len(cat_reviews) * 100, 1),
            "top_issues": _top_keywords(neg or cat_reviews, n=5, allowed_keywords=allowed),
            "priority_rank": 0,
        }

    ranked = sorted(
        by_category.items(),
        key=lambda x: (x[1]["avg_priority"], x[1]["negative_pct"]),
        reverse=True,
    )
    for rank, (cat, _) in enumerate(ranked, 1):
        by_category[cat]["priority_rank"] = rank

    urgent_count = sum(1 for r in reviews if r.get("priority_level") == "긴급")
    cutoff_7d = datetime.now(timezone.utc) - timedelta(days=7)
    recent_7d = 0
    for r in reviews:
        try:
            dt = datetime.fromisoformat(r["date"])
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            if dt >= cutoff_7d:
                recent_7d += 1
        except Exception:
            pass

    negative_pct = round(
        sentiment_dist.get("negative", 0) / total * 100, 1
    ) if total else 0.0

    trend_data = build_trend_data(reviews)

    return {
        "reviews": reviews,
        "summary": {
            "total_count": total,
            "avg_rating": avg_rating,
            "negative_pct": negative_pct,
            "urgent_count": urgent_count,
            "recent_7d": recent_7d,
            "by_source": by_source,
            "by_category": by_category,
            "sentiment_distribution": {
                "positive": sentiment_dist.get("positive", 0),
                "neutral": sentiment_dist.get("neutral", 0),
                "negative": sentiment_dist.get("negative", 0),
            },
            "rating_distribution": {
                str(i): rating_dist.get(str(i), 0) for i in range(1, 6)
            },
            "top_priority_reviews": [r["id"] for r in reviews[:20]],
            "trend_data": trend_data,
        },
    }
