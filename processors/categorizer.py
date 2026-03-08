import json
import os
import re
import logging

logger = logging.getLogger(__name__)

_KEYWORDS: dict = {}

CATEGORY_DISPLAY_NAMES = {
    "앱_성능_속도": "앱 성능/속도",
    "UI_UX": "UI/UX",
    "포인트_혜택": "포인트/혜택",
    "배송_주문": "배송/주문",
    "고객서비스": "고객서비스",
    "로그인_인증": "로그인/인증",
    "기타": "기타",
}


def _load_keywords() -> dict:
    global _KEYWORDS
    if _KEYWORDS:
        return _KEYWORDS
    keywords_path = os.path.join(
        os.path.dirname(os.path.dirname(__file__)), "data", "keywords.json"
    )
    try:
        with open(keywords_path, encoding="utf-8") as f:
            _KEYWORDS = json.load(f)
    except Exception as e:
        logger.error(f"Failed to load keywords.json: {e}")
        _KEYWORDS = {"기타": []}
    return _KEYWORDS


def categorize_review(text: str) -> tuple:
    """
    Returns (display_category_name, confidence_score 0.0-1.0).
    Uses keyword hit counting per category.
    """
    keywords = _load_keywords()
    text_lower = (text or "").lower()

    scores = {}
    for cat, kw_list in keywords.items():
        if cat == "기타" or not kw_list:
            scores[cat] = 0.0
            continue
        hits = sum(1 for kw in kw_list if kw in text_lower)
        scores[cat] = hits / max(len(kw_list), 1)

    best_cat = max(scores, key=scores.get)
    best_score = scores.get(best_cat, 0.0)

    if best_score == 0.0:
        return ("기타", 0.0)

    display_name = CATEGORY_DISPLAY_NAMES.get(best_cat, best_cat)
    return (display_name, round(best_score, 4))


def get_all_categories() -> list:
    return list(CATEGORY_DISPLAY_NAMES.values())
