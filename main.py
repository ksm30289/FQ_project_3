import json
import os
import re
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set, Tuple

import gspread
from google.oauth2.service_account import Credentials
from google_play_scraper import Sort, reviews


# =====================================
# 환경변수 유틸
# =====================================
def _required_env(name: str) -> str:
    value = os.getenv(name)
    if value is None or str(value).strip() == "":
        raise RuntimeError(f"환경변수 누락: {name}")
    return value.strip()


def _get_env(name: str, default: str) -> str:
    value = os.getenv(name)
    if value is None or str(value).strip() == "":
        return default
    return value.strip()


def _get_int_env(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None or str(value).strip() == "":
        return default
    return int(value.strip())


def _get_float_env(name: str, default: float) -> float:
    value = os.getenv(name)
    if value is None or str(value).strip() == "":
        return default
    return float(value.strip())


def _get_bool_env(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None or str(value).strip() == "":
        return default
    return str(value).strip().lower() in ("1", "true", "yes", "y", "on")


# =====================================
# 설정
# =====================================
SPREADSHEET_ID = _required_env("SPREADSHEET_ID")

MAIN_WORKSHEET_NAME = _get_env("WORKSHEET_NAME", "구글플레이 리뷰")
LOW_RATING_WORKSHEET_NAME = _get_env("LOW_RATING_WORKSHEET_NAME", "구글플레이 저평점 리뷰")
POSITIVE_WORKSHEET_NAME = _get_env("POSITIVE_WORKSHEET_NAME", "구글플레이 긍정")
NEGATIVE_WORKSHEET_NAME = _get_env("NEGATIVE_WORKSHEET_NAME", "구글플레이 부정")
NEUTRAL_WORKSHEET_NAME = _get_env("NEUTRAL_WORKSHEET_NAME", "구글플레이 중립")
META_WORKSHEET_NAME = _get_env("META_WORKSHEET_NAME", "META")

APP_ID = _get_env("APP_ID", "com.linegames.fq")
LANG = _get_env("LANG", "ko")
COUNTRY = _get_env("COUNTRY", "kr")
SORT_NAME = _get_env("SORT", "NEWEST").upper()

BATCH_SIZE = _get_int_env("BATCH_SIZE", 100)
MAX_REVIEWS = _get_int_env("MAX_REVIEWS", 1000)
SLEEP_SECONDS = _get_float_env("SLEEP_SECONDS", 1.0)
LOW_RATING_MAX_SCORE = _get_int_env("LOW_RATING_MAX_SCORE", 2)

# ✅ 오래된 리뷰를 만나도 중단하지 않고 스킵만 하고 계속 진행
STOP_WHEN_OLDER_THAN_LAST_SYNC = _get_bool_env("STOP_WHEN_OLDER_THAN_LAST_SYNC", False)

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

HEADERS = [
    "review_id",
    "user_name",
    "score",
    "sentiment",
    "content",
    "created_at_utc",
    "created_at_local",
    "app_version",
    "thumbs_up_count",
    "review_created_version",
    "lang",
    "country",
    "app_id",
    "collected_at_utc",
]

META_HEADERS = ["key", "value"]

SORT_MAP = {
    "NEWEST": Sort.NEWEST,
    "RATING": Sort.RATING,
    "HELPFULNESS": Sort.MOST_RELEVANT,
    "MOST_RELEVANT": Sort.MOST_RELEVANT,
}


# =====================================
# 감성 분류용 키워드
# =====================================
POSITIVE_KEYWORDS = [
    "좋아요", "좋다", "재밌", "재미", "만족", "최고", "굿", "추천", "훌륭", "감사",
    "great", "good", "fun", "love", "awesome", "best", "excellent", "nice", "amazing",
]

NEGATIVE_KEYWORDS = [
    "별로", "나쁘", "최악", "불편", "짜증", "버그", "오류", "튕", "렉", "느림",
    "안됨", "안 돼", "문제", "고쳐", "개선", "환불", "실망", "과금", "비쌈",
    "bad", "worst", "bug", "error", "crash", "lag", "slow", "issue", "problem",
    "refund", "fix", "broken", "pay", "expensive",
]


# =====================================
# 구글 인증
# =====================================
def get_google_credentials_dict() -> Dict[str, Any]:
    raw = _required_env("GOOGLE_CREDENTIALS")
    try:
        return json.loads(raw)
    except json.JSONDecodeError as e:
        raise RuntimeError(f"GOOGLE_CREDENTIALS JSON 파싱 실패: {e}") from e


# =====================================
# 시트 클라이언트
# =====================================
class GoogleSheetClient:
    def __init__(self):
        creds_info = get_google_credentials_dict()
        creds = Credentials.from_service_account_info(creds_info, scopes=SCOPES)
        self.gc = gspread.authorize(creds)
        self.sh = self.gc.open_by_key(SPREADSHEET_ID)

        self.main_ws = self._get_or_create_worksheet(MAIN_WORKSHEET_NAME, rows=2000, cols=20)
        self.low_ws = self._get_or_create_worksheet(LOW_RATING_WORKSHEET_NAME, rows=1000, cols=20)
        self.pos_ws = self._get_or_create_worksheet(POSITIVE_WORKSHEET_NAME, rows=1000, cols=20)
        self.neg_ws = self._get_or_create_worksheet(NEGATIVE_WORKSHEET_NAME, rows=1000, cols=20)
        self.neu_ws = self._get_or_create_worksheet(NEUTRAL_WORKSHEET_NAME, rows=1000, cols=20)
        self.meta_ws = self._get_or_create_worksheet(META_WORKSHEET_NAME, rows=100, cols=5)

        self._ensure_header(self.main_ws, HEADERS)
        self._ensure_header(self.low_ws, HEADERS)
        self._ensure_header(self.pos_ws, HEADERS)
        self._ensure_header(self.neg_ws, HEADERS)
        self._ensure_header(self.neu_ws, HEADERS)
        self._ensure_header(self.meta_ws, META_HEADERS)

    def _get_or_create_worksheet(self, title: str, rows: int = 1000, cols: int = 20):
        try:
            return self.sh.worksheet(title)
        except gspread.WorksheetNotFound:
            return self.sh.add_worksheet(title=title, rows=rows, cols=cols)

    def _ensure_header(self, ws, headers: List[str]) -> None:
        first_row = ws.row_values(1)
        if first_row != headers:
            end_col = self._column_letter(len(headers))
            ws.update(f"A1:{end_col}1", [headers])

    def _column_letter(self, n: int) -> str:
        result = ""
        while n > 0:
            n, rem = divmod(n - 1, 26)
            result = chr(65 + rem) + result
        return result

    def get_existing_review_ids(self, ws) -> Set[str]:
        values = ws.col_values(1)
        if len(values) <= 1:
            return set()
        return {str(v).strip() for v in values[1:] if str(v).strip()}

    def append_rows_safe(self, ws, rows: List[List[Any]], chunk_size: int = 200) -> None:
        if not rows:
            return
        for i in range(0, len(rows), chunk_size):
            chunk = rows[i:i + chunk_size]
            ws.append_rows(chunk, value_input_option="USER_ENTERED")
            print(f"[INFO] [{ws.title}] 적재 완료: {i + len(chunk)}/{len(rows)}")
            time.sleep(1.0)

    def get_meta_value(self, key: str) -> str:
        records = self.meta_ws.get_all_records()
        for row in records:
            if str(row.get("key", "")).strip() == key:
                return str(row.get("value", "")).strip()
        return ""

    def upsert_meta_value(self, key: str, value: str) -> None:
        records = self.meta_ws.get_all_records()
        for idx, row in enumerate(records, start=2):
            if str(row.get("key", "")).strip() == key:
                self.meta_ws.update(f"B{idx}", [[value]])
                return
        self.meta_ws.append_row([key, value], value_input_option="USER_ENTERED")


# =====================================
# 유틸
# =====================================
def normalize_datetime(dt: Optional[datetime]) -> Tuple[str, str]:
    if not dt:
        return "", ""

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)

    utc_dt = dt.astimezone(timezone.utc)
    local_dt = dt.astimezone()

    return (
        utc_dt.strftime("%Y-%m-%d %H:%M:%S"),
        local_dt.strftime("%Y-%m-%d %H:%M:%S"),
    )


def parse_utc_datetime(text: str) -> Optional[datetime]:
    text = str(text).strip()
    if not text:
        return None
    try:
        return datetime.strptime(text, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def clean_text(text: str) -> str:
    text = str(text or "").strip()
    text = re.sub(r"\s+", " ", text)
    return text


def classify_sentiment(score: int, content: str) -> str:
    text = clean_text(content).lower()

    pos_hits = sum(1 for kw in POSITIVE_KEYWORDS if kw.lower() in text)
    neg_hits = sum(1 for kw in NEGATIVE_KEYWORDS if kw.lower() in text)

    if score >= 4 and neg_hits == 0:
        return "positive"
    if score <= 2:
        return "negative"

    if pos_hits > neg_hits and pos_hits >= 1:
        return "positive"
    if neg_hits > pos_hits and neg_hits >= 1:
        return "negative"

    return "neutral"


def map_review_to_row(review: Dict[str, Any], sentiment: str) -> List[Any]:
    created_at_utc, created_at_local = normalize_datetime(review.get("at"))
    collected_at_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    # ✅ 개발자 답변(replyContent, repliedAt)은 저장하지 않음
    return [
        str(review.get("reviewId", "")).strip(),
        str(review.get("userName", "")).strip(),
        int(review.get("score", 0) or 0),
        sentiment,
        clean_text(review.get("content", "")),
        created_at_utc,
        created_at_local,
        str(review.get("appVersion", "")).strip(),
        int(review.get("thumbsUpCount", 0) or 0),
        str(review.get("reviewCreatedVersion", "")).strip(),
        LANG,
        COUNTRY,
        APP_ID,
        collected_at_utc,
    ]


# =====================================
# 리뷰 수집
# =====================================
def fetch_reviews_incremental(
    app_id: str,
    lang: str,
    country: str,
    sort_mode: Any,
    batch_size: int,
    max_reviews: int,
    last_synced_at: Optional[datetime],
) -> List[Dict[str, Any]]:
    all_reviews: List[Dict[str, Any]] = []
    continuation_token = None
    page = 0

    while len(all_reviews) < max_reviews:
        remaining = max_reviews - len(all_reviews)
        request_count = min(batch_size, remaining)

        result, continuation_token = reviews(
            app_id,
            lang=lang,
            country=country,
            sort=sort_mode,
            count=request_count,
            continuation_token=continuation_token,
        )

        page += 1
        print(f"[INFO] page={page}, fetched={len(result)}, accumulated={len(all_reviews) + len(result)}")

        if not result:
            print("[INFO] 더 이상 가져올 리뷰가 없습니다.")
            break

        old_review_found_in_page = False

        for item in result:
            review_dt = item.get("at")
            if review_dt and review_dt.tzinfo is None:
                review_dt = review_dt.replace(tzinfo=timezone.utc)

            if last_synced_at and review_dt and review_dt <= last_synced_at:
                old_review_found_in_page = True

                if STOP_WHEN_OLDER_THAN_LAST_SYNC:
                    print("[INFO] 마지막 동기화 시점보다 오래된 리뷰를 만나 수집 중단.")
                    return all_reviews

                # ✅ 오래된 리뷰는 스킵만 하고 계속 진행
                continue

            all_reviews.append(item)

            if len(all_reviews) >= max_reviews:
                break

        if continuation_token is None:
            print("[INFO] continuation_token 없음. 마지막 페이지 도달.")
            break

        if old_review_found_in_page and not STOP_WHEN_OLDER_THAN_LAST_SYNC:
            print("[INFO] 오래된 리뷰가 포함되어 있었지만 스킵 후 계속 진행합니다.")

        time.sleep(SLEEP_SECONDS)

    return all_reviews


def main():
    print("=== Google Play 리뷰 증분 수집 시작 ===")
    print(f"[INFO] APP_ID={APP_ID}")
    print(f"[INFO] LANG={LANG}, COUNTRY={COUNTRY}, SORT={SORT_NAME}")
    print(f"[INFO] BATCH_SIZE={BATCH_SIZE}, MAX_REVIEWS={MAX_REVIEWS}")
    print(f"[INFO] STOP_WHEN_OLDER_THAN_LAST_SYNC={STOP_WHEN_OLDER_THAN_LAST_SYNC}")

    sort_mode = SORT_MAP.get(SORT_NAME)
    if sort_mode is None:
        raise RuntimeError(f"지원하지 않는 SORT 값: {SORT_NAME}")

    sheet_client = GoogleSheetClient()

    existing_main_ids = sheet_client.get_existing_review_ids(sheet_client.main_ws)
    existing_low_ids = sheet_client.get_existing_review_ids(sheet_client.low_ws)
    existing_pos_ids = sheet_client.get_existing_review_ids(sheet_client.pos_ws)
    existing_neg_ids = sheet_client.get_existing_review_ids(sheet_client.neg_ws)
    existing_neu_ids = sheet_client.get_existing_review_ids(sheet_client.neu_ws)

    print(f"[INFO] 기존 main review_id 수: {len(existing_main_ids)}")

    last_synced_at_raw = sheet_client.get_meta_value("last_synced_at_utc")
    last_synced_at = parse_utc_datetime(last_synced_at_raw)

    print(f"[INFO] last_synced_at_utc = {last_synced_at_raw or '(없음)'}")

    fetched_reviews = fetch_reviews_incremental(
        app_id=APP_ID,
        lang=LANG,
        country=COUNTRY,
        sort_mode=sort_mode,
        batch_size=BATCH_SIZE,
        max_reviews=MAX_REVIEWS,
        last_synced_at=last_synced_at,
    )

    print(f"[INFO] 수집된 리뷰 수: {len(fetched_reviews)}")

    main_rows: List[List[Any]] = []
    low_rows: List[List[Any]] = []
    pos_rows: List[List[Any]] = []
    neg_rows: List[List[Any]] = []
    neu_rows: List[List[Any]] = []

    newest_review_dt: Optional[datetime] = last_synced_at

    skipped_dup = 0
    skipped_empty = 0

    for r in fetched_reviews:
        review_id = str(r.get("reviewId", "")).strip()
        content = clean_text(r.get("content", ""))
        score = int(r.get("score", 0) or 0)
        review_dt = r.get("at")

        if review_dt and review_dt.tzinfo is None:
            review_dt = review_dt.replace(tzinfo=timezone.utc)

        if not review_id or not content:
            skipped_empty += 1
            continue

        if review_id in existing_main_ids:
            skipped_dup += 1
            continue

        sentiment = classify_sentiment(score, content)
        row = map_review_to_row(r, sentiment)

        main_rows.append(row)
        existing_main_ids.add(review_id)

        if score <= LOW_RATING_MAX_SCORE and review_id not in existing_low_ids:
            low_rows.append(row)
            existing_low_ids.add(review_id)

        if sentiment == "positive" and review_id not in existing_pos_ids:
            pos_rows.append(row)
            existing_pos_ids.add(review_id)
        elif sentiment == "negative" and review_id not in existing_neg_ids:
            neg_rows.append(row)
            existing_neg_ids.add(review_id)
        elif sentiment == "neutral" and review_id not in existing_neu_ids:
            neu_rows.append(row)
            existing_neu_ids.add(review_id)

        if review_dt:
            if newest_review_dt is None or review_dt > newest_review_dt:
                newest_review_dt = review_dt

    print(f"[INFO] 신규 main 적재 대상: {len(main_rows)}")
    print(f"[INFO] 신규 low 적재 대상: {len(low_rows)}")
    print(f"[INFO] 신규 positive 적재 대상: {len(pos_rows)}")
    print(f"[INFO] 신규 negative 적재 대상: {len(neg_rows)}")
    print(f"[INFO] 신규 neutral 적재 대상: {len(neu_rows)}")
    print(f"[INFO] 중복 스킵 수: {skipped_dup}")
    print(f"[INFO] 빈값 스킵 수: {skipped_empty}")

    if main_rows:
        sheet_client.append_rows_safe(sheet_client.main_ws, main_rows)

    if low_rows:
        sheet_client.append_rows_safe(sheet_client.low_ws, low_rows)

    if pos_rows:
        sheet_client.append_rows_safe(sheet_client.pos_ws, pos_rows)

    if neg_rows:
        sheet_client.append_rows_safe(sheet_client.neg_ws, neg_rows)

    if neu_rows:
        sheet_client.append_rows_safe(sheet_client.neu_ws, neu_rows)

    if newest_review_dt:
        newest_review_dt_utc = newest_review_dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        sheet_client.upsert_meta_value("last_synced_at_utc", newest_review_dt_utc)
        sheet_client.upsert_meta_value("last_run_at_utc", datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"))
        sheet_client.upsert_meta_value("app_id", APP_ID)
        print(f"[INFO] last_synced_at_utc 갱신: {newest_review_dt_utc}")

    print("=== 작업 완료 ===")


if __name__ == "__main__":
    main()
