from __future__ import annotations

"""
kind 테이블에서 label이 있는 공시의 masked 텍스트에 대해 KR-FinBERT-SC로 감정 분석을 수행하고
label_FinBERT 테이블에 결과를 저장한다.
"""

import os
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import sqlalchemy as sa
import torch
import torch.nn.functional as F
from airflow.decorators import dag, task
from dotenv import load_dotenv
from transformers import AutoModelForSequenceClassification, AutoTokenizer

load_dotenv()

# 환경 변수 및 설정
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")

DB_URL = (
    f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}"
    f"@postgres_events:{POSTGRES_PORT}/{POSTGRES_DB}"
)
ENGINE = sa.create_engine(DB_URL, pool_pre_ping=True, future=True)

MODEL_NAME = "snunlp/KR-FinBert-SC"

# KR-FinBERT-SC label 매핑 (positive → 1, neutral → 0, negative → -1)
LABEL_MAP = {"positive": 1, "neutral": 0, "negative": -1}

KST = ZoneInfo("Asia/Seoul")
UTC = ZoneInfo("UTC")

# 전역 모델 및 토크나이저 (DAG 로드 시 한 번만 로드)
_TOKENIZER = None
_MODEL = None
_DEVICE = None


def _load_model_once():
    """모델을 한 번만 로드하고 전역 변수에 저장"""
    global _TOKENIZER, _MODEL, _DEVICE
    if _TOKENIZER is None or _MODEL is None:
        print(f"[Global] Loading model: {MODEL_NAME}")
        _TOKENIZER = AutoTokenizer.from_pretrained(MODEL_NAME)
        _MODEL = AutoModelForSequenceClassification.from_pretrained(MODEL_NAME)
        _DEVICE = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        _MODEL.to(_DEVICE).eval()
        print(f"[Global] Model loaded on {_DEVICE}")
    return _TOKENIZER, _MODEL, _DEVICE


# DAG 로드 시 모델 한 번만 로드
_load_model_once()


@dag(
    start_date=datetime(2025, 7, 4),
    schedule="@once",
    catchup=False,
    tags=["nlp", "sentiment", "batch"],
    max_active_tasks=1,
    params={"start_date": "2023-01-01", "end_date": "2023-12-31"},
)
def enrich_kind_events_with_finbert_sentiment_dag():
    """kind 테이블의 공시를 KR-FinBERT-SC로 감정 분석하는 DAG"""

    @task(task_id="create_label_FinBERT_table_if_not_exists")
    def create_label_FinBERT_table_if_not_exists() -> None:
        """label_FinBERT 테이블이 없으면 생성"""
        sql = """
            CREATE TABLE IF NOT EXISTS label_FinBERT (
                event_id BIGINT PRIMARY KEY,
                disclosed_at TIMESTAMP,
                label INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_label_FinBERT_event_id ON label_FinBERT(event_id);
            CREATE INDEX IF NOT EXISTS idx_label_FinBERT_disclosed_at ON label_FinBERT(disclosed_at);
        """
        with ENGINE.begin() as conn:
            conn.execute(sa.text(sql))
        print("Checked and created label_FinBERT table if needed")

    @task(task_id="build_month_ranges")
    def build_month_ranges(start_date_str: str, end_date_str: str) -> list[dict]:
        """월 단위 경계 생성"""
        start_dt = datetime.fromisoformat(start_date_str)
        end_base = datetime.fromisoformat(end_date_str)

        if start_dt.tzinfo is None:
            start_dt = start_dt.replace(tzinfo=KST)
        else:
            start_dt = start_dt.astimezone(KST)

        if end_base.tzinfo is None:
            end_dt = end_base.replace(
                hour=23, minute=59, second=59, microsecond=999999, tzinfo=KST
            )
        else:
            end_dt = end_base.astimezone(KST)

        def first_of_month(dt: datetime) -> datetime:
            return dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

        def first_of_next_month(dt: datetime) -> datetime:
            return (dt.replace(day=28) + timedelta(days=4)).replace(
                day=1, hour=0, minute=0, second=0, microsecond=0
            )

        cur = first_of_month(start_dt)
        out: list[dict] = []
        while cur <= end_dt:
            next_first = first_of_next_month(cur)
            kst_start = max(cur, start_dt)
            kst_end = min(end_dt, next_first - timedelta(microseconds=1))

            utc_start = kst_start.astimezone(UTC).isoformat()
            utc_end = kst_end.astimezone(UTC).isoformat()

            out.append({"start_date": utc_start, "end_date": utc_end})
            cur = next_first

        print(
            f"Generated {len(out)} month ranges (KST→UTC) from {start_date_str} to {end_date_str}"
        )
        print(f"First UTC range: {out[0] if out else 'None'}")
        return out

    @task(task_id="fetch_kind_events")
    def fetch_kind_events(
        start_date: str | None = None, end_date: str | None = None
    ) -> list[dict]:
        """kind 테이블에서 label이 있는 공시 조회"""
        s = datetime.fromisoformat(start_date)
        e = datetime.fromisoformat(end_date)
        if s.tzinfo is None:
            s = s.replace(tzinfo=UTC)
        else:
            s = s.astimezone(UTC)
        if e.tzinfo is None:
            e = e.replace(tzinfo=UTC)
        else:
            e = e.astimezone(UTC)

        sql = """
            SELECT id, disclosed_at, masked
            FROM kind
            WHERE label IN (-1, 0, 1)
              AND masked IS NOT NULL
              AND masked <> ''
              AND disclosed_at >= :start_date
              AND disclosed_at <= :end_date
              AND TO_CHAR(disclosed_at, 'HH24:MI') BETWEEN '09:00' AND '15:20'
            ORDER BY disclosed_at ASC
        """
        with ENGINE.connect() as conn:
            result = conn.execute(sa.text(sql), {"start_date": s, "end_date": e})
            events = [
                {
                    "id": row.id,
                    "disclosed_at": row.disclosed_at,
                    "masked": row.masked,
                }
                for row in result
            ]

        print(
            f"Fetched {len(events)} kind events for UTC period {s.isoformat()} → {e.isoformat()}"
        )
        return events

    @task(task_id="infer_and_save_sentiment")
    def infer_and_save_sentiment(events: list[dict]) -> None:
        """KR-FinBERT-SC로 감정 분석 수행 및 저장"""
        if not events:
            print("No events to process")
            return

        # 전역 모델 재사용
        tokenizer, model, device = _load_model_once()
        print(f"[Task] Using pre-loaded model on {device}")

        # 배치 추론
        id2label = model.config.id2label
        BATCH_SIZE = 32

        all_labels = []
        total_batches = (len(events) + BATCH_SIZE - 1) // BATCH_SIZE

        print(
            f"Starting batch inference: {len(events)} sentences in {total_batches} batches"
        )

        for i in range(0, len(events), BATCH_SIZE):
            batch_sentences = [ev["masked"] for ev in events[i : i + BATCH_SIZE]]
            current_batch = (i // BATCH_SIZE) + 1

            print(
                f"Processing batch {current_batch}/{total_batches} ({len(batch_sentences)} sentences)"
            )

            encodings = tokenizer(
                batch_sentences,
                padding=True,
                truncation=True,
                max_length=256,
                return_tensors="pt",
            )
            encodings = {k: v.to(device) for k, v in encodings.items()}

            with torch.no_grad():
                logits = model(**encodings).logits
                probs = F.softmax(logits, dim=-1).cpu()

            batch_labels = [id2label[p.argmax().item()] for p in probs]

            all_labels.extend(batch_labels)

            del encodings, logits, probs
            if torch.cuda.is_available():
                torch.cuda.empty_cache()

        print(f"Batch inference completed: {len(all_labels)} sentences processed")

        # 결과 저장 (문자열 label → 숫자로 매핑)
        insert_sql = """
            INSERT INTO label_FinBERT (event_id, disclosed_at, label)
            VALUES (:event_id, :disclosed_at, :label)
            ON CONFLICT (event_id) DO UPDATE
            SET disclosed_at = EXCLUDED.disclosed_at,
                label = EXCLUDED.label
        """
        records = [
            {
                "event_id": ev["id"],
                "disclosed_at": ev["disclosed_at"],
                "label": LABEL_MAP[label],
            }
            for ev, label in zip(events, all_labels)
        ]

        with ENGINE.begin() as conn:
            conn.execute(sa.text(insert_sql), records)

        print(f"Saved {len(records)} sentiment results to label_FinBERT table")

    # 템플릿으로 문자열 파라미터 주입
    month_ranges = build_month_ranges(
        start_date_str="{{ params.start_date }}",
        end_date_str="{{ params.end_date }}",
    )

    alter_table_task = create_label_FinBERT_table_if_not_exists()
    evts = fetch_kind_events.expand_kwargs(month_ranges)
    sentiment_results = infer_and_save_sentiment.expand(events=evts)

    # 의존성 설정
    (alter_table_task >> month_ranges >> evts >> sentiment_results)


# DAG 인스턴스
scoring_dag = enrich_kind_events_with_finbert_sentiment_dag()
