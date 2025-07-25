from __future__ import annotations

"""Airflow DAG: summarize_disclosure_events_batch_dag

요약 대상 공시(raw 텍스트)를 OpenAI **Batch API**로 일괄 요약하여
PostgreSQL `disclosure_events.summary_kr` 컬럼에 저장한다.
- 비용 절감 및 대규모 비동기 처리를 위해 Batch API 사용
- ❶ 대상 조회 → ❷ JSONL 업로드 → ❸ Batch Job 생성 →
  ❹ 완료 대기 → ❺ 결과 다운로드 → ❻ DB 업데이트
"""

import json
import os
import tempfile
import time
from datetime import datetime

import sqlalchemy as sa
from airflow.decorators import dag, task
from dotenv import load_dotenv
from openai import OpenAI

load_dotenv()

# ─────────── 환경 변수 및 클라이언트 ─────────────────────────────
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")

DB_URL = (
    f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}"
    f"@postgres_events:{POSTGRES_PORT}/{POSTGRES_DB}"
)
ENGINE = sa.create_engine(DB_URL, pool_pre_ping=True, future=True)

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
client = OpenAI(api_key=OPENAI_API_KEY)

SYSTEM_PROMPT = (
    "You are a helpful assistant to summarize the given text about a Korean "
    "corporate report within 2–3 sentences. Please respond only in Korean."
)

MODEL = "gpt-4.1-nano"


# ─────────── DAG 정의 ────────────────────────────────────────────
@dag(
    start_date=datetime(2025, 7, 4),
    schedule="@once",
    catchup=False,
    tags=["openai", "summarize", "batch"],
    max_active_tasks=1,
)
def summarize_disclosure_events_batch_dag():
    """일괄 Batch 요약 DAG"""

    # ① summary_kr 컬럼 보장
    @task
    def ensure_summary_column() -> None:
        alter_sql = """
        ALTER TABLE disclosure_events
            ADD COLUMN IF NOT EXISTS summary_kr TEXT;
        """
        with ENGINE.begin() as conn:
            conn.execute(sa.text(alter_sql))

    # ② 요약 대상 선택
    @task
    def fetch_events_to_summarize(batch_size: int = 1000) -> list[dict]:
        sql = """
            SELECT id, raw
            FROM disclosure_events
            WHERE summary_kr IS NULL
              AND raw IS NOT NULL
              AND raw <> ''
            ORDER BY id
            LIMIT :limit
            """
        with ENGINE.connect() as conn:
            result = conn.execute(sa.text(sql), {"limit": batch_size})
            return [{"id": row.id, "raw": row.raw} for row in result]

    # ③ JSONL 파일 작성 & OpenAI 업로드
    @task
    def upload_batch_file(events: list[dict]) -> str | None:
        if not events:
            return None

        with tempfile.NamedTemporaryFile(
            mode="w+", delete=False, encoding="utf-8"
        ) as fp:
            for ev in events:
                payload = {
                    "custom_id": str(ev["id"]),
                    "method": "POST",
                    "url": "/v1/chat/completions",
                    "body": {
                        "model": MODEL,
                        "messages": [
                            {"role": "system", "content": SYSTEM_PROMPT},
                            {"role": "user", "content": ev["raw"]},
                        ],
                    },
                }
                fp.write(json.dumps(payload, ensure_ascii=False) + "\n")
            fp.flush()
            file_resp = client.files.create(file=open(fp.name, "rb"), purpose="batch")
        return file_resp.id

    # ④ Batch Job 생성
    @task
    def create_batch_job(file_id: str | None) -> str | None:
        if file_id is None:
            return None
        batch = client.batches.create(
            input_file_id=file_id,
            endpoint="/v1/chat/completions",
            completion_window="24h",
        )
        return batch.id

    # ⑤ 완료 대기 (폴링)
    @task
    def wait_batch_complete(batch_id: str | None, poll_interval: int = 30):
        if batch_id is None:
            return None
        while True:
            batch = client.batches.retrieve(batch_id)
            status = batch.status
            if status in {"completed", "failed", "expired"}:
                return batch  # 반환해서 downstream에서 상태 체크
            time.sleep(poll_interval)

    # ⑥ 결과 다운로드 & 파싱
    @task
    def download_batch_output(batch) -> list[dict]:
        if not batch or batch.status != "completed":
            return []
        output_file_id = batch.output_file_id
        output_txt = client.files.retrieve_content(output_file_id)
        summaries = []
        for line in output_txt.splitlines():
            obj = json.loads(line)
            summaries.append(
                {
                    "id": int(obj["custom_id"]),
                    "summary": obj["response"]["choices"][0]["message"]["content"],
                }
            )
        return summaries

    # ⑦ DB 업데이트
    @task
    def update_summaries(summaries: list[dict]) -> None:
        if not summaries:
            print("No summaries to update")
            return
        with ENGINE.begin() as conn:
            for s in summaries:
                conn.execute(
                    sa.text(
                        """
                        UPDATE disclosure_events
                           SET summary_kr = :summary
                         WHERE id = :id
                        """
                    ),
                    {"summary": s["summary"], "id": s["id"]},
                )
        print(f"Updated {len(summaries)} rows.")

    # ─── DAG 의존성 체인 ──────────────────────────────────────────
    col = ensure_summary_column()
    evts = fetch_events_to_summarize()

    file_id = upload_batch_file(evts)
    batch_id = create_batch_job(file_id)
    batch = wait_batch_complete(batch_id)
    summaries = download_batch_output(batch)
    upd = update_summaries(summaries)

    col >> evts >> file_id >> batch_id >> batch >> summaries >> upd


# DAG 인스턴스
summarize_dag = summarize_disclosure_events_batch_dag()
