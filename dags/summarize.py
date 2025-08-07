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

SYSTEM_PROMPT = "You are a helpful assistant to summarize the given text about a Korean corporate report within 2~3 sentences. Please respond only in Korean."
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
    def fetch_events_to_summarize(
        start_date: datetime | None = None,
        end_date: datetime | None = None,
    ) -> list[dict]:
        sql = """
            SELECT id, raw
            FROM disclosure_events
            WHERE raw IS NOT NULL
            AND raw <> ''
            AND  disclosed_at AT TIME ZONE 'Asia/Seoul' >= :start_date
            AND  disclosed_at AT TIME ZONE 'Asia/Seoul' <= :end_date
            ORDER BY disclosed_at, id
        """

        with ENGINE.connect() as conn:
            result = conn.execute(
                sa.text(sql), {"start_date": start_date, "end_date": end_date}
            )
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
    def wait_batch_complete(
        batch_id: str | None, poll_interval: int = 30
    ) -> dict | None:
        """배치 완료까지 폴링 후 직렬화 가능한 dict 반환"""
        if batch_id is None:
            return None

        while True:
            batch = client.batches.retrieve(batch_id)
            if batch.status in {"completed", "failed", "expired"}:
                return batch.model_dump()
            time.sleep(poll_interval)

    # ⑥ 결과 다운로드 & 파싱

    @task
    def download_batch_output(batch: dict | None) -> list[dict]:
        if not batch or batch.get("status") != "completed":
            return []

        output_file_id: str | None = batch.get("output_file_id")
        if not output_file_id:
            return []

        txt = client.files.retrieve_content(output_file_id)
        ok, failed = [], []
        for line in txt.splitlines():
            obj = json.loads(line)
            cid = int(obj["custom_id"])  # DB PK

            # ── 성공(200) ─────────────────────────────
            if obj.get("error") is None and obj["response"]["status_code"] == 200:
                content = (
                    obj["response"]["body"]["choices"][0]["message"]["content"]
                    if obj["response"]["body"]["choices"]
                    else None
                )
                ok.append({"id": cid, "summary": content})

            # ── 실패 또는 비-200 ──────────────────────
            else:
                failed.append(
                    {
                        "id": cid,
                        "status": obj.get("response", {}).get("status_code"),
                        "error": obj.get("error"),
                    }
                )

        if failed:
            print(f"{len(failed)} requests failed/rejected → 첫 3건: {failed[:3]}")
        return ok

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
    evts = fetch_events_to_summarize(
        start_date=datetime(2023, 12, 1), end_date=datetime(2023, 12, 2)
    )

    file_id = upload_batch_file(evts)
    batch_id = create_batch_job(file_id)
    batch = wait_batch_complete(batch_id)
    summaries = download_batch_output(batch)
    upd = update_summaries(summaries)

    col >> evts >> file_id >> batch_id >> batch >> summaries >> upd


# DAG 인스턴스
summarize_dag = summarize_disclosure_events_batch_dag()
