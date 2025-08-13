from __future__ import annotations

"""Airflow DAG: summarize_disclosure_events_batch_dag (monthly-chunked)

요약 대상 공시(raw 텍스트)를 OpenAI Batch API로 월 단위로 일괄 요약하여
PostgreSQL `disclosure_events.summary_kr` 컬럼에 저장한다.
- ❶ summary 컬럼 보장 → (아래 월 반복)
  ❷ 대상 조회 → ❸ JSONL 업로드 → ❹ Batch Job 생성 →
  ❺ 완료 대기 → ❻ 결과 다운로드 → ❼ DB 업데이트
"""

import json
import os
import tempfile
import time
from datetime import datetime, timedelta

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
    "You are a helpful assistant to summarize the given text about a Korean corporate "
    "report within 2~3 sentences. Please respond only in Korean."
)
MODEL = "gpt-4.1-nano"


# ─────────── 유틸: 월 구간 생성 ───────────────────────────────────
def _first_of_month(dt: datetime) -> datetime:
    return dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)


def _first_of_next_month(dt: datetime) -> datetime:
    # 28일 트릭으로 다음 달 1일 구하기
    return (dt.replace(day=28) + timedelta(days=4)).replace(
        day=1, hour=0, minute=0, second=0, microsecond=0
    )


@dag(
    start_date=datetime(2025, 7, 4),
    schedule="@once",
    catchup=False,
    tags=["openai", "summarize", "batch"],
    max_active_tasks=1,
    # DAG 기본 파라미터는 문자열로! (JSON-serializable)
    params={"start_date": "2023-04-01", "end_date": "2023-05-31"},
)
def summarize_disclosure_events_batch_dag():
    """일괄 Batch 요약 DAG (월 단위 청크 처리)"""

    @task(task_id="build_month_ranges")
    def build_month_ranges(start_date_str: str, end_date_str: str) -> list[dict]:
        """문자열 → 내부에서 datetime 파싱, 결과는 다시 문자열로 반환 (XCom 안전)"""
        # 파싱
        start_dt = datetime.fromisoformat(start_date_str)
        # end는 날짜만 들어오면 그 날의 23:59:59로 해석
        end_base = datetime.fromisoformat(end_date_str)
        end_dt = end_base.replace(hour=23, minute=59, second=59, microsecond=999999)

        # 월별 구간 계산
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
            start = max(cur, start_dt)
            end = min(end_dt, next_first - timedelta(microseconds=1))
            # ✅ 반환은 문자열(ISO)로
            out.append({"start_date": start.isoformat(), "end_date": end.isoformat()})
            cur = next_first

        print(
            f"Generated {len(out)} month ranges from {start_date_str} to {end_date_str}"
        )
        print(f"First range: {out[0] if out else 'None'}")
        return out

    @task(task_id="fetch_events_to_summarize")
    def fetch_events_to_summarize(
        start_date: str | None = None, end_date: str | None = None
    ) -> list[dict]:
        """여기서 문자열 → datetime은 DB 쿼리 바인딩 직전에만 변환해도 되지만,
        우리는 그냥 문자열 그대로 바인딩(타임존 변환이 SQL안에서 처리되므로 OK)"""
        sql = """
            SELECT id, raw
            FROM disclosure_events
            WHERE raw IS NOT NULL
              AND raw <> ''
              AND disclosed_at AT TIME ZONE 'Asia/Seoul' >= :start_date
              AND disclosed_at AT TIME ZONE 'Asia/Seoul' <= :end_date
            ORDER BY disclosed_at, id
        """
        with ENGINE.connect() as conn:
            # 문자열(ISO) 그대로 바인딩 → DB 드라이버가 timestamp로 캐스팅
            result = conn.execute(
                sa.text(sql), {"start_date": start_date, "end_date": end_date}
            )
            events = [{"id": row.id, "raw": row.raw} for row in result]
            print(f"Fetched {len(events)} events for period {start_date} to {end_date}")
            return events

    # ④ JSONL 파일 작성 & OpenAI 업로드(월별)
    @task(task_id="upload_batch_file")
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

    # ⑤ Batch Job 생성(월별)
    @task(task_id="create_batch_job")
    def create_batch_job(file_id: str | None) -> str | None:
        if file_id is None:
            return None
        batch = client.batches.create(
            input_file_id=file_id,
            endpoint="/v1/chat/completions",
            completion_window="24h",
        )
        return batch.id

    # ⑥ 완료 대기 & 결과 다운로드(월별)
    @task(task_id="wait_and_download_batch")
    def wait_and_download_batch(
        batch_id: str | None, poll_interval: int = 30
    ) -> list[dict]:
        if batch_id is None:
            return []

        while True:
            batch = client.batches.retrieve(batch_id)
            if batch.status in {"completed", "failed", "expired"}:
                break
            time.sleep(poll_interval)

        if batch.status != "completed":
            print(f"Batch failed with status: {batch.status}")
            return []

        output_file_id: str | None = batch.output_file_id
        if not output_file_id:
            print("No output file found")
            return []

        txt = client.files.retrieve_content(output_file_id)
        ok, failed = [], []
        for line in txt.splitlines():
            obj = json.loads(line)
            cid = int(obj["custom_id"])  # DB PK

            if obj.get("error") is None and obj["response"]["status_code"] == 200:
                content = (
                    obj["response"]["body"]["choices"][0]["message"]["content"]
                    if obj["response"]["body"]["choices"]
                    else None
                )
                ok.append({"id": cid, "summary": content})
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

        print(f"Successfully processed {len(ok)} summaries")
        return ok

    # ⑦ DB 업데이트(월별)
    @task(task_id="update_summaries")
    def update_summarize(summaries: list[dict]) -> None:
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

    # 템플릿으로 문자열 파라미터 주입
    month_ranges = build_month_ranges(
        start_date_str="{{ params.start_date }}",
        end_date_str="{{ params.end_date }}",
    )

    # ✅ 리스트[dict]를 매핑할 때는 expand_kwargs 사용
    evts = fetch_events_to_summarize.expand_kwargs(month_ranges)
    file_id = upload_batch_file.expand(events=evts)
    batch_id = create_batch_job.expand(file_id=file_id)
    summaries = wait_and_download_batch.expand(batch_id=batch_id)
    upd = update_summarize.expand(summaries=summaries)

    # 의존성 설정
    month_ranges >> evts >> file_id >> batch_id >> summaries >> upd


# DAG 인스턴스 (기본 기간은 예시, 트리거 시 파라미터로 바꿔도 됨)
summarize_dag = summarize_disclosure_events_batch_dag()
