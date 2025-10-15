from __future__ import annotations

"""Airflow DAG: summarize_disclosure_events_batch_dag (monthly-chunked)

요약 대상 공시(raw 텍스트)를 OpenAI Batch API로 월 단위로 일괄 요약하여
PostgreSQL `disclosure_events.summary_kr` 컬럼에 저장한다.
- ❶ summary 컬럼 보장 → (아래 월 반복)
  ❷ 대상 조회 → ❸ JSONL 업로드 → ❹ Batch Job 생성 →
  ❺ 완료 대기 → ❻ 결과 다운로드 → ❼ DB 업데이트
"""
import io
import json
import os
import tempfile
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
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

# 테이블명 상수
DISCLOSURE_EVENTS_TABLE = "kind"

SYSTEM_PROMPT = (
    "You are a helpful assistant to summarize the given text about a Korean corporate "
    "report within 2~3 sentences. Please respond only in Korean."
)
MODEL = "gpt-4.1-nano"

KST = ZoneInfo("Asia/Seoul")
UTC = ZoneInfo("UTC")


@dag(
    start_date=datetime(2025, 7, 4),
    schedule="@once",
    catchup=False,
    tags=["openai", "summarize", "batch"],
    max_active_tasks=1,
    # DAG 기본 파라미터는 문자열로! (JSON-serializable)
    params={"start_date": "2021-01-01", "end_date": "2021-01-31"},
)
def summarize_disclosure_events_batch_dag():
    """일괄 Batch 요약 DAG (월 단위 청크 처리)"""

    @task(task_id="build_month_ranges")
    def build_month_ranges(start_date_str: str, end_date_str: str) -> list[dict]:
        """
        입력 문자열(일자/ISO)을 KST로 해석해 '월 단위' 경계를 만든 뒤,
        각 구간의 시작/끝을 UTC로 변환해 ISO 문자열로 반환.
        (DB는 disclosed_at UTC 저장이므로 UTC 경계가 인덱스 친화적)
        """
        # KST로 파싱
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
            # 이미 시간이 있다면 그 시간 기준으로 KST에 정렬 후 미세 조정 없음
            end_dt = end_base.astimezone(KST)

        # 월별 구간 (KST 기준)
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

            # ✅ DB 비교용 UTC 경계로 변환해 ISO 문자열로 반환
            utc_start = kst_start.astimezone(UTC).isoformat()
            utc_end = kst_end.astimezone(UTC).isoformat()

            out.append({"start_date": utc_start, "end_date": utc_end})
            cur = next_first

        print(
            f"Generated {len(out)} month ranges (KST→UTC) from {start_date_str} to {end_date_str}"
        )
        print(f"First UTC range: {out[0] if out else 'None'}")
        return out

    @task(task_id="fetch_events_to_summarize")
    def fetch_events_to_summarize(
        start_date: str | None = None, end_date: str | None = None
    ) -> list[dict]:
        """여기서 문자열 → datetime은 DB 쿼리 바인딩 직전에만 변환해도 되지만,
        우리는 그냥 문자열 그대로 바인딩(타임존 변환이 SQL안에서 처리되므로 OK)"""

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

        sql = f"""
            SELECT id, raw
            FROM {DISCLOSURE_EVENTS_TABLE}
            WHERE raw IS NOT NULL
            AND raw <> ''
            AND disclosed_at BETWEEN :utc_start AND :utc_end
        """
        with ENGINE.connect() as conn:
            result = conn.execute(sa.text(sql), {"utc_start": s, "utc_end": e})
            events = [{"id": row.id, "raw": row.raw} for row in result]

        print(
            f"Fetched {len(events)} events for UTC period {s.isoformat()} → {e.isoformat()}"
        )
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
                        "max_tokens": 128,
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
        """
        벌크 업데이트: 임시테이블에 COPY로 적재 → UPDATE ... FROM 로 한번에 반영
        - 루프 UPDATE 제거로 왕복/락/플랜 비용 대폭 절감
        """

        if not summaries:
            print("No summaries to update")
            return

        # 1) 메모리에서 탭 구분값(TSV) 생성 (COPY용)
        #    탭/개행은 COPY에서 문제되므로 공백으로 정규화
        buf = io.StringIO()
        count = 0
        for s in summaries:
            sid = int(s["id"])
            summary = (
                (s.get("summary") or "").replace("\t", " ").replace("\n", " ").strip()
            )
            buf.write(f"{sid}\t{summary}\n")
            count += 1
        buf.seek(0)

        # 2) 트랜잭션 내에서: 임시테이블 생성 → COPY → UPDATE ... FROM
        with ENGINE.begin() as conn:
            # 임시테이블 (트랜잭션 끝나면 자동 제거)
            conn.execute(
                sa.text(
                    """
                CREATE TEMP TABLE tmp_sum (
                    id BIGINT PRIMARY KEY,
                    summary TEXT
                ) ON COMMIT DROP;
            """
                )
            )

            # DBAPI 커넥션 꺼내서 고속 COPY 수행
            # SQLAlchemy 2.x: driver_connection으로 DBAPI 커넥션 접근
            dbapi_conn = conn.connection.driver_connection
            with dbapi_conn.cursor() as cur:
                # 기본 TEXT COPY (탭 구분) 사용
                cur.copy_from(
                    file=buf,
                    table="tmp_sum",
                    sep="\t",
                    columns=("id", "summary"),
                )

            # 3) 한 방 UPDATE
            conn.execute(
                sa.text(
                    f"""
                UPDATE {DISCLOSURE_EVENTS_TABLE} AS d
                SET summary_kr = t.summary
                FROM tmp_sum AS t
                WHERE d.id = t.id;
            """
                )
            )

        print(f"Bulk-updated {count} rows.")

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
