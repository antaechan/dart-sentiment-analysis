from __future__ import annotations

"""
masked된 공시 텍스트에서 해당 공시가 발표된 후 주가에 미칠 영향을 판단하여 0(positive), 1(negative)로 라벨링한다.
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

# 라벨링을 위한 시스템 프롬프트
LABELING_SYSTEM_PROMPT = (
    "당신은 한국 기업 공시 텍스트를 분석하여 해당 공시가 발표된 후 주가에 미칠 영향을 판단하는 작업을 수행합니다. "
    "공시 내용을 분석하여 주가에 긍정적인 영향을 미칠 것으로 예상되면 '0', "
    "중립적인 영향을 미칠 것으로 예상되면 '1', "
    "부정적인 영향을 미칠 것으로 예상되면 '2'로 응답해주세요. "
    "숫자만 반환하고 다른 설명은 하지 마세요."
)
MODEL = "gpt-5-nano"


@dag(
    start_date=datetime(2025, 7, 4),
    schedule="@once",
    catchup=False,
    tags=["openai", "labeling", "batch"],
    max_active_tasks=1,
    # DAG 기본 파라미터는 문자열로! (JSON-serializable)
    params={"start_date": "2022-07-01", "end_date": "2023-12-31"},
)
def label_disclosure_events_by_gpt_batch_dag():
    """일괄 Batch GPT로 라벨링 DAG (월 단위 청크 처리)"""

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

    @task(task_id="fetch_masked_events")
    def fetch_masked_events(
        start_date: str | None = None, end_date: str | None = None
    ) -> list[dict]:
        """label 테이블에서 masked 필드가 있는 데이터를 가져와서 라벨링 처리"""
        sql = """
            SELECT l.id, l.masked
            FROM label l
            JOIN disclosure_events de ON l.id = de.id
            WHERE l.masked IS NOT NULL
              AND l.masked <> ''
              AND de.disclosed_at AT TIME ZONE 'Asia/Seoul' >= :start_date
              AND de.disclosed_at AT TIME ZONE 'Asia/Seoul' <= :end_date
            ORDER BY de.disclosed_at, l.id
        """
        with ENGINE.connect() as conn:
            # 문자열(ISO) 그대로 바인딩 → DB 드라이버가 timestamp로 캐스팅
            result = conn.execute(
                sa.text(sql), {"start_date": start_date, "end_date": end_date}
            )
            events = [{"id": row.id, "masked": row.masked} for row in result]
            print(
                f"Fetched {len(events)} masked events for labeling period {start_date} to {end_date}"
            )
            return events

    # ④ JSONL 파일 작성 & OpenAI 업로드(월별) - 마스킹용
    @task(task_id="upload_masking_batch_file")
    def upload_masking_batch_file(events: list[dict]) -> str | None:
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
                            {"role": "system", "content": LABELING_SYSTEM_PROMPT},
                            {"role": "user", "content": ev["masked"]},
                        ],
                    },
                }
                fp.write(json.dumps(payload, ensure_ascii=False) + "\n")
            fp.flush()
            file_resp = client.files.create(file=open(fp.name, "rb"), purpose="batch")
        return file_resp.id

    # ⑤ 배치 작업 생성, 완료 대기 & 결과 다운로드(월별) - 마스킹용
    @task(task_id="process_masking_batch", max_active_tis_per_dag=2)
    def process_masking_batch(
        file_id: str | None, poll_interval: int = 30
    ) -> list[dict]:
        """배치 작업을 생성하고 완료될 때까지 대기한 후 결과를 다운로드"""
        if file_id is None:
            return []

        # 배치 작업 생성
        batch = client.batches.create(
            input_file_id=file_id,
            endpoint="/v1/chat/completions",
            completion_window="24h",
        )
        batch_id = batch.id
        print(f"Created batch job: {batch_id}")

        # 완료 대기
        while True:
            batch = client.batches.retrieve(batch_id)
            if batch.status in {"completed", "failed", "expired"}:
                break
            time.sleep(poll_interval)

        if batch.status != "completed":
            print(f"Batch failed with status: {batch.status}")
            return []

        # 결과 다운로드
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
                ok.append({"id": cid, "label": content})
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

        print(f"Successfully processed {len(ok)} masked texts")
        return ok

    # ⑦ 라벨링된 데이터를 label 테이블에 저장(월별)
    @task(task_id="save_labeled_data")
    def save_labeled_data(labeled_results: list[dict]) -> None:
        if not labeled_results:
            print("No labeled data to save")
            return

        # 원본 summary_kr과 masked 데이터도 함께 가져와서 저장
        with ENGINE.begin() as conn:
            for result in labeled_results:
                # 원본 summary_kr과 masked 가져오기
                original_sql = """
                    SELECT summary_kr, masked FROM label WHERE id = :id
                """
                original_result = conn.execute(
                    sa.text(original_sql), {"id": result["id"]}
                ).fetchone()

                if original_result:
                    # label 테이블에 데이터 저장 (UPSERT)
                    upsert_sql = """
                        INSERT INTO label (id, summary_kr, masked, label)
                        VALUES (:id, :summary_kr, :masked, :label)
                        ON CONFLICT (id) 
                        DO UPDATE SET 
                            summary_kr = EXCLUDED.summary_kr,
                            masked = EXCLUDED.masked,
                            label = EXCLUDED.label,
                            created_at = CURRENT_TIMESTAMP
                    """
                    conn.execute(
                        sa.text(upsert_sql),
                        {
                            "id": result["id"],
                            "summary_kr": original_result.summary_kr,
                            "masked": original_result.masked,
                            "label": result["label"],  # 라벨 필드에 라벨링 결과 저장
                        },
                    )

        print(f"Saved {len(labeled_results)} labeled records to label table.")

    # 템플릿으로 문자열 파라미터 주입
    month_ranges = build_month_ranges(
        start_date_str="{{ params.start_date }}",
        end_date_str="{{ params.end_date }}",
    )

    # ✅ 리스트[dict]를 매핑할 때는 expand_kwargs 사용
    evts = fetch_masked_events.expand_kwargs(month_ranges)
    file_id = upload_masking_batch_file.expand(events=evts)
    masked_results = process_masking_batch.expand(file_id=file_id)
    save_results = save_labeled_data.expand(labeled_results=masked_results)

    # 의존성 설정
    (month_ranges >> evts >> file_id >> masked_results >> save_results)


# DAG 인스턴스 (기본 기간은 예시, 트리거 시 파라미터로 바꿔도 됨)
labeling_dag = label_disclosure_events_by_gpt_batch_dag()
