from __future__ import annotations

"""
kind 테이블에서 masked된 공시 텍스트에서 해당 공시가 발표된 후 주가에 미칠 영향을 판단하여 1(positive), 0(neutral), -1(negative)로 라벨링한다.
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

# 라벨링을 위한 시스템 프롬프트
LABELING_SYSTEM_PROMPT = "You will analyze Korean corporate disclosure texts to determine the expected impact on stock prices after the disclosure. If the disclosure is expected to have a positive impact on stock prices, respond with '1'. If it is expected to have a neutral impact, respond with '0'. If it is expected to have a negative impact, respond with '-1'. Return only the number without any further explanation."
MODEL = "gpt-4.1-nano"

KST = ZoneInfo("Asia/Seoul")
UTC = ZoneInfo("UTC")


@dag(
    start_date=datetime(2025, 7, 4),
    schedule="@once",
    catchup=False,
    tags=["openai", "labeling", "batch"],
    max_active_tasks=1,
    # DAG 기본 파라미터는 문자열로! (JSON-serializable)
    params={"start_date": "2022-01-01", "end_date": "2022-06-30"},
)
def label_kind_disclosure_events_by_gpt_batch_dag():
    """일괄 Batch GPT로 라벨링 DAG (월 단위 청크 처리)"""

    @task(task_id="alter_kind_table")
    def alter_kind_table_if_not_exists() -> None:
        """kind 테이블에 label 필드가 없으면 추가하고 기존 데이터에 NULL 값 설정"""
        sql = """
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns 
                    WHERE table_name = 'kind' AND column_name = 'label'
                ) THEN
                    ALTER TABLE kind ADD COLUMN label INTEGER DEFAULT NULL;
                END IF;
            END $$;
        """
        with ENGINE.begin() as conn:
            conn.execute(sa.text(sql))
        print("Checked and added label column to kind table if needed")

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

    @task(task_id="fetch_masked_events")
    def fetch_masked_events(
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
            SELECT id, masked
            FROM kind
            WHERE masked IS NOT NULL
              AND masked <> ''
              AND disclosed_at BETWEEN :utc_start AND :utc_end
        """
        with ENGINE.connect() as conn:
            result = conn.execute(sa.text(sql), {"utc_start": s, "utc_end": e})
            events = [{"id": row.id, "masked": row.masked} for row in result]

        print(
            f"Fetched {len(events)} masked events for UTC period {s.isoformat()} → {e.isoformat()}"
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

    # ⑦ 라벨링된 데이터를 kind 테이블에 저장(월별) - 벌크 업데이트
    def save_labeled_data(labeled_results: list[dict]) -> None:
        """
        벌크 업데이트: 임시테이블에 COPY로 적재 → UPDATE ... FROM 로 한번에 반영
        - 루프 UPDATE 제거로 왕복/락/플랜 비용 대폭 절감
        - 라벨 값 검증도 벌크 처리 과정에서 수행
        """
        if not labeled_results:
            print("No labeled data to save")
            return

        # 1) 메모리에서 탭 구분값(TSV) 생성 (COPY용)
        #    라벨 값 검증도 이 과정에서 수행
        buf = io.StringIO()
        count = 0
        invalid_count = 0

        for result in labeled_results:
            rid = int(result["id"])
            label_value = result["label"]

            # 라벨 값 검증 및 변환
            validated_label = None
            try:
                if label_value and label_value.strip():
                    int_label = int(label_value.strip())
                    # 1, 0, -1 중 하나인지 확인
                    if int_label in [1, 0, -1]:
                        validated_label = int_label
                    else:
                        print(
                            f"Invalid label value {int_label} for id {rid}, setting to null"
                        )
                        invalid_count += 1
                else:
                    print(f"Empty label value for id {rid}, setting to null")
                    invalid_count += 1
            except (ValueError, TypeError):
                print(
                    f"Non-integer label value '{label_value}' for id {rid}, setting to null"
                )
                invalid_count += 1

            # NULL 값은 \N으로 저장 (PostgreSQL COPY에서 NULL 처리)
            label_str = str(validated_label) if validated_label is not None else "\\N"
            buf.write(f"{rid}\t{label_str}\n")
            count += 1

        buf.seek(0)

        # 2) 트랜잭션 내에서: 임시테이블 생성 → COPY → UPDATE ... FROM
        with ENGINE.begin() as conn:
            # 임시테이블 (트랜잭션 끝나면 자동 제거)
            conn.execute(
                sa.text(
                    """
                CREATE TEMP TABLE tmp_labeled (
                    id BIGINT PRIMARY KEY,
                    label INTEGER
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
                    table="tmp_labeled",
                    sep="\t",
                    columns=("id", "label"),
                )

            # 3) 한 방 UPDATE (INTEGER 필드 직접 업데이트)
            conn.execute(
                sa.text(
                    """
                UPDATE kind AS k
                SET label = t.label
                FROM tmp_labeled AS t
                WHERE k.id = t.id;
            """
                )
            )

        print(f"Bulk-updated {count} labeled records in kind table.")
        if invalid_count > 0:
            print(f"Found {invalid_count} invalid labels set to NULL.")

    # ⑧ 통합 태스크: 배치 처리와 데이터 저장을 함께 수행
    @task(task_id="process_and_save_masking_batch", max_active_tis_per_dag=2)
    def process_and_save_masking_batch(
        file_id: str | None, poll_interval: int = 30
    ) -> None:
        """배치 작업을 생성하고 완료될 때까지 대기한 후 결과를 다운로드하고 DB에 저장"""
        # 배치 처리 수행
        labeled_results = process_masking_batch(file_id, poll_interval)

        # 결과를 DB에 저장
        save_labeled_data(labeled_results)

    # 템플릿으로 문자열 파라미터 주입
    month_ranges = build_month_ranges(
        start_date_str="{{ params.start_date }}",
        end_date_str="{{ params.end_date }}",
    )

    # ✅ 리스트[dict]를 매핑할 때는 expand_kwargs 사용
    alter_table_task = alter_kind_table_if_not_exists()
    evts = fetch_masked_events.expand_kwargs(month_ranges)
    file_id = upload_masking_batch_file.expand(events=evts)
    process_and_save_results = process_and_save_masking_batch.expand(file_id=file_id)

    # 의존성 설정
    (alter_table_task >> month_ranges >> evts >> file_id >> process_and_save_results)


# DAG 인스턴스 (기본 기간은 예시, 트리거 시 파라미터로 바꿔도 됨)
labeling_dag = label_kind_disclosure_events_by_gpt_batch_dag()
