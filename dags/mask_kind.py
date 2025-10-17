from __future__ import annotations

"""
공시 텍스트에서 구체적인 회사명을 마스킹하여 회사 A, 회사 B 이런 식으로 저장한다.
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

# 마스킹을 위한 시스템 프롬프트
MASKING_SYSTEM_PROMPT = (
    "당신은 한국 기업 공시 텍스트에서 구체적인 회사명을 마스킹하는 작업을 수행합니다. "
    "텍스트에서 언급된 모든 회사명을 '회사 A', '회사 B', '회사 C' 등의 형태로 순서대로 마스킹해주세요. "
    "같은 회사가 여러 번 언급되면 동일한 마스킹을 사용하세요. "
    "마스킹된 텍스트만 반환하고 다른 설명은 하지 마세요."
)
MODEL = "gpt-4.1-nano"

KST = ZoneInfo("Asia/Seoul")
UTC = ZoneInfo("UTC")


@dag(
    start_date=datetime(2025, 7, 4),
    schedule="@once",
    catchup=False,
    tags=["openai", "masking", "batch"],
    max_active_tasks=1,
    params={"start_date": "2021-07-01", "end_date": "2021-12-30"},
)
def mask_kind_disclosure_events_dag():
    """일괄 Batch 마스킹 DAG (월 단위 청크 처리)"""

    @task(task_id="alter_kind_table")
    def alter_kind_table_if_not_exists() -> None:
        """kind 테이블에 masked 필드가 없으면 추가하고 기존 데이터에 '' 값 설정"""
        sql = """
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns 
                    WHERE table_name = 'kind' AND column_name = 'masked'
                ) THEN
                    ALTER TABLE kind ADD COLUMN masked TEXT DEFAULT '';
                    UPDATE kind SET masked = '' WHERE masked IS NULL;
                END IF;
            END $$;
        """
        with ENGINE.begin() as conn:
            conn.execute(sa.text(sql))
        print("Checked and added masked column to kind table if needed")

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

    @task(task_id="fetch_events_to_mask")
    def fetch_events_to_mask(
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
            SELECT id, summary_kr, disclosed_at
            FROM kind
            WHERE summary_kr IS NOT NULL
              AND summary_kr <> ''
              AND disclosed_at BETWEEN :utc_start AND :utc_end
        """
        with ENGINE.connect() as conn:
            result = conn.execute(sa.text(sql), {"utc_start": s, "utc_end": e})
            events = [
                {
                    "id": row.id,
                    "summary_kr": row.summary_kr,
                    "disclosed_at": row.disclosed_at,
                }
                for row in result
            ]

        print(
            f"Fetched {len(events)} events for UTC period {s.isoformat()} → {e.isoformat()}"
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
                            {"role": "system", "content": MASKING_SYSTEM_PROMPT},
                            {"role": "user", "content": ev["summary_kr"]},
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
                ok.append({"id": cid, "masked": content})
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

    # ⑦ 마스킹된 데이터를 kind 테이블에 저장(월별) - 벌크 업데이트
    def save_masked_data(masked_results: list[dict]) -> None:
        """
        벌크 업데이트: 임시테이블에 COPY로 적재 → UPDATE ... FROM 로 한번에 반영
        - 루프 UPDATE 제거로 왕복/락/플랜 비용 대폭 절감
        """
        if not masked_results:
            print("No masked data to save")
            return

        # 1) 메모리에서 탭 구분값(TSV) 생성 (COPY용)
        #    탭/개행은 COPY에서 문제되므로 공백으로 정규화
        buf = io.StringIO()
        count = 0
        for result in masked_results:
            rid = int(result["id"])
            masked = (
                (result.get("masked") or "")
                .replace("\t", " ")
                .replace("\n", " ")
                .strip()
            )
            buf.write(f"{rid}\t{masked}\n")
            count += 1
        buf.seek(0)

        # 2) 트랜잭션 내에서: 임시테이블 생성 → COPY → UPDATE ... FROM
        with ENGINE.begin() as conn:
            # 임시테이블 (트랜잭션 끝나면 자동 제거)
            conn.execute(
                sa.text(
                    """
                CREATE TEMP TABLE tmp_masked (
                    id BIGINT PRIMARY KEY,
                    masked TEXT
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
                    table="tmp_masked",
                    sep="\t",
                    columns=("id", "masked"),
                )

            # 3) 한 방 UPDATE
            conn.execute(
                sa.text(
                    """
                UPDATE kind AS k
                SET masked = t.masked
                FROM tmp_masked AS t
                WHERE k.id = t.id;
            """
                )
            )

        print(f"Bulk-updated {count} masked records in kind table.")

    # ⑧ 마스킹 배치 처리와 저장을 하나의 task로 묶은 함수
    @task(task_id="process_and_save_masking", max_active_tis_per_dag=1)
    def process_and_save_masking(file_id: str | None, poll_interval: int = 30) -> None:
        """process_masking_batch와 save_masked_data를 하나의 task로 묶은 함수"""
        # 1. process_masking_batch 실행
        masked_results = process_masking_batch(file_id, poll_interval)

        # 2. save_masked_data 실행
        save_masked_data(masked_results)

    # 템플릿으로 문자열 파라미터 주입
    month_ranges = build_month_ranges(
        start_date_str="{{ params.start_date }}",
        end_date_str="{{ params.end_date }}",
    )

    # ✅ 리스트[dict]를 매핑할 때는 expand_kwargs 사용
    alter_table_task = alter_kind_table_if_not_exists()
    evts = fetch_events_to_mask.expand_kwargs(month_ranges)
    file_id = upload_masking_batch_file.expand(events=evts)
    process_and_save_results = process_and_save_masking.expand(file_id=file_id)

    # 의존성 설정
    (alter_table_task >> month_ranges >> evts >> file_id >> process_and_save_results)


# DAG 인스턴스 (기본 기간은 예시, 트리거 시 파라미터로 바꿔도 됨)
masking_dag = mask_kind_disclosure_events_dag()
