from __future__ import annotations

"""
masked된 공시 텍스트에서 해당 공시의 타입을 분류하여 disclosure_type 필드에 저장한다.
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

# 공시 타입 분류를 위한 시스템 프롬프트
SYSTEM_PROMPT = """당신은 한국 기업 공시를 분류하는 전문가입니다.  
아래에 제시된 공시 요약 텍스트를 읽고, 해당 공시가 어떤 종류의 공시인지 분류하세요.

- 카테고리 예시:  
  1) 감자
  2) 국책과제
  3) 기술료 수령
  4) 기술 이전  
  5) 매출 변동  
  6) 배당  
  7) 소송
  8) 신규투자
  9) 영업양수
  10) 자산양수
  11) 영업정지
  12) 유무상증자
  13) 임상 시험 결과 발표
  14) 임상 시험 계획 철회
  15) 임상 시험 계획 승인
  16) 임상 시험 계획 신청
  17) 자사주 취득
  18) 자사주 처분
  19) 자사주 소각
  20) 타법인 주식취득
  21) 전환사채 발행
  22) 전환청구권 행사
  23) 전환가액 조정
  24) 주식 교환 및 이전
  25) 특허권 취득
  26) 품목 허가 승인
  27) 품목 허가 신청
  28) 회사 분할
  29) 회사 합병
  30) 기업설명회
  31) 공급계약체결
  32) 실적공시
  33) 공정공시
  34) 조회공시
  35) 지분공시
  
- 위의 예시에 해당하지 않는 경우, 새로운 카테고리를 스스로 정의하여 출력하세요.  
- 출력은 번호를 제외한 **카테고리명만** 작성합니다.  
- 불필요한 설명이나 이유는 포함하지 마세요."""

MODEL = "gpt-5-nano"

# report_name 키워드 기반 분류 매핑 (config.py 방식)
REPORT_NAME_KEYWORD_MAPPING = {
    "기업설명회": [
        "기업설명회",
        "IR개최",
        "투자자관계",
        "IR활동",
    ],
    "공급계약체결": [
        "단일판매ㆍ공급계약체결",
        "계약체결",
        "공급계약",
        "판매계약",
        "공급결정",
        "수주",
        "수주계약",
    ],
    "실적공시": [
        "영업실적",
        "잠정실적",
        "연결재무제표",
        "실적공시",
        "영업(잠정)실적",
        "분기보고서",
        "반기보고서",
        "사업보고서",
    ],
    "공정공시": [
        "공정공시",
        "공정",
    ],
    "조회공시": [
        "조회공시요구",
        "조회공시",
        "답변",
        "현저한시황변동",
        "풍문또는보도",
    ],
    "지분공시": [
        "최대주주변경",
        "최대주주등소유주식변동신고서",
        "주주변경",
        "주식변동",
        "주식등의대량보유상황보고서",
        "임원ㆍ주요주주특정증권등소유상황보고서",
    ],
}


@dag(
    start_date=datetime(2025, 7, 4),
    schedule="@once",
    catchup=False,
    tags=["openai", "classification", "batch"],
    max_active_tasks=1,
    # DAG 기본 파라미터는 문자열로! (JSON-serializable)
    params={"start_date": "2022-07-01", "end_date": "2023-12-31"},
)
def classify_disclosure_events_by_gpt_batch_dag():
    """일괄 Batch GPT로 공시 타입 분류 DAG (월 단위 청크 처리)"""

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

    @task(task_id="ensure_disclosure_type_column")
    def ensure_disclosure_type_column() -> None:
        """label 테이블에 disclosure_type 컬럼이 없으면 추가"""
        with ENGINE.begin() as conn:
            # 컬럼 존재 여부 확인
            check_sql = """
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = 'label' AND column_name = 'disclosure_type'
            """
            result = conn.execute(sa.text(check_sql)).fetchone()

            if not result:
                # 컬럼이 없으면 추가
                alter_sql = """
                    ALTER TABLE label ADD COLUMN disclosure_type VARCHAR(100)
                """
                conn.execute(sa.text(alter_sql))
                print("Added disclosure_type column to label table")
            else:
                print("disclosure_type column already exists in label table")

    @task(task_id="ensure_report_name_column")
    def ensure_report_name_column() -> None:
        """label 테이블에 report_name 컬럼이 없으면 추가"""
        with ENGINE.begin() as conn:
            # 컬럼 존재 여부 확인
            check_sql = """
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = 'label' AND column_name = 'report_name'
            """
            result = conn.execute(sa.text(check_sql)).fetchone()

            if not result:
                # 컬럼이 없으면 추가
                alter_sql = """
                    ALTER TABLE label ADD COLUMN report_name TEXT
                """
                conn.execute(sa.text(alter_sql))
                print("Added report_name column to label table")
            else:
                print("report_name column already exists in label table")

    @task(task_id="update_report_names")
    def update_report_names() -> None:
        """disclosure_events 테이블에서 report_name을 가져와서 label 테이블에 업데이트"""
        with ENGINE.begin() as conn:
            # label 테이블의 id와 disclosure_events 테이블의 id가 일치하는
            # report_name을 업데이트
            update_sql = """
                UPDATE label 
                SET report_name = de.report_name
                FROM disclosure_events de
                WHERE label.id = de.id
                  AND de.report_name IS NOT NULL
                  AND de.report_name <> ''
            """
            result = conn.execute(sa.text(update_sql))
            print(
                f"Updated {result.rowcount} records with report_name from disclosure_events"
            )

    def classify_by_report_name_keywords(report_name: str | None) -> str | None:
        """report_name을 기반으로 키워드 매칭하여 disclosure_type을 반환"""
        if not report_name:
            return None

        # 키워드 매핑에서 일치하는 항목 찾기
        for disclosure_type, keywords in REPORT_NAME_KEYWORD_MAPPING.items():
            for keyword in keywords:
                if keyword in report_name:
                    return disclosure_type
        return None

    @task(task_id="classify_by_keywords")
    def classify_by_keywords() -> None:
        """report_name 키워드를 기반으로 disclosure_type을 미리 할당"""
        with ENGINE.begin() as conn:
            # report_name이 있지만 disclosure_type이 없는 레코드들을 가져와서 키워드 기반 분류
            select_sql = """
                SELECT l.id, de.report_name
                FROM label l
                JOIN disclosure_events de ON l.id = de.id
                WHERE de.report_name IS NOT NULL 
                  AND de.report_name <> ''
                  AND (l.disclosure_type IS NULL OR l.disclosure_type = '')
            """
            result = conn.execute(sa.text(select_sql))
            records = result.fetchall()

            updated_count = 0
            for record in records:
                disclosure_type = classify_by_report_name_keywords(record.report_name)
                if disclosure_type:
                    update_sql = """
                        UPDATE label 
                        SET disclosure_type = :disclosure_type
                        WHERE id = :id
                    """
                    conn.execute(
                        sa.text(update_sql),
                        {"id": record.id, "disclosure_type": disclosure_type},
                    )
                    updated_count += 1

            print(
                f"Updated {updated_count} records with keyword-based disclosure_type classification"
            )

    @task(task_id="fetch_masked_events")
    def fetch_masked_events(
        start_date: str | None = None, end_date: str | None = None
    ) -> list[dict]:
        """label 테이블에서 masked 필드가 있는 데이터를 가져와서 공시 타입 분류 처리
        (키워드 기반으로 이미 분류된 항목들은 제외)"""
        sql = """
            SELECT l.id, l.masked
            FROM label l
            JOIN disclosure_events de ON l.id = de.id
            WHERE l.masked IS NOT NULL
              AND l.masked <> ''
              AND (l.disclosure_type IS NULL OR l.disclosure_type = '')
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
                f"Fetched {len(events)} masked events for disclosure type classification period {start_date} to {end_date}"
            )
            return events

    # ④ JSONL 파일 작성 & OpenAI 업로드(월별) - 공시 타입 분류용
    @task(task_id="upload_classification_batch_file")
    def upload_classification_batch_file(events: list[dict]) -> str | None:
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
                            {"role": "user", "content": ev["masked"]},
                        ],
                    },
                }
                fp.write(json.dumps(payload, ensure_ascii=False) + "\n")
            fp.flush()
            file_resp = client.files.create(file=open(fp.name, "rb"), purpose="batch")
        return file_resp.id

    # ⑤ 배치 작업 생성, 완료 대기 & 결과 다운로드(월별) - 공시 타입 분류용
    def process_classification_batch(
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
                ok.append({"id": cid, "disclosure_type": content})
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

        print(f"Successfully processed {len(ok)} disclosure type classifications")
        return ok

    # ⑦ 공시 타입 분류된 데이터를 label 테이블에 저장(월별)
    def save_labeled_data(labeled_results: list[dict]) -> None:
        if not labeled_results:
            print("No labeled data to save")
            return

        with ENGINE.begin() as conn:
            for result in labeled_results:
                # disclosure_type 값 처리
                disclosure_type_value = result["disclosure_type"]
                if disclosure_type_value and disclosure_type_value.strip():
                    disclosure_type_value = disclosure_type_value.strip()
                else:
                    print(
                        f"Empty disclosure_type value for id {result['id']}, setting to null"
                    )
                    disclosure_type_value = None

                # label 테이블에 disclosure_type만 업데이트
                update_sql = """
                    UPDATE label 
                    SET disclosure_type = :disclosure_type
                    WHERE id = :id
                """
                conn.execute(
                    sa.text(update_sql),
                    {
                        "id": result["id"],
                        "disclosure_type": disclosure_type_value,
                    },
                )

        print(
            f"Saved {len(labeled_results)} disclosure type classified records to label table."
        )

    # ⑧ 통합 태스크: 배치 처리와 데이터 저장을 함께 수행
    @task(task_id="process_and_save_classification_batch", max_active_tis_per_dag=2)
    def process_and_save_classification_batch(
        file_id: str | None, poll_interval: int = 30
    ) -> None:
        """배치 작업을 생성하고 완료될 때까지 대기한 후 결과를 다운로드하고 DB에 저장"""
        # 배치 처리 수행
        labeled_results = process_classification_batch(file_id, poll_interval)

        # 결과를 DB에 저장
        save_labeled_data(labeled_results)

    # 템플릿으로 문자열 파라미터 주입
    month_ranges = build_month_ranges(
        start_date_str="{{ params.start_date }}",
        end_date_str="{{ params.end_date }}",
    )

    # ✅ 리스트[dict]를 매핑할 때는 expand_kwargs 사용
    evts = fetch_masked_events.expand_kwargs(month_ranges)
    file_id = upload_classification_batch_file.expand(events=evts)
    process_and_save_results = process_and_save_classification_batch.expand(
        file_id=file_id
    )

    # 의존성 설정
    evts >> file_id >> process_and_save_results


# DAG 인스턴스 (기본 기간은 예시, 트리거 시 파라미터로 바꿔도 됨)
classification_dag = classify_disclosure_events_by_gpt_batch_dag()
