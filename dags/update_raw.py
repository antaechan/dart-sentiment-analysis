"""
Airflow DAG: kind 테이블에서 특정 disclosure_type인 레코드의
공시 내용을 DART API를 이용해 크롤링하여 raw 필드에 저장합니다.

요구 사항:
- Airflow 2.x (TaskFlow API)
- PostgreSQL 데이터베이스 연결
"""

from __future__ import annotations
import logging
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import sqlalchemy as sa
from sqlalchemy import text

from airflow.decorators import dag, task
from database import create_database_engine
from dart import get_disclosure, dart_API_map, DartAPIError
from crawl import crawling_function_map
from disclosure_common import extract_text

# 환경 변수
KST = ZoneInfo("Asia/Seoul")
UTC = ZoneInfo("UTC")

DISCLOSURE_EVENTS_TABLE = "kind"


@dag(
    dag_id="update_kind_raw_fields_dag",
    description="kind 테이블의 raw 필드를 DART API로 크롤링하여 업데이트",
    start_date=datetime(2025, 7, 4),
    schedule="@once",
    catchup=False,
    tags=[
        "database",
        "dart",
        "disclosure",
        "raw_update",
    ],
    max_active_tasks=1,
    params={
        "start_date": "2022-07-01",
        "end_date": "2022-12-31",
    },
)
def update_kind_raw_fields_dag():

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

            # DB 비교용 UTC 경계로 변환해 ISO 문자열로 반환
            utc_start = kst_start.astimezone(UTC).isoformat()
            utc_end = kst_end.astimezone(UTC).isoformat()

            out.append({"start_date": utc_start, "end_date": utc_end})
            cur = next_first

        print(
            f"Generated {len(out)} month ranges (KST→UTC) from {start_date_str} to {end_date_str}"
        )
        print(f"First UTC range: {out[0] if out else 'None'}")
        return out

    @task(task_id="fetch_events_to_update")
    def fetch_events_to_update(**kwargs) -> list[dict]:
        """월별로 raw가 비어있는 이벤트를 조회"""
        start_date = kwargs.get("start_date")
        end_date = kwargs.get("end_date")

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

        # 미리 가능한 disclosure_type 리스트 생성 (둘 중 하나라도 None이 아닌 경우)
        valid_disclosure_types = [
            dt
            for dt in dart_API_map.keys()
            if dart_API_map.get(dt) is not None
            or crawling_function_map.get(dt) is not None
        ]

        if not valid_disclosure_types:
            print("No valid disclosure types found")
            return []

        # SQL IN 절에 사용할 플레이스홀더 생성
        placeholders = ",".join([f"'{dt}'" for dt in valid_disclosure_types])

        sql = f"""
            SELECT k.id, k.disclosure_type, k.disclosed_at, k.detail_url, d.dart_disclosure_id, k.short_code
            FROM {DISCLOSURE_EVENTS_TABLE} k
            JOIN dart_unique_number d ON k.short_code = d.stock_code
            WHERE (k.raw IS NULL OR k.raw = '')
              AND k.disclosed_at BETWEEN :utc_start AND :utc_end
              AND k.is_modify = 0
              AND (d.dart_disclosure_id IS NOT NULL AND d.dart_disclosure_id != '')
              AND k.disclosure_type IN ({placeholders})
            ORDER BY k.disclosed_at DESC;
        """

        engine = create_database_engine()
        with engine.connect() as conn:
            result = conn.execute(
                sa.text(sql),
                {
                    "utc_start": s,
                    "utc_end": e,
                },
            )
            events = [
                {
                    "id": row.id,
                    "disclosure_type": row.disclosure_type,
                    "disclosed_at": row.disclosed_at,
                    "detail_url": row.detail_url,
                    "dart_disclosure_id": row.dart_disclosure_id,
                }
                for row in result
            ]

        print(
            f"Fetched {len(events)} events for UTC period {s.isoformat()} → {e.isoformat()}"
        )
        return events

    @task(task_id="update_events_raw")
    def update_events_raw(events: list[dict]) -> str:
        """이벤트들을 처리하여 raw 필드 업데이트"""

        if not events:
            print("No events to update")
            return "No events to update"

        total_updated = 0
        total_failed = 0
        skipped_no_api = 0

        for event in events:
            record_id = event["id"]
            disclosure_type = event["disclosure_type"]
            disclosed_at = event["disclosed_at"]
            detail_url = event["detail_url"]
            dart_disclosure_id = event["dart_disclosure_id"]
            date = disclosed_at.strftime("%Y%m%d")

            try:
                raw_content = None

                # DART API가 있는 경우 시도
                if dart_API_map.get(disclosure_type) is not None and dart_disclosure_id:
                    try:
                        logging.info(
                            f"처리 중 (DART API): {disclosure_type} - {dart_disclosure_id} ({date})"
                        )
                        raw_content = get_disclosure(
                            disclosure_type, dart_disclosure_id, date
                        )
                    except DartAPIError as dart_error:
                        # DART API 실패 시 extract_text로 fallback 시도
                        if (
                            detail_url
                            and crawling_function_map.get(disclosure_type) is not None
                        ):
                            logging.warning(
                                f"DART API 실패, 크롤링으로 fallback: {disclosure_type} - {str(dart_error)}"
                            )
                            logging.info(
                                f"처리 중 (크롤링): {disclosure_type} - {detail_url}"
                            )
                            raw_content = extract_text(detail_url, disclosure_type)
                        else:
                            # Fallback도 없으면 re-raise
                            raise dart_error
                # DART API가 없는 경우 extract_text 사용
                elif (
                    detail_url
                    and crawling_function_map.get(disclosure_type) is not None
                ):
                    logging.info(f"처리 중 (크롤링): {disclosure_type} - {detail_url}")
                    raw_content = extract_text(detail_url, disclosure_type)
                else:
                    skipped_no_api += 1
                    logging.warning(
                        f"'{disclosure_type}' 처리 방법이 없습니다. 건너뜁니다."
                    )
                    continue

                if raw_content and len(raw_content.strip()) > 0:
                    # raw 필드 업데이트
                    engine = create_database_engine()
                    with engine.connect() as conn:
                        update_sql = f"""
                        UPDATE {DISCLOSURE_EVENTS_TABLE}
                        SET raw = :raw_content,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE id = :record_id;
                        """

                        conn.execute(
                            text(update_sql),
                            {"raw_content": raw_content, "record_id": record_id},
                        )
                        conn.commit()

                    total_updated += 1
                    logging.info(
                        f"✓ 업데이트 완료: ID {record_id} (길이: {len(raw_content)}자)"
                    )
                else:
                    total_failed += 1
                    logging.warning(f"✗ 크롤링 실패 (빈 내용): ID {record_id}")

            except Exception as e:
                total_failed += 1
                logging.error(f"✗ 크롤링 실패: ID {record_id} - {str(e)}")
                continue

        result_message = f"raw 업데이트 완료: 성공 {total_updated}건, 실패 {total_failed}건, API 없음 건너뜀 {skipped_no_api}건 (전체 {len(events)}건)"
        print(result_message)
        return result_message

    # 템플릿으로 문자열 파라미터 주입
    month_ranges = build_month_ranges(
        start_date_str="{{ params.start_date }}",
        end_date_str="{{ params.end_date }}",
    )

    # ✅ 리스트[dict]를 매핑할 때는 expand_kwargs 사용
    evts = fetch_events_to_update.expand_kwargs(month_ranges)
    upd = update_events_raw.expand(events=evts)

    # 의존성 설정
    month_ranges >> evts >> upd


# DAG 인스턴스 생성
dag_instance = update_kind_raw_fields_dag()
