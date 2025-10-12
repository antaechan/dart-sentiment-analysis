"""
Airflow DAG: kind 테이블에서 특정 disclosure_type인 레코드의
detail_url을 이용해 공시 내용을 크롤링하여 raw 필드에 저장합니다.

요구 사항:
- Airflow 2.x (TaskFlow API)
- PostgreSQL 데이터베이스 연결
"""

from __future__ import annotations
import logging
from datetime import datetime
from typing import List, Dict

import sqlalchemy as sa
from sqlalchemy import text

from airflow.decorators import dag, task
from database import create_database_engine
from disclosure_common import extract_text


def fetch_and_update_disclosure_raw(
    engine: sa.engine.Engine, disclosure_type: str
) -> str:
    """
    kind 테이블에서 특정 disclosure_type이고
    raw가 NULL이거나 비어있는 레코드를 조회한 뒤,
    detail_url로 extract_text를 호출하여 raw 필드를 업데이트합니다.

    Args:
        engine: SQLAlchemy 엔진
        disclosure_type: 처리할 공시 유형 (예: '단일판매ㆍ공급계약체결')

    Returns:
        str: 작업 결과 메시지
    """
    try:
        total_updated = 0
        total_failed = 0

        with engine.connect() as conn:
            # disclosure_type에 해당하고 raw가 비어있고 is_modify = 0인 레코드 조회
            select_sql = """
            SELECT id, disclosure_id, detail_url
            FROM kind
            WHERE disclosure_type = :disclosure_type
              AND (raw IS NULL OR raw = '')
              AND detail_url IS NOT NULL
              AND detail_url != ''
              AND is_modify = 0
            ORDER BY disclosed_at DESC
            LIMIT 2000;
            """

            result = conn.execute(
                text(select_sql), {"disclosure_type": disclosure_type}
            )
            records = result.fetchall()

            logging.info(f"처리할 '{disclosure_type}' 공시: {len(records)}건")

            if not records:
                return f"처리할 '{disclosure_type}' 공시가 없습니다."

            for record in records:
                record_id = record[0]
                disclosure_id = record[1]
                detail_url = record[2]

                try:
                    logging.info(f"처리 중: {disclosure_id} - {detail_url}")
                    raw_content = extract_text(detail_url, disclosure_type, timeout=30)

                    if raw_content and len(raw_content.strip()) > 0:
                        # raw 필드 업데이트
                        update_sql = """
                        UPDATE kind
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
                            f"✓ 업데이트 완료: {disclosure_id} (길이: {len(raw_content)}자)"
                        )
                    else:
                        total_failed += 1
                        logging.warning(f"✗ 크롤링 실패 (빈 내용): {disclosure_id}")

                except Exception as e:
                    total_failed += 1
                    logging.error(f"✗ 크롤링 실패: {disclosure_id} - {str(e)}")
                    # 개별 레코드 실패해도 계속 진행
                    continue

            logging.info(
                f"'{disclosure_type}' raw 업데이트 완료: 성공 {total_updated}건, 실패 {total_failed}건"
            )

        return f"'{disclosure_type}' raw 업데이트 완료: 성공 {total_updated}건, 실패 {total_failed}건 (전체 {len(records)}건)"

    except Exception as e:
        logging.error(f"'{disclosure_type}' raw 업데이트 중 오류 발생: {str(e)}")
        raise


# ---------------------- Airflow DAG (TaskFlow) ----------------------
@dag(
    dag_id="update_kind_disclosure_raw_dag",
    description="kind 테이블에서 특정 disclosure_type 공시의 detail_url로 크롤링하여 raw 필드 업데이트",
    start_date=datetime(2025, 8, 1),
    schedule="@once",
    catchup=False,
    tags=[
        "database",
        "kind",
        "disclosure",
        "crawling",
        "raw_update",
    ],
    params={
        "disclosure_type": "횡령ㆍ배임혐의발생",
    },
)
def update_kind_disclosure_raw_dag():

    @task
    def update_kind_disclosure_raw_task(**context) -> str:
        """
        kind 테이블에서 특정 disclosure_type인 레코드의
        detail_url로 크롤링하여 raw 필드를 업데이트하는 태스크입니다.
        """
        try:
            # DAG 파라미터에서 disclosure_type 가져오기
            disclosure_type = context["params"].get("disclosure_type", "")
            logging.info(f"=== '{disclosure_type}' raw 업데이트 작업 시작 ===")

            # 데이터베이스 연결
            engine = create_database_engine()
            logging.info("데이터베이스 연결이 성공적으로 설정되었습니다.")

            # 공시 크롤링 및 업데이트 실행
            result_message = fetch_and_update_disclosure_raw(engine, disclosure_type)
            logging.info(result_message)

            logging.info(f"=== '{disclosure_type}' raw 업데이트 작업 완료 ===")
            return result_message

        except Exception as e:
            error_message = f"raw 업데이트 작업 중 오류 발생: {str(e)}"
            logging.error(error_message)
            raise e

    # 태스크 실행
    update_kind_disclosure_raw_task()


# DAG 인스턴스 생성
dag_instance = update_kind_disclosure_raw_dag()
