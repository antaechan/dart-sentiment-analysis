"""
Airflow DAG: kind 테이블에서 disclosure_type이 '단일판매ㆍ공급계약체결'인 레코드의
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
from get_disclosure_supply import get_disclosure_supply


def fetch_and_update_supply_contracts(engine: sa.engine.Engine) -> str:
    """
    kind 테이블에서 disclosure_type이 '단일판매ㆍ공급계약체결'이고
    raw가 NULL이거나 비어있는 레코드를 조회한 뒤,
    detail_url로 get_disclosure_supply를 호출하여 raw 필드를 업데이트합니다.

    Args:
        engine: SQLAlchemy 엔진

    Returns:
        str: 작업 결과 메시지
    """
    try:
        total_updated = 0
        total_failed = 0

        with engine.connect() as conn:
            # disclosure_type이 '단일판매ㆍ공급계약체결'이고 raw가 비어있고 is_modify = 0인 레코드 조회
            select_sql = """
            SELECT id, disclosure_id, detail_url
            FROM kind
            WHERE disclosure_type = '단일판매ㆍ공급계약체결'
              AND (raw IS NULL OR raw = '')
              AND detail_url IS NOT NULL
              AND detail_url != ''
              AND is_modify = 0
            ORDER BY disclosed_at DESC
            LIMIT 1500;
            """

            result = conn.execute(text(select_sql))
            records = result.fetchall()

            logging.info(f"처리할 공급계약 공시: {len(records)}건")

            if not records:
                return "처리할 공급계약 공시가 없습니다."

            # 각 레코드에 대해 get_disclosure_supply 호출
            for record in records:
                record_id = record[0]
                disclosure_id = record[1]
                detail_url = record[2]

                try:
                    logging.info(f"처리 중: {disclosure_id} - {detail_url}")

                    # get_disclosure_supply 함수 호출
                    raw_content = get_disclosure_supply(detail_url, timeout=30)

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
                f"공급계약 raw 업데이트 완료: 성공 {total_updated}건, 실패 {total_failed}건"
            )

        return f"공급계약 raw 업데이트 완료: 성공 {total_updated}건, 실패 {total_failed}건 (전체 {len(records)}건)"

    except Exception as e:
        logging.error(f"공급계약 raw 업데이트 중 오류 발생: {str(e)}")
        raise


# ---------------------- Airflow DAG (TaskFlow) ----------------------


@dag(
    dag_id="update_supply_contract_raw_dag",
    description="kind 테이블에서 공급계약 공시의 detail_url로 크롤링하여 raw 필드 업데이트",
    start_date=datetime(2025, 8, 1),
    schedule="@once",
    catchup=False,
    tags=[
        "database",
        "kind",
        "disclosure_supply",
        "crawling",
        "supply_contract",
    ],
)
def update_supply_contract_raw_dag():

    @task
    def update_supply_contract_raw_task() -> str:
        """
        kind 테이블에서 disclosure_type이 '단일판매ㆍ공급계약체결'인 레코드의
        detail_url로 크롤링하여 raw 필드를 업데이트하는 태스크입니다.
        """
        try:
            logging.info("=== 공급계약 raw 업데이트 작업 시작 ===")

            # 데이터베이스 연결
            engine = create_database_engine()
            logging.info("데이터베이스 연결이 성공적으로 설정되었습니다.")

            # 공급계약 크롤링 및 업데이트 실행
            result_message = fetch_and_update_supply_contracts(engine)
            logging.info(result_message)

            logging.info("=== 공급계약 raw 업데이트 작업 완료 ===")
            return result_message

        except Exception as e:
            error_message = f"공급계약 raw 업데이트 작업 중 오류 발생: {str(e)}"
            logging.error(error_message)
            raise e

    # 태스크 실행
    update_supply_contract_raw_task()


# DAG 인스턴스 생성
dag_instance = update_supply_contract_raw_dag()
