"""
Airflow DAG: kind 테이블에 disclosure_type 컬럼을 추가합니다.

이 DAG는 멱등성을 고려하여:
- disclosure_type 컬럼이 이미 존재하면 추가하지 않습니다
- 컬럼이 없으면 NULL 값으로 추가합니다

요구 사항:
- Airflow 2.x (TaskFlow API)
- PostgreSQL 데이터베이스 연결
"""

from __future__ import annotations
import logging
from datetime import datetime
from typing import Optional

import sqlalchemy as sa
from sqlalchemy import text

from airflow.decorators import dag, task
from database import create_database_engine
from config import keywords


def add_disclosure_type_column(engine: sa.engine.Engine) -> str:
    """
    kind 테이블에 disclosure_type 컬럼을 추가합니다.
    PostgreSQL의 IF NOT EXISTS 구문을 사용하여 멱등성을 보장합니다.

    Args:
        engine: SQLAlchemy 엔진

    Returns:
        str: 작업 결과 메시지
    """
    try:
        # 컬럼 추가 SQL (IF NOT EXISTS 사용)
        add_column_sql = """
        ALTER TABLE kind 
        ADD COLUMN IF NOT EXISTS disclosure_type VARCHAR(50) DEFAULT NULL;
        """

        with engine.connect() as conn:
            conn.execute(text(add_column_sql))
            conn.commit()

        logging.info("disclosure_type 컬럼 추가 작업이 완료되었습니다.")
        return "disclosure_type 컬럼 추가 작업이 완료되었습니다."

    except Exception as e:
        logging.error(f"disclosure_type 컬럼 추가 중 오류 발생: {str(e)}")
        raise


def add_dart_unique_id_column(engine: sa.engine.Engine) -> str:
    """
    kind 테이블에 dart_unique_id 컬럼을 추가하고 dart_unique_number 테이블의
    dart_disclosure_id 값으로 채웁니다. 멱등성을 고려합니다.

    Args:
        engine: SQLAlchemy 엔진

    Returns:
        str: 작업 결과 메시지
    """
    try:
        with engine.connect() as conn:
            # 1. 컬럼 추가 (IF NOT EXISTS 사용)
            add_column_sql = """
            ALTER TABLE kind 
            ADD COLUMN IF NOT EXISTS dart_unique_id VARCHAR(50) DEFAULT NULL;
            """

            conn.execute(text(add_column_sql))
            logging.info("dart_unique_id 컬럼 추가/확인 완료")

            # 2. 데이터 업데이트 (dart_unique_number 테이블과 조인)
            update_sql = """
            UPDATE kind 
            SET dart_unique_id = d.dart_disclosure_id
            FROM dart_unique_number d
            WHERE kind.short_code = d.stock_code
              AND kind.dart_unique_id IS NULL
              AND d.dart_disclosure_id IS NOT NULL;
            """

            result = conn.execute(text(update_sql))
            updated_rows = result.rowcount
            conn.commit()

            logging.info(
                f"dart_unique_id 업데이트 완료: {updated_rows}개 행 업데이트됨"
            )

        return f"dart_unique_id 컬럼 추가 및 데이터 업데이트 완료: {updated_rows}개 행 업데이트됨"

    except Exception as e:
        logging.error(
            f"dart_unique_id 컬럼 추가 및 데이터 업데이트 중 오류 발생: {str(e)}"
        )
        raise


def strip_kind_title(engine: sa.engine.Engine) -> str:
    """
    kind 테이블의 title 필드에서 앞뒤 공백을 제거합니다.

    Args:
        engine: SQLAlchemy 엔진

    Returns:
        str: 작업 결과 메시지
    """
    try:
        with engine.connect() as conn:
            # title 필드의 앞뒤 공백 제거
            strip_sql = """
            UPDATE kind 
            SET title = TRIM(title)
            WHERE title IS NOT NULL 
              AND title != TRIM(title);
            """

            result = conn.execute(text(strip_sql))
            updated_rows = result.rowcount
            conn.commit()

            logging.info(f"title 필드 strip 완료: {updated_rows}개 행 업데이트됨")

        return f"title 필드 strip 완료: {updated_rows}개 행 업데이트됨"

    except Exception as e:
        logging.error(f"title 필드 strip 중 오류 발생: {str(e)}")
        raise


def update_disclosure_type(engine: sa.engine.Engine) -> str:
    """
    kind 테이블의 title을 기반으로 disclosure_type을 업데이트합니다.
    config.py의 keywords를 사용하여 패턴 매칭을 수행합니다.

    Args:
        engine: SQLAlchemy 엔진

    Returns:
        str: 작업 결과 메시지
    """
    try:
        total_updated = 0

        with engine.connect() as conn:
            # keywords를 기반으로 disclosure_type 매핑
            for disclosure_type, title_variations in keywords.items():
                if title_variations:  # 빈 리스트가 아닌 경우
                    # 각 title_variation이 title에 포함되어 있는지 확인
                    for variation in title_variations:
                        # PostgreSQL의 SIMILAR TO 또는 ~ (regex) 연산자 사용
                        update_sql = f"""
                        UPDATE kind
                        SET disclosure_type = :disclosure_type
                        WHERE title ~ :pattern
                          AND (disclosure_type IS NULL OR disclosure_type = '');
                        """

                        result = conn.execute(
                            text(update_sql),
                            {"disclosure_type": disclosure_type, "pattern": variation},
                        )
                        updated_rows = result.rowcount
                        total_updated += updated_rows

                        if updated_rows > 0:
                            logging.info(
                                f"'{disclosure_type}' 타입 매칭 (패턴: '{variation}'): {updated_rows}개 행 업데이트"
                            )
                else:  # 빈 리스트인 경우, key 자체를 검색
                    update_sql = f"""
                    UPDATE kind
                    SET disclosure_type = :disclosure_type
                    WHERE title ~ :pattern
                      AND (disclosure_type IS NULL OR disclosure_type = '');
                    """

                    result = conn.execute(
                        text(update_sql),
                        {
                            "disclosure_type": disclosure_type,
                            "pattern": disclosure_type,
                        },
                    )
                    updated_rows = result.rowcount
                    total_updated += updated_rows

                    if updated_rows > 0:
                        logging.info(
                            f"'{disclosure_type}' 타입 매칭 (키워드 자체): {updated_rows}개 행 업데이트"
                        )

            conn.commit()
            logging.info(
                f"disclosure_type 업데이트 완료: 총 {total_updated}개 행 업데이트됨"
            )

        return f"disclosure_type 업데이트 완료: 총 {total_updated}개 행 업데이트됨"

    except Exception as e:
        logging.error(f"disclosure_type 업데이트 중 오류 발생: {str(e)}")
        raise


# ---------------------- Airflow DAG (TaskFlow) ----------------------


@dag(
    dag_id="add_disclosure_type_column_dag",
    description="kind 테이블에 disclosure_type 및 dart_unique_id 컬럼 추가, title 필드 정리, disclosure_type 업데이트",
    start_date=datetime(2025, 8, 1),
    schedule="@once",
    catchup=False,
    tags=[
        "database",
        "schema",
        "kind",
        "disclosure_type",
        "dart_unique_id",
        "title_strip",
        "update_disclosure_type",
    ],
)
def add_disclosure_type_column_dag():

    @task
    def add_disclosure_type_column_task() -> str:
        """
        kind 테이블에 disclosure_type 컬럼을 추가하는 태스크입니다.
        멱등성을 고려하여 이미 존재하면 추가하지 않습니다.
        """
        try:
            logging.info("=== disclosure_type 컬럼 추가 작업 시작 ===")

            # 데이터베이스 연결
            engine = create_database_engine()
            logging.info("데이터베이스 연결이 성공적으로 설정되었습니다.")

            # 컬럼 추가 시도
            result_message = add_disclosure_type_column(engine)
            logging.info(result_message)

            logging.info("=== disclosure_type 컬럼 추가 작업 완료 ===")
            return result_message

        except Exception as e:
            error_message = f"disclosure_type 컬럼 추가 작업 중 오류 발생: {str(e)}"
            logging.error(error_message)
            raise e

    @task
    def add_dart_unique_id_column_task() -> str:
        """
        kind 테이블에 dart_unique_id 컬럼을 추가하고 dart_unique_number 테이블의
        dart_disclosure_id 값으로 채우는 태스크입니다. 멱등성을 고려합니다.
        """
        try:
            logging.info(
                "=== dart_unique_id 컬럼 추가 및 데이터 업데이트 작업 시작 ==="
            )

            # 데이터베이스 연결
            engine = create_database_engine()
            logging.info("데이터베이스 연결이 성공적으로 설정되었습니다.")

            # 컬럼 추가 및 데이터 업데이트 시도
            result_message = add_dart_unique_id_column(engine)
            logging.info(result_message)

            logging.info(
                "=== dart_unique_id 컬럼 추가 및 데이터 업데이트 작업 완료 ==="
            )
            return result_message

        except Exception as e:
            error_message = f"dart_unique_id 컬럼 추가 및 데이터 업데이트 작업 중 오류 발생: {str(e)}"
            logging.error(error_message)
            raise e

    @task
    def strip_kind_title_task() -> str:
        """
        kind 테이블의 title 필드에서 앞뒤 공백을 제거하는 태스크입니다.
        """
        try:
            logging.info("=== title 필드 strip 작업 시작 ===")

            # 데이터베이스 연결
            engine = create_database_engine()
            logging.info("데이터베이스 연결이 성공적으로 설정되었습니다.")

            # title 필드 strip 시도
            result_message = strip_kind_title(engine)
            logging.info(result_message)

            logging.info("=== title 필드 strip 작업 완료 ===")
            return result_message

        except Exception as e:
            error_message = f"title 필드 strip 작업 중 오류 발생: {str(e)}"
            logging.error(error_message)
            raise e

    @task
    def update_disclosure_type_task() -> str:
        """
        kind 테이블의 title을 기반으로 disclosure_type을 업데이트하는 태스크입니다.
        config.py의 keywords를 사용하여 패턴 매칭을 수행합니다.
        """
        try:
            logging.info("=== disclosure_type 업데이트 작업 시작 ===")

            # 데이터베이스 연결
            engine = create_database_engine()
            logging.info("데이터베이스 연결이 성공적으로 설정되었습니다.")

            # disclosure_type 업데이트 시도
            result_message = update_disclosure_type(engine)
            logging.info(result_message)

            logging.info("=== disclosure_type 업데이트 작업 완료 ===")
            return result_message

        except Exception as e:
            error_message = f"disclosure_type 업데이트 작업 중 오류 발생: {str(e)}"
            logging.error(error_message)
            raise e

    # 태스크 실행
    (
        add_disclosure_type_column_task()
        >> add_dart_unique_id_column_task()
        >> strip_kind_title_task()
        >> update_disclosure_type_task()
    )


# DAG 인스턴스 생성
dag_instance = add_disclosure_type_column_dag()
