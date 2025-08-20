from __future__ import annotations

import os
from datetime import datetime
from airflow.decorators import dag, task
from dotenv import load_dotenv
import logging

# database.py에서 필요한 함수들 import
from database import (
    create_database_engine,
    create_dart_unique_number_table_if_not_exists,
    save_into_dart_unique_number_table,
)

load_dotenv()

# DART API 키 환경변수에서 가져오기
DART_API_KEY = os.getenv("DART_API_KEY")

if not DART_API_KEY:
    raise ValueError("DART_API_KEY 환경변수가 설정되지 않았습니다.")


@dag(
    start_date=datetime(2025, 8, 20),
    schedule="@once",
    catchup=False,
    max_active_tasks=1,
    tags=["dart", "corp_code", "table_creation"],
    description="DART 고유 번호 테이블을 생성하고 기업 정보를 삽입하는 DAG",
)
def create_dart_unique_table_dag():

    @task
    def create_table():
        """dart_unique_number 테이블을 생성합니다."""
        try:
            engine = create_database_engine()
            table_created = create_dart_unique_number_table_if_not_exists(engine)

            if table_created:
                logging.info("dart_unique_number 테이블이 새로 생성되었습니다.")
            else:
                logging.info("dart_unique_number 테이블이 이미 존재합니다.")

            return table_created
        except Exception as e:
            logging.error(f"테이블 생성 중 오류 발생: {str(e)}")
            raise

    @task
    def populate_table():
        """DART API에서 기업 정보를 가져와 테이블에 삽입합니다."""
        try:
            engine = create_database_engine()

            # DART API에서 기업 정보 가져와서 테이블에 삽입
            save_into_dart_unique_number_table(
                api_key=DART_API_KEY,
                engine=engine,
                timeout=60,  # DART API 응답 대기 시간을 60초로 설정
            )

            logging.info(
                "dart_unique_number 테이블에 기업 정보가 성공적으로 삽입되었습니다."
            )
            return True
        except Exception as e:
            logging.error(f"테이블 데이터 삽입 중 오류 발생: {str(e)}")
            raise

    # 태스크 실행 순서 정의
    table_created = create_table()
    data_populated = populate_table()

    # 의존성 설정
    data_populated.set_upstream(table_created)


# DAG 인스턴스 생성
dag_instance = create_dart_unique_table_dag()
