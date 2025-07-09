from __future__ import annotations

import gzip
from datetime import datetime
from pathlib import Path

from airflow.decorators import dag, task

TARGET_MONTHS = ["2023_11", "2023_12"]


@dag(
    dag_id="unzip_kospi_data",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["kospi", "unzip"],
    description="KOSPI .dat.gz 파일들을 압축해제하는 DAG",
)
def unzip_kospi_dag():
    @task
    def unzip_dat_files():
        """zip 폴더의 .dat.gz 파일들을 unzip 폴더에 압축해제"""

        # 대상 월 설정 (YYYY_MM 형식)
        print(f"[DEBUG] 대상 월 목록: {TARGET_MONTHS}")

        zip_dir = Path("/opt/airflow/data/KOSPI/zip")
        unzip_dir = Path("/opt/airflow/data/KOSPI/unzip")

        print(f"[DEBUG] zip 디렉토리 경로: {zip_dir}")
        print(f"[DEBUG] unzip 디렉토리 경로: {unzip_dir}")

        # unzip 디렉토리가 없으면 생성
        unzip_dir.mkdir(parents=True, exist_ok=True)
        print(f"[DEBUG] unzip 디렉토리 생성/확인 완료")

        if not zip_dir.exists():
            print(f"[ERROR] zip 디렉토리가 존재하지 않습니다: {zip_dir}")
            return 0

        print(f"[DEBUG] zip 디렉토리 존재 확인 완료")

        processed_count = 0
        skipped_count = 0

        # 대상 월에 해당하는 .dat.gz 파일들을 찾아서 처리
        for target_month in TARGET_MONTHS:
            print(f"[DEBUG] 처리 중인 대상 월: {target_month}")
            pattern = f"SKSNXTRDIJH_{target_month}.dat.gz"
            print(f"[DEBUG] 검색 패턴: {pattern}")

            gz_files = list(zip_dir.glob(pattern))
            print(f"[DEBUG] 찾은 파일 개수: {len(gz_files)}")

            if not gz_files:
                print(
                    f"[WARNING] 대상 월 {target_month}에 해당하는 파일을 찾을 수 없습니다: {pattern}"
                )
                continue

            for gz_file in gz_files:
                print(f"[DEBUG] 처리 중인 파일: {gz_file}")

                # 압축해제될 파일명 (.dat)
                dat_filename = gz_file.stem  # .dat.gz에서 .gz 제거
                output_file = unzip_dir / dat_filename

                print(f"[DEBUG] 출력 파일 경로: {output_file}")

                # 이미 압축해제된 파일이 존재하면 건너뛰기
                if output_file.exists():
                    print(f"[INFO] 이미 존재하는 파일 건너뛰기: {dat_filename}")
                    skipped_count += 1
                    continue

                try:
                    print(f"[DEBUG] 압축해제 시작: {gz_file.name}")

                    # 파일 크기 확인
                    gz_size = gz_file.stat().st_size
                    print(f"[DEBUG] 압축 파일 크기: {gz_size:,} bytes")

                    # gzip 파일 압축해제
                    with gzip.open(gz_file, "rb") as f_in:
                        with open(output_file, "wb") as f_out:
                            data = f_in.read()
                            f_out.write(data)
                            print(
                                f"[DEBUG] 압축해제된 데이터 크기: {len(data):,} bytes"
                            )

                    print(f"[SUCCESS] 압축해제 완료: {gz_file.name} -> {dat_filename}")
                    processed_count += 1

                except Exception as e:
                    print(f"[ERROR] 압축해제 실패 {gz_file.name}: {e}")
                    print(f"[ERROR] 에러 타입: {type(e).__name__}")

        print(
            f"[SUMMARY] 처리 완료 - 압축해제: {processed_count}개, 건너뛰기: {skipped_count}개"
        )
        return processed_count

    # Task 실행
    unzip_dat_files()


# DAG 인스턴스 생성
dag_instance = unzip_kospi_dag()
