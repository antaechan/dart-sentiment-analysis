# dags/tick_to_parquet.py
from __future__ import annotations

import re
from datetime import datetime
from pathlib import Path

import polars as pl
from airflow.decorators import dag, task
from polars import PartitionByKey, col

from config import (COLS, KEEP, KOSDAQ_PARTITION_DIR, KOSDAQ_RAW_DIR,
                    KOSPI_PARTITION_DIR, KOSPI_RAW_DIR)


# ─────────────────────── DAG 정의 ───────────────────────
@dag(
    start_date=datetime(2025, 7, 4),
    schedule="@once",
    catchup=False,
    max_active_tasks=1,
    tags=["ticks", "parquet"],
)
def tick_to_parquet_dag():

    # ① 대상 .dat 파일 목록 수집
    @task
    def get_files(year_month_list: list[str]) -> list[str]:
        patt = re.compile("|".join(year_month_list))  # "2023_12|2024_01" ...
        kospi = [
            str(p)
            for p in KOSPI_RAW_DIR.iterdir()
            if p.suffix == ".dat" and patt.search(p.name)
        ]
        kosdaq = [
            str(p)
            for p in KOSDAQ_RAW_DIR.iterdir()
            if p.suffix == ".dat" and patt.search(p.name)
        ]
        return kospi + kosdaq

    # ② CSV → 날짜별 Parquet 파티션
    @task
    def convert(file_path: str) -> str:
        p = Path(file_path)
        out_dir = KOSPI_PARTITION_DIR if "KOSPI" in p.parts else KOSDAQ_PARTITION_DIR
        out_dir.mkdir(parents=True, exist_ok=True)

        # LazyFrame + streaming
        lf = (
            pl.scan_csv(
                p,
                separator="|",
                has_header=False,
                new_columns=COLS,
                low_memory=True,
                infer_schema_length=100,
            )
            .select(KEEP)
            .sink_parquet(  # → 메모리 안 올리고 바로 디스크로 스트리밍
                PartitionByKey(
                    base_path=out_dir,  # 예: "./out"
                    by=[col("체결일자")],  # 파티션 키
                    # include_key=True  # 기본값. False 로 두면 파일 안에 '체결일자' 컬럼을 빼고 저장
                    # file_path 콜백을 주면 디렉터리/파일명 커스터마이즈 가능
                ),
                compression="zstd",
                row_group_size=50_000,
                mkdir=True,  # out_dir 가 없으면 생성
            )
        )
        # 폴더 경로를 반환 (후속 DAG에 전달할 수도 있음)
        return str(out_dir)

    # ── Dynamic task-mapping ──
    convert.expand(
        file_path=get_files(
            year_month_list=[
                "2023_01",
                "2023_02",
                "2023_03",
                "2023_04",
                "2023_05",
                "2023_06",
                "2023_07",
                "2023_08",
                "2023_09",
            ]
        )
    )


dag = tick_to_parquet_dag()
