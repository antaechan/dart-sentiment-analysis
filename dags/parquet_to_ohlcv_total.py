# dags/tick_to_ohlcv.py
from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path

import pandas as pd
import polars as pl
import sqlalchemy as sa
from airflow.decorators import dag, task
from dotenv import load_dotenv

from config import KOSDAQ_OUT_DIR  # OHLCV 출력 위치
from config import KOSDAQ_PARTITION_DIR  # Tick Parquet 파티션 루트
from config import KEEP, KOSPI_OUT_DIR, KOSPI_PARTITION_DIR, TZ

load_dotenv()


@dag(
    start_date=datetime(2025, 7, 4),
    schedule="@once",
    catchup=False,
    tags=["ticks", "ohlcv"],
    max_active_tasks=1,
)
def parquet_to_ohlcv_total_dag():

    # 파티션 디렉터리에서 실제 존재하는 체결일자=YYYYMMDD를 수집
    @task
    def collect_target_dates(year_month_list: list[str], market: str) -> list[str]:
        """
        year_month_list 예: ["2023_12", "2024_01"]
        지정한 market의 partition 루트에서 '체결일자=YYYYMMDD' 디렉터리를 스캔해서
        해당 월들에 속하는 날짜만 뽑아 ["YYYYMMDD", ...]로 반환
        """
        root: Path
        if market == "KOSPI":
            root = KOSPI_PARTITION_DIR
        elif market == "KOSDAQ":
            root = KOSDAQ_PARTITION_DIR
        else:
            raise ValueError(f"Unknown market: {market}")

        # 허용 월 셋 (YYYY_MM → (YYYY, MM))
        allow_months = set()
        for ym in year_month_list:
            if "_" not in ym:
                print(f"[WARNING] 잘못된 형식의 year_month: {ym}, 건너뜀")
                continue
            try:
                y, m = ym.split("_")
                allow_months.add((int(y), int(m)))
            except (ValueError, IndexError) as e:
                print(f"[WARNING] year_month 파싱 실패: {ym}, 오류: {e}, 건너뜀")
                continue

        # 체결일자=* 디렉터리만 훑기 (파일까지 내려가지 않음)
        dates: set[str] = set()
        try:
            # os.listdir을 사용하여 디렉터리 목록 조회
            for item in os.listdir(root):
                print(item)
                try:
                    ymd = item.split("=", 1)[1]  # "YYYYMMDD"
                    if len(ymd) != 8 or not ymd.isdigit():
                        continue
                    y, m = int(ymd[0:4]), int(ymd[4:6])
                    if (y, m) in allow_months:
                        dates.add(ymd)
                except Exception:
                    continue
        except OSError as e:
            print(f"[WARNING] 디렉터리 읽기 실패: {e}")
            return []

        target_dates = sorted(dates)
        print(
            f"[DEBUG] [{market}] partition에서 수집된 target_dates({len(target_dates)}): {target_dates[:10]}{' ...' if len(target_dates)>10 else ''}"
        )
        return target_dates

    @task
    def convert(market: str, target_dates: list[str]) -> list[str]:
        print(f"[DEBUG] 변환 시작 (market): {market}")

        if market == "KOSPI":
            OUT_DIR = KOSPI_OUT_DIR
            PARTITION_DIR = KOSPI_PARTITION_DIR
        elif market == "KOSDAQ":
            OUT_DIR = KOSDAQ_OUT_DIR
            PARTITION_DIR = KOSDAQ_PARTITION_DIR
        else:
            raise ValueError(f"Unknown market: {market}")

        out_paths: list[str] = []

        if OUT_DIR is None:
            raise ValueError(f"Unknown market root: {market}")
        print(f"[DEBUG] 출력 디렉터리: {OUT_DIR}")

        for date_str in target_dates:
            out = OUT_DIR / f"{date_str}_1m.parquet"
            if out.exists():
                print(f"[DEBUG] {date_str}: 이미 파일 존재, 건너뜀")
                continue

            # 파티션 데이터 확인
            partition_path = PARTITION_DIR / f"체결일자={date_str}"
            print(f"[DEBUG] 파티션 경로: {partition_path}")

            # OHLCV 변환 실행
            try:
                result_df = (
                    pl.scan_parquet(
                        partition_path,
                        hive_partitioning=True,
                        low_memory=True,
                    )
                    .select(KEEP)
                    .with_columns(
                        pl.datetime(
                            (pl.col("체결일자") // 10_000),
                            ((pl.col("체결일자") // 100) % 100).cast(pl.UInt8),
                            (pl.col("체결일자") % 100).cast(pl.UInt8),
                            (pl.col("체결시각") // 10_000_000).cast(pl.UInt8),
                            ((pl.col("체결시각") // 100_000) % 100).cast(pl.UInt8),
                            ((pl.col("체결시각") // 1_000) % 100).cast(pl.UInt8),
                            (pl.col("체결시각") % 1_000) * 1_000,
                            time_zone=TZ,
                        ).alias("ts")
                    )
                    .drop(["체결일자", "체결시각"])
                    .sort("ts")
                    .collect()  # 먼저 collect하여 중간 결과 확인
                )

                ohlcv_df = (
                    result_df.lazy()
                    .group_by_dynamic(
                        index_column="ts",
                        every="1m",
                        by="종목코드",
                        closed="left",
                    )
                    .agg(
                        open=pl.col("체결가격").first(),
                        high=pl.col("체결가격").max(),
                        low=pl.col("체결가격").min(),
                        close=pl.col("체결가격").last(),
                        volume=pl.col("체결수량").sum(),
                    )
                    .collect()
                )

                print(f"[DEBUG] OHLCV 집계 후 데이터 건수: {len(ohlcv_df)}")
                print(
                    f"[DEBUG] OHLCV 집계 후 종목 수: {ohlcv_df.select('종목코드').n_unique()}"
                )

                # 파일 저장
                ohlcv_df.write_parquet(out, compression="zstd")
                print(f"[DEBUG] {date_str}: OHLCV 변환 완료 → {out}")
                out_paths.append(str(out))

            except Exception as e:
                print(f"[ERROR] {date_str}: OHLCV 변환 중 오류 발생: {e}")
                continue

        return out_paths

    # ── DAG wiring (expand) ───────────────────────────────────────────────
    print("[DEBUG] DAG 실행 시작")
    markets = ["KOSPI", "KOSDAQ"]
    target_year_month = ["2023_9", "2023_10"]

    # market별로 partition에서 target_dates 수집
    # 각 market에 대해 동일한 year_month_list 전달
    target_dates_list = collect_target_dates.partial(
        year_month_list=target_year_month
    ).expand(market=markets)

    convert.expand(market=markets, target_dates=target_dates_list)

    print("[DEBUG] DAG 설정 완료")


dag = parquet_to_ohlcv_total_dag()
