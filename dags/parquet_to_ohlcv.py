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
def parquet_to_ohlcv_dag():

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

    # partition에서 얻은 target_dates만 공시 조회에 사용
    @task
    def build_disclosure_universe(target_dates: list[str]) -> dict[str, list[str]]:
        print(f"[DEBUG] Universe 구축 시작 — target_dates={len(target_dates)}개")
        if not target_dates:
            print("[DEBUG] target_dates 비어있음 → 빈 universe 반환")
            return {}

        POSTGRES_USER = os.getenv("POSTGRES_USER")
        POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
        POSTGRES_DB = os.getenv("POSTGRES_DB")
        POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")

        db_url = (
            f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}"
            f"@postgres_events:{POSTGRES_PORT}/{POSTGRES_DB}"
        )
        engine = sa.create_engine(db_url, pool_pre_ping=True, future=True)

        # target_dates는 "YYYYMMDD" 문자열 리스트이므로 to_char로 비교
        sql = sa.text(
            """
            SELECT
              (disclosed_at AT TIME ZONE 'Asia/Seoul')::date AS disclosed_at,
              stock_code
            FROM disclosure_events
            WHERE stock_code IS NOT NULL
              AND to_char((disclosed_at AT TIME ZONE 'Asia/Seoul')::date, 'YYYYMMDD') IN :target_dates
              AND (
                  (EXTRACT(HOUR FROM disclosed_at AT TIME ZONE 'Asia/Seoul') BETWEEN 9 AND 14)
                  OR
                  (EXTRACT(HOUR FROM disclosed_at AT TIME ZONE 'Asia/Seoul') = 15
                   AND EXTRACT(MINUTE FROM disclosed_at AT TIME ZONE 'Asia/Seoul') <= 30)
              )
            """
        ).bindparams(sa.bindparam("target_dates", expanding=True))

        params = {
            "target_dates": target_dates,
        }

        with engine.connect() as conn:
            df_events = pd.read_sql(sql, conn, params=params)
            print(f"[DEBUG] 조회된 공시 건수: {len(df_events)}")

        if df_events.empty:
            print("[DEBUG] 공시 결과 비어있음 → 빈 universe 반환")
            return {}

        events_pl = pl.from_pandas(df_events)
        events = events_pl.group_by("disclosed_at").agg(
            pl.col("stock_code").unique().alias("codes")
        )

        universe = {
            d.strftime("%Y%m%d"): codes
            for d, codes in zip(
                events["disclosed_at"].to_list(),
                events["codes"].to_list(),
            )
        }
        print(f"[DEBUG] Universe 구축 완료. 일자 수: {len(universe)}")
        for date_str, codes in universe.items():
            print(f"[DEBUG] {date_str}: 공시 종목 수 = {len(codes)}개")
        return universe

    @task
    def convert(market: str, universe: dict[str, list[str]]) -> list[str]:
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

        for date_str, codes in universe.items():
            if not codes:
                print(f"[DEBUG] {date_str}: 공시 종목 없음, 건너뜀")
                continue

            out = OUT_DIR / f"{date_str}_1m.parquet"
            if out.exists():
                print(f"[DEBUG] {date_str}: 이미 파일 존재, 건너뜀")
                continue

            print(f"[DEBUG] {date_str}: OHLCV 변환 시작 (codes: {len(codes)}개)")
            print(f"[DEBUG] 대상 종목코드: {codes}")

            # 파티션 데이터 확인
            partition_path = PARTITION_DIR / f"체결일자={date_str}"
            print(f"[DEBUG] 파티션 경로: {partition_path}")

            # 실제 파티션에서 해당 종목들의 데이터 존재 여부 확인
            try:
                available_data = (
                    pl.scan_parquet(
                        partition_path,
                        hive_partitioning=True,
                        low_memory=True,
                    )
                    .filter(
                        (pl.col("체결일자") == int(date_str))
                        & (pl.col("종목코드").is_in(codes))
                    )
                    .select(["종목코드"])
                    .unique()
                    .collect()
                )

                actual_codes = available_data["종목코드"].to_list()
                print(f"[DEBUG] 파티션에서 실제 발견된 종목: {len(actual_codes)}개")
                print(f"[DEBUG] 실제 종목코드: {actual_codes}")

                missing_codes = set(codes) - set(actual_codes)
                if missing_codes:
                    print(f"[WARNING] 파티션에 없는 종목들: {missing_codes}")

            except Exception as e:
                print(f"[ERROR] 파티션 데이터 확인 중 오류: {e}")

            # OHLCV 변환 실행
            try:
                result_df = (
                    pl.scan_parquet(
                        partition_path,
                        hive_partitioning=True,
                        low_memory=True,
                    )
                    .select(KEEP)
                    .filter(
                        (pl.col("체결일자") == int(date_str))
                        & (pl.col("종목코드").is_in(codes))
                    )
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

                print(f"[DEBUG] 필터링 후 데이터 건수: {len(result_df)}")
                print(
                    f"[DEBUG] 필터링 후 종목 수: {result_df.select('종목코드').n_unique()}"
                )

                if len(result_df) == 0:
                    print(
                        f"[WARNING] {date_str}: 필터링 후 데이터가 없어 OHLCV 파일을 생성하지 않습니다."
                    )
                    continue

                # OHLCV 집계
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

        print(f"[DEBUG] 변환 완료. 생성된 파일 수: {len(out_paths)}")
        return out_paths

    # ── DAG wiring (expand) ───────────────────────────────────────────────
    print("[DEBUG] DAG 실행 시작")
    markets = ["KOSPI", "KOSDAQ"]
    target_year_month = ["2023_11", "2023_12"]

    # market별로 partition에서 target_dates 수집
    # 각 market에 대해 동일한 year_month_list 전달
    target_dates_list = collect_target_dates.partial(
        year_month_list=target_year_month
    ).expand(market=markets)

    # 수집한 target_dates만으로 공시 Universe 생성
    universes = build_disclosure_universe.expand(
        target_dates=target_dates_list,
    )

    # roots(KOSPI/KOSDAQ)와 universes를 1:1 zip 매핑
    convert.expand(market=markets, universe=universes)

    print("[DEBUG] DAG 설정 완료")


dag = parquet_to_ohlcv_dag()
