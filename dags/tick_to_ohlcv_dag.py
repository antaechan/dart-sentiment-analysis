# dags/tick_to_ohlcv.py
from __future__ import annotations

import os
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd
import polars as pl
import sqlalchemy as sa
from airflow.decorators import dag, task
from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv

load_dotenv()

# ────────────────────────────────────────────────────────────────────────────
# 상수
# ────────────────────────────────────────────────────────────────────────────
KOSPI_RAW_DIR = Path("/opt/airflow/data/KOSPI/unzip")
KOSPI_OUT_DIR = Path("/opt/airflow/data/KOSPI/ohlcv")
KOSDAQ_RAW_DIR = Path("/opt/airflow/data/KOSDAQ/unzip")
KOSDAQ_OUT_DIR = Path("/opt/airflow/data/KOSDAQ/ohlcv")
TZ = "Asia/Seoul"

COLS = [
    "일자",
    "대량매매구분코드",
    "정규시간외구분코드",
    "종목코드",
    "종목인덱스",
    "체결번호",
    "체결가격",
    "체결수량",
    "체결유형코드",
    "체결일자",
    "체결시각",
    "근월물체결가격",
    "원월물체결가격",
    "매수회원번호",
    "매수호가유형코드",
    "매수자사주신고서ID",
    "매수자사주매매방법코드",
    "매수매도유형코드",
    "매수위탁자기구분코드",
    "매수위탁사번호",
    "매수PT구분코드",
    "매수투자자구분코드",
    "매수외국인투자자구분코드",
    "매수호가접수번호",
    "매도회원번호",
    "매도호가유형코드",
    "매도자사주신고서ID",
    "매도자사주매매방법코드",
    "매도매도유형코드",
    "매도위탁자기구분코드",
    "매도위탁사번호",
    "매도PT구분코드",
    "매도투자자구분코드",
    "매도외국인투자자구분코드",
    "매도호가접수번호",
    "시가",
    "고가",
    "저가",
    "직전가격",
    "누적체결수량",
    "누적거래대금",
    "최종매도매수구분코드",
    "LP보유수량",
    "데이터구분",
    "메세지일련번호",
    "매수프로그램호가신고구분코드",
    "매도프로그램호가신고구분코드",
    "보드ID",
    "세션ID",
    "실시간상한가",
    "실시간하한가",
]  # (생략: 기존과 동일)
KEEP = ["종목코드", "체결가격", "체결수량", "체결일자", "체결시각"]


# ────────────────────────────────────────────────────────────────────────────
@dag(
    start_date=datetime(2025, 7, 4),
    schedule="@once",
    catchup=False,
    tags=["ticks", "ohlcv"],
    max_active_tasks=1,
)
def tick_to_ohlcv_dag():

    # ① 가공해야 할 tick 파일 목록
    @task
    def get_files_to_process(year_month_list: list[str]) -> list[str]:
        kospi_files = [
            str(p)
            for p in KOSPI_RAW_DIR.iterdir()
            if p.name.endswith(".dat") and any(ym in p.name for ym in year_month_list)
        ]
        kosdaq_files = [
            str(p)
            for p in KOSDAQ_RAW_DIR.iterdir()
            if p.name.endswith(".dat") and any(ym in p.name for ym in year_month_list)
        ]
        return kospi_files + kosdaq_files

    # ② (NEW) 공시 Universe 조회를 전용 Task로 분리
    @task
    def build_disclosure_universe(year_month_list: list[str]) -> dict[str, list[str]]:
        """
        year_month_list 예: ["2023_12", "2024_01"]
        ⇒ {YYYYMMDD: [stock_code, …]} 형식의 딕셔너리 반환
        """
        # (1) 쿼리 기간 계산 ────────────────────────────────────────────────
        month_ranges: list[tuple[date, date]] = []
        for ym in year_month_list:
            y, m = map(int, ym.split("_"))
            first = date(y, m, 1)
            last = first + relativedelta(months=1) - timedelta(days=1)
            month_ranges.append((first, last))

        # (2) DB 연결 & 조회 ───────────────────────────────────────────────
        POSTGRES_USER = os.getenv("POSTGRES_USER")
        POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
        POSTGRES_DB = os.getenv("POSTGRES_DB")
        POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")

        db_url = (
            f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}"
            f"@postgres_events:{POSTGRES_PORT}/{POSTGRES_DB}"
        )
        engine = sa.create_engine(db_url, pool_pre_ping=True, future=True)

        # 여러 월을 하나의 쿼리로 처리 (BETWEEN OR …) -----------------------
        filters = " OR ".join(
            f"(disclosed_at AT TIME ZONE 'Asia/Seoul')::date "
            f"BETWEEN :start_{i} AND :end_{i}"
            for i, _ in enumerate(month_ranges)
        )
        sql = sa.text(
            f"""
            SELECT
              (disclosed_at AT TIME ZONE 'Asia/Seoul')::date AS disclosed_at,
              stock_code
            FROM disclosure_events
            WHERE ({filters})
              AND stock_code IS NOT NULL
            """
        )

        params = {}
        for i, (s, e) in enumerate(month_ranges):
            params[f"start_{i}"] = s
            params[f"end_{i}"] = e

        with engine.connect() as conn:
            df_events = pd.read_sql(sql, conn, params=params)

        # (3) 집계 & 딕셔너리화 ────────────────────────────────────────────
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
        return universe

    # ③ tick → 1‑분 OHLCV 변환
    @task
    def convert(file_path: str, universe: dict[str, list[str]]) -> list[str]:
        """
        파일 하나를 받아:
        (a) 실제 포함된 날짜 추출 →
        (b) Universe와 교차 →
        (c) 1‑분 OHLCV 생성
        """
        p = Path(file_path)
        out_paths: list[str] = []

        # 출력 디렉터리 결정 ---------------------------------------------------
        OUT_DIR = (
            KOSPI_OUT_DIR
            if "KOSPI" in str(p)
            else KOSDAQ_OUT_DIR if "KOSDAQ" in str(p) else None
        )
        if OUT_DIR is None:
            raise ValueError(f"Unknown market type for file: {p}")

        for date_str, codes in universe.items():
            print(f"Processing date: {date_str}")
            if not codes:  # 공시 종목 없으면 skip
                continue

            out = OUT_DIR / f"{date_str}_1m.parquet"
            if out.exists():
                continue

            (
                pl.scan_csv(
                    p,
                    separator="|",
                    has_header=False,
                    new_columns=COLS,
                    low_memory=True,
                    infer_schema_length=1000,
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
                .sink_parquet(out, compression="zstd", row_group_size=50_000)
            )
            out_paths.append(str(out))

        return out_paths

    # ── DAG wiring ──────────────────────────────────────────────────────────
    files = get_files_to_process(year_month_list=["2023_12"])
    universe = build_disclosure_universe(year_month_list=["2023_12"])
    convert.partial(universe=universe).expand(file_path=files)


dag = tick_to_ohlcv_dag()
