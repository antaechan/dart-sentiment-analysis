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

load_dotenv()

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
]
KEEP = ["종목코드", "체결가격", "체결수량", "체결일자", "체결시각"]


@dag(
    start_date=datetime(2025, 7, 4),
    schedule="@once",
    catchup=False,
    tags=["ticks", "ohlcv"],
    max_active_tasks=1,
)
def tick_to_ohlcv_dag():

    @task
    def get_files_to_process(year_month_list: list[str]) -> list[str]:
        """Return all raw tick files that *don't* have a matching parquet yet."""
        kospi_files = [
            str(p)
            for p in KOSPI_RAW_DIR.iterdir()
            if p.name.endswith(".dat")
            and any(year_month in p.name for year_month in year_month_list)
        ]

        kosdaq_files = [
            str(p)
            for p in KOSDAQ_RAW_DIR.iterdir()
            if p.name.endswith(".dat")
            and any(year_month in p.name for year_month in year_month_list)
        ]

        return kospi_files + kosdaq_files

    @task
    def convert(file_path: str) -> list[str]:
        """
        (1) tick 파일에 포함된 날짜 목록을 뽑고,
        (2) disclosure_events에서 같은 날짜에 공시가 나온 종목코드 universe를 만들고,
        (3) universe 안에 있는 종목만 그룹핑하여 1-분 OHLCV를 저장합니다.
        """
        p = Path(file_path)
        out_paths: list[str] = []

        # 파일 경로에 따라 출력 디렉토리 결정
        if "KOSPI" in str(p):
            OUT_DIR = KOSPI_OUT_DIR
        elif "KOSDAQ" in str(p):
            OUT_DIR = KOSDAQ_OUT_DIR
        else:
            raise ValueError(f"Unknown market type for file: {p}")

        # ── ① tick 파일 안에 실제로 존재하는 '체결일자' 목록 ───────────────────────
        dates: list[int] = (
            pl.scan_csv(
                p,
                separator="|",
                has_header=False,
                new_columns=COLS,
                low_memory=True,
            )
            .select(["체결일자"])
            .unique()
            .sort("체결일자")
            .collect(streaming=True)["체결일자"]
            .to_list()
        )

        print(dates)
        if not dates:
            return out_paths  # 비어 있으면 바로 종료

        # ── ② disclosure_events에서 공시가 있었던 종목 universe 확보 ────────────────
        #     공시 테이블은 YYYYMMDD → 종목코드 배열(dict) 형태로 변환

        POSTGRES_USER = os.getenv("POSTGRES_USER")
        POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
        POSTGRES_DB = os.getenv("POSTGRES_DB")
        POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")

        db_url = f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@postgres_events:{POSTGRES_PORT}/{POSTGRES_DB}"
        engine = sa.create_engine(db_url, pool_pre_ping=True, future=True)
        # 1️⃣ 파이썬에서 date 로 변환
        dates_date = [datetime.strptime(str(d), "%Y%m%d").date() for d in dates]

        with engine.connect() as conn:
            sql = sa.text(
                """
                SELECT
                    -- UTC → KST 변환 후 날짜만 추출
                    (disclosed_at AT TIME ZONE 'Asia/Seoul')::date AS disclosed_at,
                    stock_code
                FROM disclosure_events
                WHERE (disclosed_at AT TIME ZONE 'Asia/Seoul')::date IN :dates
                    AND stock_code IS NOT NULL
                """
            ).bindparams(sa.bindparam("dates", expanding=True))

            # DB → pandas
            df_events = pd.read_sql(sql, conn, params={"dates": dates_date})

        # pandas → Polars → 집계
        events_pl = pl.from_pandas(df_events)
        events = events_pl.group_by("disclosed_at").agg(
            pl.col("stock_code").unique().alias("codes")
        )

        # {YYYYMMDD: [code, …]} 딕셔너리
        universe: dict[int, list[str]] = {
            int(d.strftime("%Y%m%d")): codes
            for d, codes in zip(
                events["disclosed_at"].to_list(), events["codes"].to_list()
            )
        }

        # ── ③ 날짜별 변환 ─────────────────────────────────────────────────────────────
        for d in dates:
            print("date: ", d)
            codes = universe.get(d, [])
            if not codes:  # 공시 종목이 없으면 스킵
                continue

            date_str = f"{d:08d}"
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
                    (pl.col("체결일자") == d)
                    & (pl.col("종목코드").is_in(codes))  # ← **공시 종목 필터**
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
                    index_column="ts", every="1m", by="종목코드", closed="left"
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

    # 🎉 Dynamic mapping fan-out
    files = get_files_to_process(year_month_list=["2023_12"])
    convert.expand(file_path=files)


dag = tick_to_ohlcv_dag()
