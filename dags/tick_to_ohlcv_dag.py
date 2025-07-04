# dags/tick_to_ohlcv.py
from __future__ import annotations
from pathlib import Path
from datetime import datetime

import polars as pl
from airflow.decorators import dag, task

RAW_DIR = Path("/opt/airflow/data")
OUT_DIR = Path("/opt/airflow/data/ohlcv")
TZ      = "Asia/Seoul"

COLS  = [
    "일자", "대량매매구분코드", "정규시간외구분코드", "종목코드", "종목인덱스", "체결번호", "체결가격", "체결수량",
    "체결유형코드", "체결일자", "체결시각", "근월물체결가격", "원월물체결가격", "매수회원번호", "매수호가유형코드",
    "매수자사주신고서ID", "매수자사주매매방법코드", "매수매도유형코드", "매수위탁자기구분코드", "매수위탁사번호",
    "매수PT구분코드", "매수투자자구분코드", "매수외국인투자자구분코드", "매수호가접수번호", "매도회원번호",
    "매도호가유형코드", "매도자사주신고서ID", "매도자사주매매방법코드", "매도매도유형코드", "매도위탁자기구분코드",
    "매도위탁사번호", "매도PT구분코드", "매도투자자구분코드", "매도외국인투자자구분코드", "매도호가접수번호",
    "시가", "고가", "저가", "직전가격", "누적체결수량", "누적거래대금", "최종매도매수구분코드", "LP보유수량",
    "데이터구분", "메세지일련번호", "매수프로그램호가신고구분코드", "매도프로그램호가신고구분코드", "보드ID",
    "세션ID", "실시간상한가", "실시간하한가"
]
KEEP = ["종목코드","체결가격","체결수량","체결일자","체결시각"]

@dag(
    start_date=datetime(2025, 7, 4),
    schedule="@once",
    catchup=False,
    tags=["ticks", "ohlcv"],
    max_active_tasks=1,
)
def tick_to_ohlcv_dag():

    @task
    def list_files() -> list[str]:
        """Return all raw tick files that *don’t* have a matching parquet yet."""
        return [
            str(p)
            for p in RAW_DIR.iterdir()
            if p.name.endswith(".dat")
            and not (OUT_DIR / f"{p.stem}_1m.parquet").exists()
        ]

    @task
    def convert(file_path: str) -> str:
        """Convert one .dat.gz file to 1-minute OHLCV parquet."""
        p   = Path(file_path)
        out = OUT_DIR / f"{p.stem}_1m.parquet"

        (pl.scan_csv(
            p,
            separator="|",
            new_columns=COLS,
            ignore_errors=True,
            infer_schema_length=500,
            has_header=False,
            low_memory=True,
        )
        .select(KEEP)
        .with_columns(
            pl.datetime(
                pl.col("체결일자").str.slice(0,4).cast(pl.Int32),
                pl.col("체결일자").str.slice(4,2).cast(pl.Int8),
                pl.col("체결일자").str.slice(6,2).cast(pl.Int8),
                pl.col("체결시각").str.zfill(9).str.slice(0,2).cast(pl.Int8),
                pl.col("체결시각").str.zfill(9).str.slice(2,2).cast(pl.Int8),
                pl.col("체결시각").str.zfill(9).str.slice(4,2).cast(pl.Int8),
                pl.col("체결시각").str.zfill(9).str.slice(6,3).cast(pl.Int32) * 1_000,
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
            low =pl.col("체결가격").min(),
            close=pl.col("체결가격").last(),
            volume=pl.col("체결수량").sum(),
        )
        .sink_parquet(out, compression="zstd", maintain_order=True)
        )

        return str(out)
    # 🎉 Dynamic mapping fan-out
    convert.expand(file_path=list_files())


dag = tick_to_ohlcv_dag()
