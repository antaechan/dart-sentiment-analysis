# dags/tick_to_ohlcv.py
from __future__ import annotations
from pathlib import Path
from datetime import datetime

import polars as pl
from airflow.decorators import dag, task

RAW_DIR = Path("/opt/airflow/data/KOSPI/unzip")
OUT_DIR = Path("/opt/airflow/data/KOSPI/ohlcv")
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
        """Return all raw tick files that *don't* have a matching parquet yet."""
        return [
            str(p)
            for p in RAW_DIR.iterdir()
            if p.name.endswith(".dat")
            and not (OUT_DIR / f"{p.stem}_1m.parquet").exists()
        ]

    @task
    def convert(file_path: str) -> list[str]:
        """Convert one .dat.gz file to 1-minute OHLCV parquet."""
        p   = Path(file_path)
        out_paths: list[str] = []

        # ① 먼저 날짜 목록만 추출 (칼럼 1개라 메모리 미미)
        dates = (
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



    # ── ② 날짜별 처리 ──────────────────────────────────────
        for d in dates:
            print("date", d)
            date_str = f"{d:08d}"
            out = OUT_DIR / f"{date_str}_1m.parquet"
            if out.exists():
                continue

            # ── (a) 날짜 필터 + ts 생성  ───────────────────────
            lf = (
                pl.scan_csv(
                    p, separator="|", has_header=False,
                    new_columns=COLS, low_memory=True,
                    infer_schema_length=1000,
                )
                .select(KEEP)
                .filter(pl.col("체결일자") == d)        # push-down
                .with_columns(
                    pl.datetime(
                        (pl.col("체결일자") // 10_000),
                        ((pl.col("체결일자") // 100) % 100).cast(pl.UInt8),
                        (pl.col("체결일자") % 100).cast(pl.UInt8),
                        (pl.col("체결시각") // 10_000_000).cast(pl.UInt8),
                        ((pl.col("체결시각") // 100_000) % 100).cast(pl.UInt8),
                        ((pl.col("체결시각") //   1_000) % 100).cast(pl.UInt8),
                        (pl.col("체결시각") % 1_000) * 1_000,
                        time_zone=TZ,
                    ).alias("ts")
                )
                .drop(["체결일자", "체결시각"])
            )

            # ── (b) 먼저 "정렬됐다고 가정"하고 group_by_dynamic 시도 ──
            try:
                (
                    lf.with_columns(pl.col("ts").set_sorted())   # 0.21+
                    .group_by_dynamic(index_column="ts", every="1m",
                                        by="종목코드", closed="left")
                    .agg(
                        open   = pl.col("체결가격").first(),
                        high   = pl.col("체결가격").max(),
                        low    = pl.col("체결가격").min(),
                        close  = pl.col("체결가격").last(),
                        volume = pl.col("체결수량").sum(),
                    )
                    .sink_parquet(out, compression="zstd",
                                    row_group_size=50_000)
                )
            except pl.ComputeError:
                # ── (c) "정렬 안 됨" 오류가 난 경우에만 **부분 정렬** ──
                print(f"{d} sort error occured")

                (
                    lf.sort("ts")                              # 하루치만 정렬
                    .group_by_dynamic(index_column="ts", every="1m",
                                        by="종목코드", closed="left")
                    .agg(
                        open   = pl.col("체결가격").first(),
                        high   = pl.col("체결가격").max(),
                        low    = pl.col("체결가격").min(),
                        close  = pl.col("체결가격").last(),
                        volume = pl.col("체결수량").sum(),
                    )
                    .sink_parquet(out, compression="zstd", row_group_size=50_000)
                )

            out_paths.append(str(out))

        return out_paths
    # 🎉 Dynamic mapping fan-out
    convert.expand(file_path=list_files())


dag = tick_to_ohlcv_dag()
