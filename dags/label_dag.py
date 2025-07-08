# dags/event_reaction_returns.py
from __future__ import annotations

import os
import zoneinfo
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import polars as pl
import sqlalchemy as sa
from airflow.decorators import dag, task

# ─────────── 공통 상수 ───────────────────────────────────────────────
RAW_PARQUET_DIR = Path("/opt/airflow/data/KOSPI/ohlcv")  # 1 분 OHLCV 저장소
TZ = zoneinfo.ZoneInfo("Asia/Seoul")
LAGS_MIN = [1, 3, 10, 60]  # 분 단위 지연


# ─────────── DB 커넥션 ─────────────────────────────────────────────
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_URL = (
    f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}"
    f"@postgres_events:{POSTGRES_PORT}/{POSTGRES_DB}"
)
ENGINE = sa.create_engine(DB_URL, pool_pre_ping=True, future=True)


@dag(
    start_date=datetime(2025, 7, 4),
    schedule="@once",  # 필요에 따라 cron 으로 변경
    catchup=False,
    tags=["ticks", "event_returns"],
    max_active_tasks=1,
)
def event_reaction_returns_dag():
    @task
    def ensure_columns() -> None:
        """
        disclosure_events 테이블에 수익률 컬럼(ret_1m, ret_3m, ret_10m, ret_60m)이
        없으면 자동으로 추가합니다.
        """
        alter_sql = """
        ALTER TABLE disclosure_events
            ADD COLUMN IF NOT EXISTS ret_1m  DOUBLE PRECISION,
            ADD COLUMN IF NOT EXISTS ret_3m  DOUBLE PRECISION,
            ADD COLUMN IF NOT EXISTS ret_10m DOUBLE PRECISION,
            ADD COLUMN IF NOT EXISTS ret_60m DOUBLE PRECISION;
        """
        with ENGINE.begin() as conn:
            conn.execute(sa.text(alter_sql))

    # ────────────────────────────────────────────────────────────────
    @task
    def fetch_events() -> list[dict]:
        """
        disclosure_events 중,
        * 발표 시각이 국내 장중(09:00~15:30, KST) **이전** 에 발생했고
        * OHLCV Parquet 파일이 존재하는 날짜만 반환
        반환 형식: [{'event_id': …, 'ts': datetime, 'code': str}, …]
        """
        with ENGINE.connect() as conn:
            sql = """
            SELECT
                id                    AS event_id,
                (disclosed_at AT TIME ZONE 'Asia/Seoul') AS ts_kst,
                stock_code
            FROM disclosure_events
            WHERE stock_code IS NOT NULL
              AND disclosed_at IS NOT NULL
              -- 09:00~15:30 사이 공시만 (KST 기준)
              AND EXTRACT(HOUR   FROM disclosed_at AT TIME ZONE 'Asia/Seoul') BETWEEN 9 AND 15
              AND EXTRACT(MINUTE FROM disclosed_at AT TIME ZONE 'Asia/Seoul') <= 30
            """
            df = pd.read_sql(sql, conn)

        if df.empty:
            return []

        df["event_ts"] = df["ts_kst"].dt.strftime("%Y-%m-%dT%H:%M:%S%z")
        df["date_int"] = df["ts_kst"].dt.strftime("%Y%m%d").astype(int)

        return df[["event_id", "stock_code", "event_ts", "date_int"]].to_dict(
            orient="records"
        )

    # ────────────────────────────────────────────────────────────────
    @task
    def compute_returns(events: list[dict]) -> None:
        """
        ret_* 컬럼을 disclosure_events 테이블에 직접 UPDATE.
        """
        if not events:
            return

        # 날짜별 parquet 한 번씩만 읽도록 묶기
        events_by_date: dict[int, list[dict]] = {}
        for ev in events:
            events_by_date.setdefault(ev["date_int"], []).append(ev)

        updates = []  # [{event_id:…, ret_1m:…}, …] 누적

        for date_int, ev_list in events_by_date.items():
            pq_path = RAW_PARQUET_DIR / f"{date_int:08d}_1m.parquet"
            if not pq_path.exists():
                continue

            ohlcv = pl.read_parquet(
                pq_path, columns=["ts", "종목코드", "close"]
            ).rename({"종목코드": "code"})

            price_map = {
                (row["code"], row["ts"].replace(tzinfo=TZ)): row["close"]
                for row in ohlcv.iter_rows(named=True)
            }

            for ev in ev_list:
                ts0 = datetime.fromisoformat(ev["event_ts"]).replace(tzinfo=TZ)
                p0 = price_map.get((ev["stock_code"], ts0))
                if p0 is None or p0 == 0:
                    continue

                rec = {"event_id": ev["event_id"]}
                for lag in LAGS_MIN:
                    p_lag = price_map.get(
                        (ev["stock_code"], ts0 + timedelta(minutes=lag))
                    )
                    if p_lag:
                        rec[f"ret_{lag}m"] = (p_lag / p0 - 1.0) * 100.0
                if len(rec) > 1:  # 수익률이 하나 이상 계산된 경우
                    updates.append(rec)

        if not updates:
            return

        # ---------- bulk UPDATE ----------
        with ENGINE.begin() as conn:
            stmt = sa.text(
                """
                UPDATE disclosure_events AS d
                SET
                    ret_1m  = COALESCE(:ret_1m,  d.ret_1m),
                    ret_3m  = COALESCE(:ret_3m,  d.ret_3m),
                    ret_10m = COALESCE(:ret_10m, d.ret_10m),
                    ret_60m = COALESCE(:ret_60m, d.ret_60m)
                WHERE d.id = :event_id
                """
            )

            # executemany → 한 번에 다중 업데이트
            conn.execute(stmt, updates)

    # ───────────────────────── DAG 의 Task 의존성 ───────────────────
    ensure_columns()
    compute_returns(fetch_events())


dag = event_reaction_returns_dag()
