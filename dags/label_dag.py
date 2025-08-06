# dags/event_reaction_returns.py
from __future__ import annotations

import os
import zoneinfo
from datetime import datetime, timedelta

import pandas as pd
import polars as pl
import sqlalchemy as sa
from airflow.decorators import dag, task
from dotenv import load_dotenv

load_dotenv()
from config import KOSDAQ_OUT_DIR, KOSPI_OUT_DIR, TZ

LAGS_MIN = [1, 3, 10, 15, 30, 60]  # 분 단위 지연

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


# ---------- helper ----------
def to_min(dt):
    """초·마이크로초를 0으로 만들어 분 단위로 맞춘다."""
    return dt.replace(second=0, microsecond=0)


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
        disclosure_events 테이블에 수익률 컬럼(ret_1m, ret_3m, ret_10m, ret_15m, ret_30m, ret_60m)이
        없으면 자동으로 추가합니다.
        """
        alter_sql = """
        ALTER TABLE disclosure_events
            ADD COLUMN IF NOT EXISTS ret_1m  DOUBLE PRECISION,
            ADD COLUMN IF NOT EXISTS ret_3m  DOUBLE PRECISION,
            ADD COLUMN IF NOT EXISTS ret_10m DOUBLE PRECISION,
            ADD COLUMN IF NOT EXISTS ret_15m DOUBLE PRECISION,
            ADD COLUMN IF NOT EXISTS ret_30m DOUBLE PRECISION,
            ADD COLUMN IF NOT EXISTS ret_60m DOUBLE PRECISION;
        """
        with ENGINE.begin() as conn:
            conn.execute(sa.text(alter_sql))

    # ────────────────────────────────────────────────────────────────
    @task
    def fetch_events() -> list[dict]:
        """
        disclosure_events 중,
        * 발표 시각이 국내 장중(09:00~15:30, KST) 사이에 발생했고
        * OHLCV Parquet 파일이 존재하는 날짜만 반환
        반환 형식: [{'event_id': …, 'ts': datetime, 'code': str, 'market': str}, …]
        """
        with ENGINE.connect() as conn:
            sql = """
            SELECT
                id                    AS event_id,
                (disclosed_at AT TIME ZONE 'Asia/Seoul') AS ts_kst,
                stock_code,
                market
            FROM disclosure_events
            WHERE stock_code IS NOT NULL
              AND disclosed_at IS NOT NULL
              AND market IN ('KOSPI', 'KOSDAQ')
              -- 09:00~15:30 사이 공시만 (KST 기준)
              AND (
                  (EXTRACT(HOUR FROM disclosed_at AT TIME ZONE 'Asia/Seoul') BETWEEN 9 AND 14)
                  OR
                  (EXTRACT(HOUR FROM disclosed_at AT TIME ZONE 'Asia/Seoul') = 15
                   AND EXTRACT(MINUTE FROM disclosed_at AT TIME ZONE 'Asia/Seoul') <= 30)
              )
            """
            df = pd.read_sql(sql, conn)

        if df.empty:
            print("No events found")
            return []

        df["ts_kst"] = df["ts_kst"].dt.tz_localize("Asia/Seoul")

        # 문자열은 isoformat()으로( +09:00 형태 보장 )
        df["event_ts"] = df["ts_kst"].apply(lambda x: x.isoformat())
        df["date_int"] = df["ts_kst"].dt.strftime("%Y%m%d").astype(int)

        return df[["event_id", "stock_code", "event_ts", "date_int", "market"]].to_dict(
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
            # 시장별로 이벤트 그룹화
            kospi_events = [ev for ev in ev_list if ev["market"] == "KOSPI"]
            kosdaq_events = [ev for ev in ev_list if ev["market"] == "KOSDAQ"]

            # KOSPI 이벤트 처리
            if kospi_events:
                kospi_pq_path = KOSPI_OUT_DIR / f"{date_int:08d}_1m.parquet"
                if kospi_pq_path.exists():
                    ohlcv = pl.read_parquet(
                        kospi_pq_path, columns=["ts", "종목코드", "close"]
                    ).rename({"종목코드": "code"})

                    # 가격 맵 생성
                    price_map = {
                        (
                            row["code"],
                            row["ts"].replace(tzinfo=zoneinfo.ZoneInfo(TZ)),
                        ): row["close"]
                        for row in ohlcv.iter_rows(named=True)
                    }

                    # KOSPI 이벤트들의 수익률 계산
                    updates.extend(
                        calculate_returns_for_events(kospi_events, price_map)
                    )

            # KOSDAQ 이벤트 처리
            if kosdaq_events:
                kosdaq_pq_path = KOSDAQ_OUT_DIR / f"{date_int:08d}_1m.parquet"
                if kosdaq_pq_path.exists():
                    ohlcv = pl.read_parquet(
                        kosdaq_pq_path, columns=["ts", "종목코드", "close"]
                    ).rename({"종목코드": "code"})

                    # 가격 맵 생성
                    price_map = {
                        (
                            row["code"],
                            row["ts"].replace(tzinfo=zoneinfo.ZoneInfo(TZ)),
                        ): row["close"]
                        for row in ohlcv.iter_rows(named=True)
                    }

                    # KOSDAQ 이벤트들의 수익률 계산
                    updates.extend(
                        calculate_returns_for_events(kosdaq_events, price_map)
                    )

        if not updates:
            return

        print("updates: ", updates)

        # ---------- bulk UPDATE ----------
        with ENGINE.begin() as conn:
            stmt = sa.text(
                """
                UPDATE disclosure_events AS d
                SET
                    ret_1m  = COALESCE(:ret_1m,  d.ret_1m),
                    ret_3m  = COALESCE(:ret_3m,  d.ret_3m),
                    ret_10m = COALESCE(:ret_10m, d.ret_10m),
                    ret_15m = COALESCE(:ret_15m, d.ret_15m),
                    ret_30m = COALESCE(:ret_30m, d.ret_30m),
                    ret_60m = COALESCE(:ret_60m, d.ret_60m)
                WHERE d.id = :event_id
                """
            )

            # executemany → 한 번에 다중 업데이트
            result = conn.execute(stmt, updates)
            print("rows matched → modified:", result.rowcount)

    def calculate_returns_for_events(events: list[dict], price_map: dict) -> list[dict]:
        """이벤트 리스트에 대해 수익률을 계산하는 헬퍼 함수"""
        RET_COLS = {
            1: "ret_1m",
            3: "ret_3m",
            10: "ret_10m",
            15: "ret_15m",
            30: "ret_30m",
            60: "ret_60m",
        }
        updates = []

        for ev in events:
            ts0 = to_min(
                datetime.fromisoformat(ev["event_ts"]).replace(
                    tzinfo=zoneinfo.ZoneInfo(TZ)
                )
            )
            p0 = price_map.get((ev["stock_code"], ts0))
            print(f"ev: {ev}, ts0: {ts0}, p0: {p0}")

            if p0 is None or p0 == 0:
                continue

            rec = {"event_id": ev["event_id"]}
            for lag in LAGS_MIN:
                p_lag = price_map.get((ev["stock_code"], ts0 + timedelta(minutes=lag)))
                if p_lag:
                    rec[RET_COLS[lag]] = round((p_lag / p0 - 1.0) * 100.0, 2)

            # ★ 계산된 수익률이 하나라도 있을 때만 업데이트
            if any(k in rec for k in RET_COLS.values()):
                # ★ 누락된 칼럼을 None 으로 채워 넣음
                for col in RET_COLS.values():
                    rec.setdefault(col, None)
                updates.append(rec)

        return updates

    # ───────────────────────── DAG 의 Task 의존성 ───────────────────
    columns_task = ensure_columns()
    events_task = fetch_events()
    returns_task = compute_returns(events_task)
    columns_task >> returns_task


dag = event_reaction_returns_dag()
