# dags/event_reaction_returns.py
from __future__ import annotations

import os
import zoneinfo
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

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


def get_monthly_ranges(
    start_date: datetime, end_date: datetime
) -> list[tuple[datetime, datetime]]:
    """시작일과 종료일 사이의 월별 범위를 반환합니다."""
    monthly_ranges = []
    current_date = start_date.replace(day=1)

    while current_date <= end_date:
        # 해당 월의 마지막 날
        if current_date.month == 12:
            next_month = current_date.replace(year=current_date.year + 1, month=1)
        else:
            next_month = current_date.replace(month=current_date.month + 1)

        month_end = next_month - timedelta(days=1)

        # 실제 end_date와 비교하여 더 작은 값 사용
        actual_end = min(month_end, end_date)

        monthly_ranges.append((current_date, actual_end))
        current_date = next_month

    return monthly_ranges


import zoneinfo
from datetime import datetime

import polars as pl


def build_price_map_with_ffill(ohlcv: pl.DataFrame, date_int: int) -> dict:
    """
    해당 날짜(09:00~15:30, 1분 간격)의 모든 분 타임스탬프를 만들고,
    종목별로 close를 forward fill하여 (code, ts) -> close 맵을 만든다.
    """
    tz = zoneinfo.ZoneInfo(TZ)

    # 1) 날짜 범위 (09:00 ~ 15:30, inclusive) 분단위 인덱스 생성
    d = datetime.strptime(str(date_int), "%Y%m%d")
    start = d.replace(hour=9, minute=0, second=0, microsecond=0, tzinfo=tz)
    end = d.replace(hour=15, minute=30, second=0, microsecond=0, tzinfo=tz)

    ts_idx = pl.datetime_range(
        start, end, interval="1m", time_unit="us", time_zone=TZ, eager=True
    )
    idx_df = pl.DataFrame({"ts": ts_idx})

    # 2) ts를 TZ-aware로 맞추기 (parquet이 naive면 timezone 부여)
    ohlcv = ohlcv.with_columns(pl.col("ts").dt.replace_time_zone(TZ))

    # 3) 종목코드 × 모든 분단위 ts 생성(크로스 조인) 후 원본과 left join
    codes = ohlcv.select("code").unique()
    full_grid = codes.join(idx_df, how="cross")

    df = (
        full_grid.join(ohlcv, on=["code", "ts"], how="left")
        .sort(["code", "ts"])
        .with_columns(pl.col("close").forward_fill().alias("close"))
    )

    # 4) dict로 변환 (None 걸러내기)
    price_map = {
        (row["code"], row["ts"]): row["close"]
        for row in df.iter_rows(named=True)
        if row["close"] is not None
    }
    return price_map


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
    def fetch_events_monthly(start_date: datetime, end_date: datetime) -> list[dict]:
        """
        지정된 월 범위의 disclosure_events 중,
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
              AND market IN ('KOSPI', 'KOSDAQ', 'KOSDAQ GLOBAL')
              -- 09:00~15:30 사이 공시만 (KST 기준)
              AND (
                  (EXTRACT(HOUR FROM disclosed_at AT TIME ZONE 'Asia/Seoul') BETWEEN 9 AND 14)
                  OR
                  (EXTRACT(HOUR FROM disclosed_at AT TIME ZONE 'Asia/Seoul') = 15
                   AND EXTRACT(MINUTE FROM disclosed_at AT TIME ZONE 'Asia/Seoul') <= 30)
              )
              -- 월별 범위 필터링
              AND disclosed_at >= %(start_date)s
              AND disclosed_at <= %(end_date)s
            """
            df = pd.read_sql(
                sql, conn, params={"start_date": start_date, "end_date": end_date}
            )

        if df.empty:
            print(f"No events found for {start_date} to {end_date}")
            return []

        df["ts_kst"] = df["ts_kst"].dt.tz_localize("Asia/Seoul")

        # 문자열은 isoformat()으로( +09:00 형태 보장 )
        df["event_ts"] = df["ts_kst"].apply(lambda x: x.isoformat())
        df["date_int"] = df["ts_kst"].dt.strftime("%Y%m%d").astype(int)

        return df[["event_id", "stock_code", "event_ts", "date_int", "market"]].to_dict(
            orient="records"
        )

    @task
    def fetch_events(
        start_date: datetime | None = None,
        end_date: datetime | None = None,
    ) -> list[list[dict]]:
        """
        전체 기간을 월별로 분할하여 이벤트를 가져옵니다.
        반환 형식: [[월1_이벤트들], [월2_이벤트들], ...]
        """
        monthly_ranges = get_monthly_ranges(start_date, end_date)
        print(
            f"Processing {len(monthly_ranges)} monthly ranges from {start_date} to {end_date}"
        )

        # 각 월별 범위에 대해 이벤트 가져오기
        monthly_events = []
        for month_start, month_end in monthly_ranges:
            month_events = fetch_events_monthly(month_start, month_end)
            if month_events:  # 빈 리스트가 아닌 경우만 추가
                monthly_events.append(month_events)
                print(
                    f"Found {len(month_events)} events for {month_start.strftime('%Y-%m')}"
                )

        return monthly_events

    # ────────────────────────────────────────────────────────────────
    @task
    def compute_returns_monthly(events: list[dict]) -> None:
        """
        한 달의 이벤트들에 대해 ret_* 컬럼을 disclosure_events 테이블에 직접 UPDATE.
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
            kosdaq_events = [
                ev
                for ev in ev_list
                if ev["market"] == "KOSDAQ" or ev["market"] == "KOSDAQ GLOBAL"
            ]

            # KOSPI 이벤트 처리
            if kospi_events:
                kospi_pq_path = KOSPI_OUT_DIR / f"{date_int:08d}_1m.parquet"
                if kospi_pq_path.exists():
                    ohlcv = pl.read_parquet(
                        kospi_pq_path, columns=["ts", "종목코드", "close"]
                    ).rename({"종목코드": "code"})

                    price_map = build_price_map_with_ffill(ohlcv, date_int)

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

                    price_map = build_price_map_with_ffill(ohlcv, date_int)

                    # KOSDAQ 이벤트들의 수익률 계산
                    updates.extend(
                        calculate_returns_for_events(kosdaq_events, price_map)
                    )

        if not updates:
            return

        print(f"Processing {len(updates)} updates for monthly batch")

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
            print(f"Monthly batch completed: {result.rowcount} rows updated")

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
    events_task = fetch_events(
        start_date=datetime(2022, 7, 1),
        end_date=datetime(2022, 12, 31),
    )
    returns_task = compute_returns_monthly.expand(events=events_task)

    columns_task >> returns_task


dag = event_reaction_returns_dag()
