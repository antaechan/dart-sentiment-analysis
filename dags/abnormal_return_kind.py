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

# 테이블 및 컬럼 설정
TABLE_NAME = "abnormal_return_kind"
ABN_RET_COLUMNS = {
    -60: "abn_ret_minus_60m",
    -50: "abn_ret_minus_50m",
    -40: "abn_ret_minus_40m",
    -30: "abn_ret_minus_30m",
    -20: "abn_ret_minus_20m",
    -10: "abn_ret_minus_10m",
    10: "abn_ret_10m",
    20: "abn_ret_20m",
    30: "abn_ret_30m",
    40: "abn_ret_40m",
    50: "abn_ret_50m",
    60: "abn_ret_60m",
}

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


def build_market_price_map_with_ffill(
    ohlcv: pl.DataFrame, date_int: int, market_code: str
) -> dict:
    """
    해당 날짜의 market 지수(KOSPI: KR7069500007, KOSDAQ: KR7229200001)의
    1분 간격 가격 맵을 만든다.
    """
    tz = zoneinfo.ZoneInfo(TZ)

    # 1) 날짜 범위 (09:00 ~ 15:30, inclusive) 분단위 인덱스 생성
    d = datetime.strptime(str(date_int), "%Y%m%d")
    start = d.replace(hour=9, minute=0, second=0, microsecond=0, tzinfo=tz)
    end = d.replace(hour=15, minute=20, second=0, microsecond=0, tzinfo=tz)

    ts_idx = pl.datetime_range(
        start, end, interval="1m", time_unit="us", time_zone=TZ, eager=True
    )
    idx_df = pl.DataFrame({"ts": ts_idx})

    # 2) ts를 TZ-aware로 맞추기 (parquet이 naive면 timezone 부여)
    ohlcv = ohlcv.with_columns(pl.col("ts").dt.replace_time_zone(TZ))

    # 3) market 지수 데이터만 필터링
    market_data = ohlcv.filter(pl.col("code") == market_code)

    if market_data.is_empty():
        return {}

    # 4) 모든 분단위 ts와 left join 후 forward fill
    df = (
        idx_df.join(market_data, on="ts", how="left")
        .sort("ts")
        .with_columns(pl.col("close").forward_fill().alias("close"))
    )

    # 5) dict로 변환 (None 걸러내기)
    price_map = {
        row["ts"]: row["close"]
        for row in df.iter_rows(named=True)
        if row["close"] is not None
    }
    return price_map


def build_price_map_with_ffill(
    ohlcv: pl.DataFrame, date_int: int, required_codes: set[str] = None
) -> dict:
    """
    필요한 종목만 처리하여 메모리 사용량을 크게 줄인 price_map 생성
    """
    tz = zoneinfo.ZoneInfo(TZ)

    # 1) 필요한 종목만 필터링 (메모리 절약)
    if required_codes:
        ohlcv = ohlcv.filter(pl.col("code").is_in(list(required_codes)))

    if ohlcv.is_empty():
        return {}

    # 2) 날짜 범위 (09:00 ~ 15:30, inclusive) 분단위 인덱스 생성
    d = datetime.strptime(str(date_int), "%Y%m%d")
    start = d.replace(hour=9, minute=0, second=0, microsecond=0, tzinfo=tz)
    end = d.replace(hour=15, minute=20, second=0, microsecond=0, tzinfo=tz)

    ts_idx = pl.datetime_range(
        start, end, interval="1m", time_unit="us", time_zone=TZ, eager=True
    )
    idx_df = pl.DataFrame({"ts": ts_idx})

    # 3) ts를 TZ-aware로 맞추기 (parquet이 naive면 timezone 부여)
    ohlcv = ohlcv.with_columns(pl.col("ts").dt.replace_time_zone(TZ))

    # 4) 종목별로 청크 단위 처리 (메모리 효율성)
    price_map = {}
    unique_codes = ohlcv.select("code").unique().to_series()

    for code in unique_codes:
        # 종목별로 작은 단위로 처리
        code_data = ohlcv.filter(pl.col("code") == code)
        code_grid = pl.DataFrame({"code": [code] * len(ts_idx), "ts": ts_idx})

        df = (
            code_grid.join(code_data, on=["code", "ts"], how="left")
            .sort("ts")
            .with_columns(pl.col("close").forward_fill().alias("close"))
        )

        # 딕셔너리에 추가
        for row in df.iter_rows(named=True):
            if row["close"] is not None:
                price_map[(row["code"], row["ts"])] = row["close"]

    return price_map


@dag(
    start_date=datetime(2025, 7, 4),
    schedule="@once",  # 필요에 따라 cron 으로 변경
    catchup=False,
    tags=["ticks", "event_returns"],
    max_active_tasks=1,
)
def event_reaction_returns_kind_dag():
    @task
    def create_table_if_not_exists() -> None:
        """
        abnormal_return 테이블이 없으면 생성합니다.
        """
        # 컬럼 정의 생성
        ret_columns_sql = ",\n            ".join(
            [f"{col} DOUBLE PRECISION" for col in ABN_RET_COLUMNS.values()]
        )

        create_sql = f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            id SERIAL PRIMARY KEY,
            event_id INTEGER NOT NULL UNIQUE,
            stock_code VARCHAR(20) NOT NULL,
            event_ts TIMESTAMP WITH TIME ZONE NOT NULL,
            market VARCHAR(20) NOT NULL,
            {ret_columns_sql},
            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            FOREIGN KEY (event_id) REFERENCES kind(id)
        );
        
        -- 인덱스 생성
        CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_event_id ON {TABLE_NAME}(event_id);
        CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_stock_code ON {TABLE_NAME}(stock_code);
        CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_event_ts ON {TABLE_NAME}(event_ts);
        """
        with ENGINE.begin() as conn:
            conn.execute(sa.text(create_sql))

    # ────────────────────────────────────────────────────────────────
    def fetch_events_monthly(start_date: datetime, end_date: datetime) -> list[dict]:
        """
        지정된 월 범위의 kind 테이블 중,
        * 발표 시각이 국내 장중(09:00~15:20, KST) 사이에 발생했고
        * OHLCV Parquet 파일이 존재하는 날짜만 반환
        반환 형식: [{'event_id': …, 'ts': datetime, 'code': str, 'market': str}, …]
        """
        with ENGINE.connect() as conn:
            sql = """
            SELECT
                id                    AS event_id,
                disclosed_at,
                stock_code,
                market
            FROM kind
            WHERE stock_code IS NOT NULL
              AND stock_code != ''
              AND disclosed_at IS NOT NULL
              AND label IS NOT NULL
              AND market IN ('KOSPI', 'KOSDAQ', 'KOSDAQ GLOBAL')
              -- 09:00~15:20 사이 공시만 (KST 기준)
              AND (
                  (EXTRACT(HOUR FROM disclosed_at) BETWEEN 9 AND 14)
                  OR
                  (EXTRACT(HOUR FROM disclosed_at) = 15
                   AND EXTRACT(MINUTE FROM disclosed_at) <= 20)
              )
              -- 월별 범위 필터링
              AND disclosed_at >= %(start_date)s
              AND disclosed_at <= %(end_date)s
            ORDER BY disclosed_at
            """
            df = pd.read_sql(
                sql, conn, params={"start_date": start_date, "end_date": end_date}
            )

        if df.empty:
            print(f"No events found for {start_date} to {end_date}")
            return []

        # 문자열은 strftime으로 변환 (timezone 정보 제거)
        df["event_ts"] = df["disclosed_at"].dt.strftime("%Y-%m-%dT%H:%M:%S")
        df["date_int"] = df["disclosed_at"].dt.strftime("%Y%m%d").astype(int)

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
        한 달의 이벤트들에 대해 abn_ret_* 컬럼을 abnormal_return 테이블에 INSERT.
        """
        if not events:
            return

        # 날짜별 parquet 한 번씩만 읽도록 묶기
        events_by_date: dict[int, list[dict]] = {}
        for ev in events:
            events_by_date.setdefault(ev["date_int"], []).append(ev)

        updates = []  # [{event_id:…, abn_ret_1m:…}, …] 누적

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

                    # 필요한 종목 코드만 추출하여 메모리 효율성 향상
                    required_codes = set(ev["stock_code"] for ev in kospi_events)
                    price_map = build_price_map_with_ffill(
                        ohlcv, date_int, required_codes
                    )
                    market_price_map = build_market_price_map_with_ffill(
                        ohlcv, date_int, "KR7069500007"
                    )

                    # KOSPI 이벤트들의 abnormal return 계산
                    updates.extend(
                        calculate_abnormal_returns_for_events(
                            kospi_events, price_map, market_price_map
                        )
                    )

            # KOSDAQ 이벤트 처리
            if kosdaq_events:
                kosdaq_pq_path = KOSDAQ_OUT_DIR / f"{date_int:08d}_1m.parquet"
                kospi_pq_path = KOSPI_OUT_DIR / f"{date_int:08d}_1m.parquet"

                if kosdaq_pq_path.exists() and kospi_pq_path.exists():
                    # 개별 종목 데이터 (KOSDAQ)
                    kosdaq_ohlcv = pl.read_parquet(
                        kosdaq_pq_path, columns=["ts", "종목코드", "close"]
                    ).rename({"종목코드": "code"})

                    # Market 지수 데이터 (KOSPI 디렉토리에서)
                    kospi_ohlcv = pl.read_parquet(
                        kospi_pq_path, columns=["ts", "종목코드", "close"]
                    ).rename({"종목코드": "code"})

                    # 필요한 종목 코드만 추출하여 메모리 효율성 향상
                    required_codes = set(ev["stock_code"] for ev in kosdaq_events)
                    price_map = build_price_map_with_ffill(
                        kosdaq_ohlcv, date_int, required_codes
                    )
                    market_price_map = build_market_price_map_with_ffill(
                        kospi_ohlcv, date_int, "KR7229200001"
                    )

                    # KOSDAQ 이벤트들의 abnormal return 계산
                    updates.extend(
                        calculate_abnormal_returns_for_events(
                            kosdaq_events, price_map, market_price_map
                        )
                    )

        if not updates:
            return

        print(f"Processing {len(updates)} inserts for monthly batch")

        # ---------- bulk INSERT ----------
        ret_columns_str = ", ".join(ABN_RET_COLUMNS.values())
        ret_values_str = ", ".join([f":{col}" for col in ABN_RET_COLUMNS.values()])
        ret_update_str = ", ".join(
            [f"{col} = EXCLUDED.{col}" for col in ABN_RET_COLUMNS.values()]
        )

        with ENGINE.begin() as conn:
            stmt = sa.text(
                f"""
                INSERT INTO {TABLE_NAME} (
                    event_id, stock_code, event_ts, market,
                    {ret_columns_str}
                ) VALUES (
                    :event_id, :stock_code, :event_ts, :market,
                    {ret_values_str}
                )
                ON CONFLICT (event_id) DO UPDATE SET
                    {ret_update_str}
                """
            )

            # executemany → 한 번에 다중 삽입
            result = conn.execute(stmt, updates)
            print(f"Monthly batch completed: {result.rowcount} rows inserted/updated")

    def calculate_abnormal_returns_for_events(
        events: list[dict], price_map: dict, market_price_map: dict
    ) -> list[dict]:
        """이벤트 리스트에 대해 abnormal return을 계산하는 헬퍼 함수 (딕셔너리 조회 최적화)"""
        updates = []
        lag_minutes = list(ABN_RET_COLUMNS.keys())

        for ev in events:
            ts0 = to_min(
                datetime.fromisoformat(ev["event_ts"]).replace(
                    tzinfo=zoneinfo.ZoneInfo(TZ)
                )
            )

            # 기본 가격 조회
            stock_code = ev["stock_code"]

            # 빈 종목코드 체크
            if not stock_code or stock_code.strip() == "":
                print(f"Skipping event with empty stock_code: {ev}")
                continue

            p0 = price_map.get((stock_code, ts0))
            market_p0 = market_price_map.get(ts0)

            print(f"ev: {ev}, ts0: {ts0}, p0: {p0}, market_p0: {market_p0}")

            if p0 is None or p0 == 0 or market_p0 is None or market_p0 == 0:
                continue

            # 한 번에 모든 필요한 시간대 계산
            all_timestamps = [ts0 + timedelta(minutes=lag) for lag in lag_minutes]

            # 한 번에 모든 lag 가격 조회 (딕셔너리 조회 최적화)
            p_lags = [price_map.get((stock_code, ts)) for ts in all_timestamps]
            market_p_lags = [market_price_map.get(ts) for ts in all_timestamps]

            rec = {
                "event_id": ev["event_id"],
                "stock_code": stock_code,
                "event_ts": ev["event_ts"],
                "market": ev["market"],
            }

            # 벡터화된 수익률 계산
            for i, lag in enumerate(lag_minutes):
                p_lag = p_lags[i]
                market_p_lag = market_p_lags[i]

                if p_lag and market_p_lag:
                    # 개별 수익률 계산
                    stock_return = (p_lag / p0 - 1.0) * 100.0
                    # market 수익률 계산
                    market_return = (market_p_lag / market_p0 - 1.0) * 100.0
                    # abnormal return = 개별 수익률 - market 수익률
                    abnormal_return = stock_return - market_return
                    rec[ABN_RET_COLUMNS[lag]] = round(abnormal_return, 2)

            # ★ 계산된 abnormal return이 하나라도 있을 때만 삽입
            if any(k in rec for k in ABN_RET_COLUMNS.values()):
                # ★ 누락된 칼럼을 None 으로 채워 넣음
                for col in ABN_RET_COLUMNS.values():
                    rec.setdefault(col, None)
                updates.append(rec)

        return updates

    # ───────────────────────── DAG 의 Task 의존성 ───────────────────
    create_table_task = create_table_if_not_exists()
    events_task = fetch_events(
        start_date=datetime(2023, 1, 1),
        end_date=datetime(2023, 12, 31),
    )
    returns_task = compute_returns_monthly.expand(events=events_task)

    create_table_task >> returns_task


dag = event_reaction_returns_kind_dag()
