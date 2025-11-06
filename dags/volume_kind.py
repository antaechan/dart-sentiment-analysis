# dags/volume_kind.py
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
VOLUME_COLUMNS = {
    -10: "volume_minus_10m",
    10: "volume_10m",
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


def build_volume_map_with_ffill(
    ohlcv: pl.DataFrame, date_int: int, required_codes: set[str] = None
) -> dict:
    """
    필요한 종목만 처리하여 메모리 사용량을 크게 줄인 volume_map 생성
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
    volume_map = {}
    unique_codes = ohlcv.select("code").unique().to_series()

    for code in unique_codes:
        # 종목별로 작은 단위로 처리
        code_data = ohlcv.filter(pl.col("code") == code)
        code_grid = pl.DataFrame({"code": [code] * len(ts_idx), "ts": ts_idx})

        df = (
            code_grid.join(code_data, on=["code", "ts"], how="left")
            .sort("ts")
            .with_columns(pl.col("volume").forward_fill().alias("volume"))
        )

        # 딕셔너리에 추가
        for row in df.iter_rows(named=True):
            if row["volume"] is not None:
                volume_map[(row["code"], row["ts"])] = row["volume"]

    return volume_map


@dag(
    start_date=datetime(2025, 7, 4),
    schedule="@once",  # 필요에 따라 cron 으로 변경
    catchup=False,
    tags=["ticks", "event_volume"],
    max_active_tasks=1,
)
def event_reaction_volume_kind_dag():
    @task
    def add_volume_columns_if_not_exists() -> None:
        """
        VOLUME_COLUMNS 필드들이 테이블에 없으면 추가합니다 (멱등성 보장).
        """
        with ENGINE.begin() as conn:
            for col_name in VOLUME_COLUMNS.values():
                alter_sql = f"""
                ALTER TABLE {TABLE_NAME}
                ADD COLUMN IF NOT EXISTS {col_name} DOUBLE PRECISION;
                """
                conn.execute(sa.text(alter_sql))
                print(f"Ensured column exists: {col_name}")

    # ────────────────────────────────────────────────────────────────
    def fetch_events_monthly(start_date: datetime, end_date: datetime) -> list[dict]:
        """
        지정된 월 범위의 abnormal_return_kind 테이블에서 이벤트를 가져옵니다.
        반환 형식: [{'event_id': …, 'ts': datetime, 'code': str, 'market': str}, …]
        """
        with ENGINE.connect() as conn:
            sql = """
            SELECT
                event_id,
                event_ts,
                stock_code,
                market
            FROM abnormal_return_kind
            WHERE stock_code IS NOT NULL
              AND stock_code != ''
              AND event_ts IS NOT NULL
              AND market IN ('KOSPI', 'KOSDAQ', 'KOSDAQ GLOBAL')
              -- 09:00~15:20 사이 공시만 (KST 기준)
              AND (
                  (EXTRACT(HOUR FROM event_ts) BETWEEN 9 AND 14)
                  OR
                  (EXTRACT(HOUR FROM event_ts) = 15
                   AND EXTRACT(MINUTE FROM event_ts) <= 20)
              )
              -- 월별 범위 필터링
              AND event_ts >= %(start_date)s
              AND event_ts <= %(end_date)s
            ORDER BY event_ts
            """
            df = pd.read_sql(
                sql, conn, params={"start_date": start_date, "end_date": end_date}
            )

        if df.empty:
            print(f"No events found for {start_date} to {end_date}")
            return []

        # 문자열은 strftime으로 변환 (timezone 정보 제거)
        df["date_int"] = df["event_ts"].dt.strftime("%Y%m%d").astype(int)
        df["event_ts"] = df["event_ts"].dt.strftime("%Y-%m-%dT%H:%M:%S")

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
    def compute_volumes_monthly(events: list[dict]) -> None:
        """
        한 달의 이벤트들에 대해 volume_* 컬럼을 volume_kind 테이블에 INSERT.
        """
        if not events:
            return

        # 날짜별 parquet 한 번씩만 읽도록 묶기
        events_by_date: dict[int, list[dict]] = {}
        for ev in events:
            events_by_date.setdefault(ev["date_int"], []).append(ev)

        updates = []  # [{event_id:…, volume_1m:…}, …] 누적

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
                        kospi_pq_path, columns=["ts", "종목코드", "volume"]
                    ).rename({"종목코드": "code"})

                    # 필요한 종목 코드만 추출하여 메모리 효율성 향상
                    required_codes = set(ev["stock_code"] for ev in kospi_events)
                    volume_map = build_volume_map_with_ffill(
                        ohlcv, date_int, required_codes
                    )

                    # KOSPI 이벤트들의 volume 계산
                    updates.extend(
                        calculate_volumes_for_events(kospi_events, volume_map)
                    )

            # KOSDAQ 이벤트 처리
            if kosdaq_events:
                kosdaq_pq_path = KOSDAQ_OUT_DIR / f"{date_int:08d}_1m.parquet"

                if kosdaq_pq_path.exists():
                    # 개별 종목 데이터 (KOSDAQ)
                    kosdaq_ohlcv = pl.read_parquet(
                        kosdaq_pq_path, columns=["ts", "종목코드", "volume"]
                    ).rename({"종목코드": "code"})

                    # 필요한 종목 코드만 추출하여 메모리 효율성 향상
                    required_codes = set(ev["stock_code"] for ev in kosdaq_events)
                    volume_map = build_volume_map_with_ffill(
                        kosdaq_ohlcv, date_int, required_codes
                    )

                    # KOSDAQ 이벤트들의 volume 계산
                    updates.extend(
                        calculate_volumes_for_events(kosdaq_events, volume_map)
                    )

        if not updates:
            return

        print(f"Processing {len(updates)} inserts for monthly batch")

        # ---------- bulk INSERT ----------
        volume_columns_str = ", ".join(VOLUME_COLUMNS.values())
        volume_values_str = ", ".join([f":{col}" for col in VOLUME_COLUMNS.values()])
        volume_update_str = ", ".join(
            [f"{col} = EXCLUDED.{col}" for col in VOLUME_COLUMNS.values()]
        )

        with ENGINE.begin() as conn:
            stmt = sa.text(
                f"""
                INSERT INTO {TABLE_NAME} (
                    event_id, stock_code, event_ts, market,
                    {volume_columns_str}
                ) VALUES (
                    :event_id, :stock_code, :event_ts, :market,
                    {volume_values_str}
                )
                ON CONFLICT (event_id) DO UPDATE SET
                    {volume_update_str}
                """
            )

            # executemany → 한 번에 다중 삽입
            result = conn.execute(stmt, updates)
            print(f"Monthly batch completed: {result.rowcount} rows inserted/updated")

    def calculate_volumes_for_events(
        events: list[dict], volume_map: dict
    ) -> list[dict]:
        """이벤트 리스트에 대해 volume을 계산하는 헬퍼 함수 (딕셔너리 조회 최적화)"""
        updates = []
        lag_minutes = list(VOLUME_COLUMNS.keys())

        for ev in events:
            ts0 = to_min(
                datetime.fromisoformat(ev["event_ts"]).replace(
                    tzinfo=zoneinfo.ZoneInfo(TZ)
                )
            )

            # 기본 volume 조회
            stock_code = ev["stock_code"]

            # 빈 종목코드 체크
            if not stock_code or stock_code.strip() == "":
                print(f"Skipping event with empty stock_code: {ev}")
                continue

            # 한 번에 모든 필요한 시간대 계산
            all_timestamps = [ts0 + timedelta(minutes=lag) for lag in lag_minutes]

            # 한 번에 모든 lag volume 조회 (딕셔너리 조회 최적화)
            volume_lags = [volume_map.get((stock_code, ts)) for ts in all_timestamps]

            rec = {
                "event_id": ev["event_id"],
                "stock_code": stock_code,
                "event_ts": ev["event_ts"],
                "market": ev["market"],
            }

            # 벡터화된 volume 계산
            for i, lag in enumerate(lag_minutes):
                volume_lag = volume_lags[i]

                if volume_lag is not None:
                    rec[VOLUME_COLUMNS[lag]] = volume_lag

            # ★ 계산된 volume이 하나라도 있을 때만 삽입
            if any(k in rec for k in VOLUME_COLUMNS.values()):
                # ★ 누락된 칼럼을 None 으로 채워 넣음
                for col in VOLUME_COLUMNS.values():
                    rec.setdefault(col, None)
                updates.append(rec)

        return updates

    # ───────────────────────── DAG 의 Task 의존성 ───────────────────
    add_columns_task = add_volume_columns_if_not_exists()
    events_task = fetch_events(
        start_date=datetime(2023, 1, 1),
        end_date=datetime(2023, 12, 31),
    )
    volumes_task = compute_volumes_monthly.expand(events=events_task)

    add_columns_task >> volumes_task


dag = event_reaction_volume_kind_dag()
