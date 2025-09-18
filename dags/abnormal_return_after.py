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
TABLE_NAME = "abnormal_return_after"
ABN_RET_COLUMNS = {
    1: "round1",  # 첫 번째 10분 단위 체결 (공시 발표 후 첫 번째 체결)
    2: "round2",  # 두 번째 10분 단위 체결
    3: "round3",  # 세 번째 10분 단위 체결
    4: "round4",  # 네 번째 10분 단위 체결
    5: "round5",  # 다섯 번째 10분 단위 체결
}

# 시간 범위 설정
DISCLOSURE_START_HOUR = 16
DISCLOSURE_END_HOUR = 18
DISCLOSURE_END_MINUTE = 0

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


def get_next_round_times(event_time: datetime) -> dict[int, datetime]:
    """
    공시 발표 시간 이후 시간외 단일가 매매 체결 시간들을 계산합니다.

    시간외 단일가 매매는 4시~6시 사이에서 10분 단위로 체결됩니다:
    - 4:10, 4:20, 4:30, 4:40, 4:50
    - 5:00, 5:10, 5:20, 5:30, 5:40, 5:50

    Args:
        event_time: 공시 발표 시간

    Returns:
        {round_number: next_round_time} 딕셔너리
    """
    tz = zoneinfo.ZoneInfo(TZ)

    # 시간외 단일가 매매 체결 시간들 (4시~6시, 10분 단위)
    round_times = [
        datetime.strptime(
            f"{event_time.year:04d}{event_time.month:02d}{event_time.day:02d} 16:10",
            "%Y%m%d %H:%M",
        ).replace(tzinfo=tz),
        datetime.strptime(
            f"{event_time.year:04d}{event_time.month:02d}{event_time.day:02d} 16:20",
            "%Y%m%d %H:%M",
        ).replace(tzinfo=tz),
        datetime.strptime(
            f"{event_time.year:04d}{event_time.month:02d}{event_time.day:02d} 16:30",
            "%Y%m%d %H:%M",
        ).replace(tzinfo=tz),
        datetime.strptime(
            f"{event_time.year:04d}{event_time.month:02d}{event_time.day:02d} 16:40",
            "%Y%m%d %H:%M",
        ).replace(tzinfo=tz),
        datetime.strptime(
            f"{event_time.year:04d}{event_time.month:02d}{event_time.day:02d} 16:50",
            "%Y%m%d %H:%M",
        ).replace(tzinfo=tz),
        datetime.strptime(
            f"{event_time.year:04d}{event_time.month:02d}{event_time.day:02d} 17:00",
            "%Y%m%d %H:%M",
        ).replace(tzinfo=tz),
        datetime.strptime(
            f"{event_time.year:04d}{event_time.month:02d}{event_time.day:02d} 17:10",
            "%Y%m%d %H:%M",
        ).replace(tzinfo=tz),
        datetime.strptime(
            f"{event_time.year:04d}{event_time.month:02d}{event_time.day:02d} 17:20",
            "%Y%m%d %H:%M",
        ).replace(tzinfo=tz),
        datetime.strptime(
            f"{event_time.year:04d}{event_time.month:02d}{event_time.day:02d} 17:30",
            "%Y%m%d %H:%M",
        ).replace(tzinfo=tz),
        datetime.strptime(
            f"{event_time.year:04d}{event_time.month:02d}{event_time.day:02d} 17:40",
            "%Y%m%d %H:%M",
        ).replace(tzinfo=tz),
        datetime.strptime(
            f"{event_time.year:04d}{event_time.month:02d}{event_time.day:02d} 17:50",
            "%Y%m%d %H:%M",
        ).replace(tzinfo=tz),
    ]

    # 공시 발표 시간 이후의 체결 시간들만 필터링
    next_rounds = {}
    round_number = 1

    for round_time in round_times:
        if round_time > event_time and round_number <= 5:  # 최대 5라운드까지만
            next_rounds[round_number] = round_time
            round_number += 1

    return next_rounds


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
    시간외 단일가 매매 시간대(4시~6시)에서는 10분 단위로 체결되므로
    해당 시간대에서는 10분 단위 타임스탬프를 생성하고 forward fill한다.
    """
    tz = zoneinfo.ZoneInfo(TZ)

    # 1) 날짜 범위 (지정된 시간 범위) 타임스탬프 인덱스 생성
    d = datetime.strptime(str(date_int), "%Y%m%d")
    start = d.replace(
        hour=DISCLOSURE_START_HOUR, minute=0, second=0, microsecond=0, tzinfo=tz
    )
    end = d.replace(
        hour=DISCLOSURE_END_HOUR,
        minute=DISCLOSURE_END_MINUTE,
        second=0,
        microsecond=0,
        tzinfo=tz,
    )

    # 시간외 단일가 매매 시간대(4시~6시)에서는 10분 단위로만 체결
    ts_idx = pl.datetime_range(
        start, end, interval="10m", time_unit="us", time_zone=TZ, eager=True
    )
    idx_df = pl.DataFrame({"ts": ts_idx})

    # 2) ts를 TZ-aware로 맞추기 (parquet이 naive면 timezone 부여)
    ohlcv = ohlcv.with_columns(pl.col("ts").dt.replace_time_zone(TZ))

    # 3) market 지수 데이터만 필터링
    market_data = ohlcv.filter(pl.col("code") == market_code)

    if market_data.is_empty():
        return {}

    # 4) 모든 10분 단위 ts와 left join 후 forward fill
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


def build_price_map_with_ffill(ohlcv: pl.DataFrame, date_int: int) -> dict:
    """
    해당 날짜의 시간외 단일가 매매 시간대(4시~6시)에서는 10분 단위로만 체결되므로
    해당 시간대에서는 10분 단위 타임스탬프를 생성하고 종목별로 close를 forward fill한다.
    """
    tz = zoneinfo.ZoneInfo(TZ)

    # 1) 날짜 범위 (지정된 시간 범위) 타임스탬프 인덱스 생성
    d = datetime.strptime(str(date_int), "%Y%m%d")
    start = d.replace(
        hour=DISCLOSURE_START_HOUR, minute=0, second=0, microsecond=0, tzinfo=tz
    )
    end = d.replace(
        hour=DISCLOSURE_END_HOUR,
        minute=DISCLOSURE_END_MINUTE,
        second=0,
        microsecond=0,
        tzinfo=tz,
    )

    # 시간외 단일가 매매 시간대(4시~6시)에서는 10분 단위로만 체결
    ts_idx = pl.datetime_range(
        start, end, interval="10m", time_unit="us", time_zone=TZ, eager=True
    )
    idx_df = pl.DataFrame({"ts": ts_idx})

    # 2) ts를 TZ-aware로 맞추기 (parquet이 naive면 timezone 부여)
    ohlcv = ohlcv.with_columns(pl.col("ts").dt.replace_time_zone(TZ))

    # 3) 종목코드 × 모든 10분 단위 ts 생성(크로스 조인) 후 원본과 left join
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
def event_reaction_returns_after_dag():
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
            stock_code VARCHAR(10) NOT NULL,
            event_ts TIMESTAMP WITH TIME ZONE NOT NULL,
            market VARCHAR(20) NOT NULL,
            {ret_columns_sql},
            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            FOREIGN KEY (event_id) REFERENCES disclosure_events(id)
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
        지정된 월 범위의 disclosure_events 중,
        * 발표 시각이 지정된 시간 범위 (KST) 사이에 발생했고
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
              -- 지정된 시간 범위 사이 공시만 (KST 기준)
              AND (
                  (EXTRACT(HOUR FROM disclosed_at AT TIME ZONE 'Asia/Seoul') BETWEEN %(start_hour)s AND %(end_hour)s)
              )
              -- 월별 범위 필터링
              AND disclosed_at >= %(start_date)s
              AND disclosed_at <= %(end_date)s
            """
            df = pd.read_sql(
                sql,
                conn,
                params={
                    "start_date": start_date,
                    "end_date": end_date,
                    "start_hour": DISCLOSURE_START_HOUR,
                    "end_hour": DISCLOSURE_END_HOUR - 1,  # BETWEEN은 양끝 포함이므로 -1
                },
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

                    price_map = build_price_map_with_ffill(ohlcv, date_int)
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

                    price_map = build_price_map_with_ffill(kosdaq_ohlcv, date_int)
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
        """이벤트 리스트에 대해 abnormal return을 계산하는 헬퍼 함수"""
        updates = []

        for ev in events:
            event_time = datetime.fromisoformat(ev["event_ts"]).replace(
                tzinfo=zoneinfo.ZoneInfo(TZ)
            )

            # 공시 발표 시간 이후의 체결 시간들 계산
            next_round_times = get_next_round_times(event_time)

            if not next_round_times:
                print(
                    f"No valid round times found for event {ev['event_id']} at {event_time}"
                )
                continue

            # 공시 발표 시간의 가격 (기준점)
            ts0 = to_min(event_time)
            p0 = price_map.get((ev["stock_code"], ts0))
            market_p0 = market_price_map.get(ts0)

            print(f"ev: {ev}, ts0: {ts0}, p0: {p0}, market_p0: {market_p0}")
            print(f"Next round times: {next_round_times}")

            if p0 is None or p0 == 0 or market_p0 is None or market_p0 == 0:
                continue

            rec = {
                "event_id": ev["event_id"],
                "stock_code": ev["stock_code"],
                "event_ts": ev["event_ts"],
                "market": ev["market"],
            }

            # 각 라운드별로 abnormal return 계산
            for round_num, round_time in next_round_times.items():
                round_ts = to_min(round_time)
                p_round = price_map.get((ev["stock_code"], round_ts))
                market_p_round = market_price_map.get(round_ts)

                if p_round and market_p_round:
                    # 개별 수익률 계산
                    stock_return = (p_round / p0 - 1.0) * 100.0
                    # market 수익률 계산
                    market_return = (market_p_round / market_p0 - 1.0) * 100.0
                    # abnormal return = 개별 수익률 - market 수익률
                    abnormal_return = stock_return - market_return
                    rec[ABN_RET_COLUMNS[round_num]] = round(abnormal_return, 2)
                    print(
                        f"Round {round_num} ({round_time}): stock_return={stock_return:.2f}%, market_return={market_return:.2f}%, abnormal_return={abnormal_return:.2f}%"
                    )

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


dag = event_reaction_returns_after_dag()
