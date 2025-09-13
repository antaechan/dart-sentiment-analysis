# dags/abnormal_return_sort.py
from __future__ import annotations

import os
from datetime import datetime

import pandas as pd
import sqlalchemy as sa
from airflow.decorators import dag, task
from dotenv import load_dotenv

load_dotenv()

# 테이블 설정
TABLE_NAME = "abnormal_return"

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
    tags=["ticks", "event_sort"],
    max_active_tasks=1,
)
def abnormal_return_sort_dag():
    @task
    def create_table_if_not_exists() -> None:
        """
        abnormal_return 테이블이 없으면 생성합니다.
        """
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            event_id INTEGER NOT NULL UNIQUE,
            stock_code VARCHAR(15) NOT NULL,
            event_ts TIMESTAMP WITH TIME ZONE NOT NULL,
            market VARCHAR(20) NOT NULL,
            abn_ret_10m DOUBLE PRECISION,
            abn_ret_20m DOUBLE PRECISION,
            abn_ret_30m DOUBLE PRECISION,
            abn_ret_40m DOUBLE PRECISION,
            abn_ret_50m DOUBLE PRECISION,
            abn_ret_60m DOUBLE PRECISION,
            FOREIGN KEY (event_id) REFERENCES disclosure_events(id)
        );
        
        -- 인덱스 생성
        CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_event_id ON {TABLE_NAME}(event_id);
        CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_stock_code ON {TABLE_NAME}(stock_code);
        CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_event_ts ON {TABLE_NAME}(event_ts);
        """
        with ENGINE.begin() as conn:
            conn.execute(sa.text(create_sql))

    @task
    def fetch_all_events(
        start_date: datetime | None = None,
        end_date: datetime | None = None,
    ) -> list[dict]:
        """
        전체 기간의 disclosure_events를 가져옵니다.
        반환 형식: [{'event_id': …, 'event_ts': str, 'stock_code': str, 'market': str}, …]
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
              -- 기간 필터링
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

        df["ts_kst"] = df["ts_kst"].dt.tz_localize("Asia/Seoul")

        # 문자열은 isoformat()으로( +09:00 형태 보장 )
        df["event_ts"] = df["ts_kst"].apply(lambda x: x.isoformat())

        events = df[["event_id", "stock_code", "event_ts", "market"]].to_dict(
            orient="records"
        )

        print(f"Found {len(events)} events total")
        return events

    @task
    def insert_all_events(events: list[dict]) -> None:
        """
        모든 이벤트들을 event_ts 기준으로 정렬하여 abnormal_return 테이블에 INSERT.
        """
        if not events:
            return

        # event_ts 기준으로 정렬
        events_sorted = sorted(events, key=lambda x: x["event_ts"])

        print(f"Processing {len(events_sorted)} events total")

        # ---------- bulk INSERT ----------
        with ENGINE.begin() as conn:
            stmt = sa.text(
                f"""
                INSERT INTO {TABLE_NAME} (
                    event_id, stock_code, event_ts, market
                ) VALUES (
                    :event_id, :stock_code, :event_ts, :market
                )
                ON CONFLICT (event_id) DO NOTHING
                """
            )

            # 각 이벤트에 대해 기본 정보만 삽입 (abnormal return 컬럼들은 건드리지 않음)
            insert_data = []
            for event in events_sorted:
                insert_data.append(
                    {
                        "event_id": event["event_id"],
                        "stock_code": event["stock_code"],
                        "event_ts": event["event_ts"],
                        "market": event["market"],
                    }
                )

            # executemany → 한 번에 다중 삽입
            result = conn.execute(stmt, insert_data)
            print(f"Batch completed: {result.rowcount} rows inserted/updated")

    # ───────────────────────── DAG 의 Task 의존성 ───────────────────
    create_table_task = create_table_if_not_exists()
    events_task = fetch_all_events(
        start_date=datetime(2022, 7, 1),
        end_date=datetime(2023, 6, 30),
    )
    insert_task = insert_all_events(events_task)

    create_table_task >> insert_task


dag = abnormal_return_sort_dag()
