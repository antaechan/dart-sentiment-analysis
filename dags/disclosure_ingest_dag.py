from __future__ import annotations
from airflow.decorators import dag, task
from datetime import datetime, timedelta, date
import os
import time
import asyncpg
import  pandas as pd
from telethon.sync import TelegramClient
from openai import OpenAI
from dotenv import load_dotenv
from telethon.sessions import StringSession
from telethon.errors import FloodWaitError
import re
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert
from zoneinfo import ZoneInfo

KST = ZoneInfo("Asia/Seoul")
load_dotenv()

API_ID     = os.getenv("API_ID")
API_HASH   = os.getenv("API_HASH")
CHANNEL    = os.getenv("CHANNEL_USERNAME")
OPENAI_KEY = os.getenv("OPENAI_API_KEY")
SESSION = os.getenv("TG_SESSION")

# START = date(2022, 5, 1)
START = date(2023, 11, 1)
END   = date(2023, 12, 31)

# ────── 정규식 헬퍼 ──────
_rx_company = re.compile(
    r"(?:기업명|회사명)\s*[:：]\s*([^\(\n]+)"  # ‘기업명:’ 또는 ‘회사명 :’ 모두 허용
)
_rx_report = re.compile(
    r"보고서명\s*[:：]\s*(.+)"                # ‘보고서명 :’, 전각 콜론(：)도 허용
)

def _extract_company(text: str) -> str:
    for line in text.split("\n"):
        m = _rx_company.search(line)   # match → search 로 변경
        if m:
            return m.group(1).strip()
    return ""

def _extract_report(text: str) -> str:
    m = _rx_report.search(text)        # 한 번에 검색
    return m.group(1).strip() if m else ""

# ──────────────────────
# 헬퍼 : 월 단위 범위 생성
# ──────────────────────
def month_ranges(start: date, end: date):
    d = start.replace(day=1)
    while d <= end:
        next_m = (d.replace(day=28) + timedelta(days=4)).replace(day=1)
        yield (d, min(next_m - timedelta(days=1), end))
        d = next_m

def upsert_do_nothing(table, conn, keys, data_iter):           # ➋
        rows = [dict(zip(keys, row)) for row in data_iter]
        if not rows:
            return
        stmt = (insert(table.table)                                  # ➌
                .values(rows)
                .on_conflict_do_nothing(index_elements=['id']))      # ➍
        conn.execute(stmt)

@dag(
    start_date=datetime(2025, 7, 1),
    schedule="@once",         # 手動 trigger or once
    catchup=False,
    max_active_tasks=1,
    tags=["telegram","backfill"],
)
def disclosure_ingest_dag():
    # 1) 기간 리스트 산출
    @task
    def make_ranges():
        return list(month_ranges(START, END))  # [(2020-01-01,2020-01-31), …]

    # 2) 메시지 수집 (동기 Telethon 그대로 사용)
    @task
    def fetch_range(period: tuple) -> list[dict]:
        d1, d2 = period
        all_msgs = []
        # d2 마지막 날의 다음 날 00:00(KST) → UTC 로 변환
        offset_d2_kst = datetime.combine(d2 + timedelta(days=1), datetime.min.time(), tzinfo=KST)
        offset_d2_utc = offset_d2_kst.astimezone(ZoneInfo("UTC"))

        with TelegramClient(StringSession(SESSION), API_ID, API_HASH) as tg:
            for msg in tg.iter_messages(CHANNEL,
                                        offset_date=offset_d2_utc,
                                        limit=2500,
                                        wait_time=0,
                                        reverse=False):
                try:
                    # ↙️ ① KST 로 변환 후 date 비교
                    disclosed_at_KST = msg.date.astimezone(KST)

                    if disclosed_at_KST.date() < d1:
                        break
                    text = msg.message or ""
                    # ① “주요 공시 정리” 문서 제외
                    if "주요 공시 정리" in text:
                        continue

                    company_name = _extract_company(text)
                    report_name  = _extract_report(text)

                    all_msgs.append({
                        "id":msg.id,
                        "company_name": company_name,
                        "report_name": report_name,
                        "disclosed_at": disclosed_at_KST,
                        "summary_kr": None,
                        "raw": text
                    })
                    if len(all_msgs) % 100 == 0:
                        print(f"fetched {len(all_msgs)} messages")
                except FloodWaitError as e:
                    print(f"FloodWaitError: {e}")
                    time.sleep(e.seconds)

            return all_msgs  # XCom push (작은 리스트면 OK)

    

    # 3) 요약 + SQL 적재
    @task
    def summarize_and_insert(records: list[dict]):
        if not records:
            return 0

        POSTGRES_USER = os.getenv("POSTGRES_USER")
        POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
        POSTGRES_DB = os.getenv("POSTGRES_DB")
        POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
        
        db_url = f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@postgres_events:{POSTGRES_PORT}/{POSTGRES_DB}"   
        df = pd.DataFrame.from_records(records)

        engine = sa.create_engine(db_url, pool_pre_ping=True, future=True)

        # ensure table exists
        create_sql = sa.text(
            """
            CREATE TABLE IF NOT EXISTS disclosure_events (
                id           BIGINT PRIMARY KEY,
                company_name  TEXT,
                report_name   TEXT,
                disclosed_at TIMESTAMPTZ,
                summary_kr   TEXT,   
                raw          TEXT,
                created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        with engine.begin() as conn:
            conn.execute(sa.text("SET TIME ZONE 'Asia/Seoul'"))
            conn.execute(create_sql)

        dtype_map = {
            "id": sa.BigInteger(),
            "company_name": sa.Text(),
            "report_name": sa.Text(),
            "disclosed_at": sa.TIMESTAMP(timezone=True),
            "summary_kr": sa.Text(),
            "raw": sa.Text(),
        }

        # pandas -> SQL (COPY-like multi insert)
        with engine.begin() as conn:
            conn.execute(sa.text("SET TIME ZONE 'Asia/Seoul'"))
            df.to_sql(
                "disclosure_events",
                con=conn,
                if_exists="append",
                index=False,
                method=upsert_do_nothing,
                chunksize=3000,
                dtype=dtype_map,
            )
        print(f"Inserted {len(records)} rows into disclosure_events")
        return len(records)

    # DAG 의존성
    ranges = make_ranges()
    fetched = fetch_range.expand(period=ranges)
    summarize_and_insert.expand(records=fetched)

dag = disclosure_ingest_dag()
