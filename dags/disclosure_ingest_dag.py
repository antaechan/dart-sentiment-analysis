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

load_dotenv()

API_ID     = os.getenv("API_ID")
API_HASH   = os.getenv("API_HASH")
CHANNEL    = os.getenv("CHANNEL_USERNAME")
OPENAI_KEY = os.getenv("OPENAI_API_KEY")
SESSION = os.getenv("TG_SESSION")

START = date(2023, 10, 1)
END   = date(2023, 12, 31)

# ──────────────────────
# 헬퍼 : 월 단위 범위 생성
# ──────────────────────
def month_ranges(start: date, end: date):
    d = start.replace(day=1)
    while d <= end:
        next_m = (d.replace(day=28) + timedelta(days=4)).replace(day=1)
        yield (d, min(next_m - timedelta(days=1), end))
        d = next_m

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

        with TelegramClient(StringSession(SESSION), API_ID, API_HASH) as tg:
            for msg in tg.iter_messages(CHANNEL,
                                        offset_date=d2,
                                        limit=2500,
                                        wait_time=0,
                                        reverse=True):  # 오래된→최근
                try:
                    if msg.date.date() < d1:
                        break
                    all_msgs.append(
                    {
                        "id": msg.id,
                        "corp": msg.message,
                        "evt":  msg.message,
                        "date": msg.date,
                        "raw":  msg.text
                    })
                    if len(all_msgs) % 100 == 0:
                        print(f"fetched {len(all_msgs)} messages")
                except FloodWaitError as e:
                    print(f"FloodWaitError: {e}")
                    time.sleep(e.seconds)

            return all_msgs  # XCom push (작은 리스트면 OK)

        # 3) 요약 + SQL 적재
    # @task
    # async def summarize_and_insert(records: list[dict]):
    #     client_ai = OpenAI(api_key=OPENAI_KEY)
    #     # 3-1. 요약
    #     texts = [r["raw"] for r in records]
    #     batched, summaries = [], []
    #     for i in range(0, len(texts), 50):
    #         part = texts[i:i+50]
    #         resp = client_ai.chat.completions.create(
    #             model="gpt-4o-mini",
    #             messages=[{"role":"user",
    #                        "content":"각 공시를 한국어로 2~3문장 요약:\n" +
    #                                  "\n\n---\n\n".join(part)}],
    #             temperature=0.2,
    #         )
    #         summaries.extend(resp.choices[0].message.content.split("\n\n"))
    #     for r, s in zip(records, summaries):
    #         r["summary"] = s.strip()

    #     # 3-2. Postgres 벌크 insert
    #     conn = await asyncpg.connect(
    #         dsn="postgresql://airflow:airflow@postgres_events:5432/disclosures"
    #     )
    #     await conn.copy_records_to_table(
    #         "disclosure_events",
    #         records=[
    #             (r["corp"], r["evt"], r["date"], r["summary"])
    #             for r in records
    #         ],
    #         columns=["corp_code", "event_name", "disclosed_at", "summary_kr"],
    #     )
    #     await conn.close()

    # DAG 의존성
    ranges = make_ranges()
    fetched = fetch_range.expand(period=ranges)
    # print(fetched[0])
    # summarize_and_insert.expand(records=fetched)

dag = disclosure_ingest_dag()
