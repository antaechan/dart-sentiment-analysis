from __future__ import annotations
from airflow.decorators import dag, task
from datetime import datetime, timedelta, date
from itertools import pairwise
import os, time, asyncpg, pandas as pd
from telethon.sync import TelegramClient
from openai import OpenAI
from dotenv import load_dotenv

load_dotenv()
API_ID     = int(os.getenv("API_ID"))
API_HASH   = os.getenv("API_HASH")
CHANNEL    = os.getenv("CHANNEL_USERNAME", "Awake")
OPENAI_KEY = os.getenv("OPENAI_API_KEY")

client_ai = OpenAI(api_key=OPENAI_KEY)

START = date(2020, 1, 1)
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
    dag_id="telegram_backfill_2020_2023",
    start_date=datetime(2025, 6, 30),
    schedule="@once",         # 手動 trigger or once
    catchup=False,
    max_active_tasks=4,       # 병렬度 제한
    tags=["telegram","backfill"],
)
def backfill_dag():
    # 1) 기간 리스트 산출
    @task
    def make_ranges():
        return list(month_ranges(START, END))  # [(2020-01-01,2020-01-31), …]

    # 2) 메시지 수집 (동기 Telethon 그대로 사용)
    @task
    def fetch_range(period: tuple) -> list[dict]:
        d1, d2 = period
        all_msgs = []
        with TelegramClient("backfill_session", API_ID, API_HASH) as tg:
            for msg in tg.iter_messages(CHANNEL,
                                        offset_date=d2,
                                        reverse=True):  # 오래된→최근
                if msg.date.date() < d1:
                    break
                if msg.message:
                    all_msgs.append(
                        {"id": msg.id, "corp": parse_corp(msg.message),
                         "evt":  parse_event(msg.message),
                         "date": msg.date,
                         "raw":  msg.message}
                    )
                if len(all_msgs) % 100 == 0:
                    time.sleep(0.5)
        return all_msgs  # XCom push (작은 리스트면 OK)

    # 3) 요약 + SQL 적재
    @task
    async def summarize_and_insert(records: list[dict]):
        # 3-1. 요약
        texts = [r["raw"] for r in records]
        batched, summaries = [], []
        for i in range(0, len(texts), 50):
            part = texts[i:i+50]
            resp = client_ai.chat.completions.create(
                model="gpt-4o-mini",
                messages=[{"role":"user",
                           "content":"각 공시를 한국어로 2~3문장 요약:\n" +
                                     "\n\n---\n\n".join(part)}],
                temperature=0.2,
            )
            summaries.extend(resp.choices[0].message.content.split("\n\n"))
        for r, s in zip(records, summaries):
            r["summary"] = s.strip()

        # 3-2. Postgres 벌크 insert
        conn = await asyncpg.connect(
            dsn="postgresql://airflow:airflow@postgres_events:5432/disclosures"
        )
        await conn.copy_records_to_table(
            "disclosure_events",
            records=[
                (r["corp"], r["evt"], r["date"], r["summary"])
                for r in records
            ],
            columns=["corp_code", "event_name", "disclosed_at", "summary_kr"],
        )
        await conn.close()

    # DAG 의존성
    ranges = make_ranges()
    fetch_range.expand(period=ranges) >> summarize_and_insert.expand()

backfill_workflow = backfill_dag()
