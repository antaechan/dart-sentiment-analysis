"""
1) Awake 텔레그램 채널 > Telethon 스트림
2) 공시 원문 파싱
3) OpenAI ChatCompletion으로 2~3문장 요약
4) Postgres INSERT
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os, openai, asyncpg, asyncio
from telethon import TelegramClient, events  # :contentReference[oaicite:1]{index=1}

OPENAI_KEY = os.getenv("OPENAI_API_KEY")
openai.api_key = OPENAI_KEY


def pull_and_summarize(**context):
    async def _async_main():
        client = TelegramClient("airflow", api_id, api_hash)
        await client.start()
        async for msg in client.iter_messages("Awake", limit=20):
            text = msg.text
            # 1) 공시 본문·회사·이벤트 유형 추출 (정규식/LLM 둘 다 가능)
            corp, ev_name = extract_fields(text)
            # 2) 요약
            resp = openai.ChatCompletion.create(  # :contentReference[oaicite:2]{index=2}
                model="gpt-4o-mini",
                messages=[
                    {
                        "role": "user",
                        "content": f"다음 공시를 한국어로 2~3문장 핵심 요약:\n{text}",
                    }
                ],
                temperature=0.2,
            )
            summary = resp.choices[0].message.content.strip()
            # 3) 저장
            conn = await asyncpg.connect(
                "postgresql://airflow:airflow@postgres_events:5432/disclosures"
            )
            await conn.execute(
                """
                INSERT INTO disclosure_events
                (corp_code, event_name, disclosed_at, summary_kr)
                VALUES ($1,$2,$3,$4)
                ON CONFLICT DO NOTHING
            """,
                corp,
                ev_name,
                msg.date,
                summary,
            )
            await conn.close()
        await client.disconnect()

    asyncio.run(_async_main())


with DAG(
    dag_id="disclosure_ingest",
    start_date=datetime(2025, 6, 30),
    schedule_interval="*/5 * * * *",  # 5분마다
    catchup=False,
    tags=["telegram", "openai"],
) as dag:
    ingest = PythonOperator(
        task_id="pull_and_summarize", python_callable=pull_and_summarize
    )
