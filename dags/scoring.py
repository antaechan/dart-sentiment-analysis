from __future__ import annotations

"""Airflow DAG: enrich_disclosure_events_with_sentiment
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Run KR‑FinBERT‑SC on new disclosure events and **write the sentiment back into
``disclosure_events``** (``label`` + ``score`` columns). If the columns are not
present, they are created on‑the‑fly with ``ALTER TABLE … IF NOT EXISTS``.

* Executes hourly at **HH:30** (Asia/Seoul).
* TaskFlow API via ``@task``.
* No logging — simple ``print`` statements for visibility.
"""

import os
from datetime import datetime

import pandas as pd
import pendulum
import sqlalchemy as sa
import torch
import torch.nn.functional as F
from airflow import DAG
from airflow.decorators import task
from transformers import AutoModelForSequenceClassification, AutoTokenizer

from database import create_database_engine

# ---------------------------------------------------------------------------
# Constants & configuration
# ---------------------------------------------------------------------------
MODEL_NAME = "snunlp/KR-FinBert-SC"
KST = pendulum.timezone("Asia/Seoul")


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="enrich_disclosure_events_with_sentiment",
    description="Infer sentiment of disclosure events and store results in-place (label & score).",
    start_date=datetime(2025, 8, 3, tzinfo=KST),
    schedule="@once",
    catchup=False,
    tags=["nlp", "finance", "sentiment"],
) as dag:

    @task
    def infer_and_update(
        start_date: datetime | None = None,
        end_date: datetime | None = None,
    ):
        """Infer sentiment for new events and update *disclosure_events*."""
        # ---------------------------------------------------------------------------
        # Global model loading (outside of DAG)
        # ---------------------------------------------------------------------------
        print(f"[sentiment_dag] Loading AI model and tokenizer: {MODEL_NAME}")
        tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME)
        model = AutoModelForSequenceClassification.from_pretrained(MODEL_NAME)
        device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        model.to(device).eval()
        print(f"[sentiment_dag] Model loaded on {device}")

        # -----------------------------------------------------------------
        # Build DB engine
        # -----------------------------------------------------------------
        engine = create_database_engine()

        # -----------------------------------------------------------------
        # Ensure cols exist (idempotent)
        # -----------------------------------------------------------------
        alter_stmt = sa.text(
            """
            ALTER TABLE disclosure_events
            ADD COLUMN IF NOT EXISTS label TEXT,
            ADD COLUMN IF NOT EXISTS score NUMERIC(5,4);
            """
        )
        with engine.begin() as conn:
            conn.execute(alter_stmt)

        # -----------------------------------------------------------------
        # Fetch events lacking sentiment
        # -----------------------------------------------------------------
        fetch_stmt = sa.text(
            """
            SELECT id, summary_kr
            FROM disclosure_events
            WHERE stock_code IS NOT NULL AND summary_kr IS NOT NULL AND label IS NULL
            AND disclosed_at AT TIME ZONE 'Asia/Seoul' >= :start_date
            AND disclosed_at AT TIME ZONE 'Asia/Seoul' <= :end_date
            AND (
                  (EXTRACT(HOUR FROM disclosed_at AT TIME ZONE 'Asia/Seoul') BETWEEN 9 AND 14)
                  OR
                  (EXTRACT(HOUR FROM disclosed_at AT TIME ZONE 'Asia/Seoul') = 15
                   AND EXTRACT(MINUTE FROM disclosed_at AT TIME ZONE 'Asia/Seoul') <= 30)
              )
            ORDER BY disclosed_at, id;
            """
        )
        with engine.begin() as conn:
            df = pd.read_sql(
                fetch_stmt,
                conn,
                params={"start_date": start_date, "end_date": end_date},
            )

        if df.empty:
            print("[sentiment_dag] No new disclosure events — nothing to do.")
            return

        # -----------------------------------------------------------------
        # Batch inference with memory management
        # -----------------------------------------------------------------
        id2label = model.config.id2label  # {0: 'negative', 1: 'neutral', 2: 'positive'}
        BATCH_SIZE = 32  # 메모리에 맞게 조정

        all_labels = []
        all_scores = []
        total_batches = (len(df) + BATCH_SIZE - 1) // BATCH_SIZE

        print(
            f"[sentiment_dag] Starting batch inference: {len(df)} sentences in {total_batches} batches (batch_size={BATCH_SIZE})"
        )

        for i in range(0, len(df), BATCH_SIZE):
            batch_sentences = df["summary_kr"].iloc[i : i + BATCH_SIZE].tolist()
            current_batch = (i // BATCH_SIZE) + 1

            print(
                f"[sentiment_dag] Processing batch {current_batch}/{total_batches} ({len(batch_sentences)} sentences)"
            )

            # 배치별로 토크나이징
            encodings = tokenizer(
                batch_sentences,
                padding=True,
                truncation=True,
                max_length=256,
                return_tensors="pt",
            )
            encodings = {k: v.to(device) for k, v in encodings.items()}

            # 배치별 추론
            with torch.no_grad():
                logits = model(**encodings).logits
                probs = F.softmax(logits, dim=-1).cpu()

            # 결과 저장
            batch_labels = [id2label[p.argmax().item()] for p in probs]
            batch_scores = [round(p.max().item(), 4) for p in probs]

            all_labels.extend(batch_labels)
            all_scores.extend(batch_scores)

            # 메모리 정리
            del encodings, logits, probs
            if torch.cuda.is_available():
                torch.cuda.empty_cache()

        print(
            f"[sentiment_dag] Batch inference completed: {len(all_labels)} sentences processed"
        )

        df["label"] = all_labels
        df["score"] = all_scores

        # -----------------------------------------------------------------
        # Bulk update disclosure_events
        # -----------------------------------------------------------------
        update_stmt = sa.text(
            """
            UPDATE disclosure_events
            SET label = :label, score = :score
            WHERE id = :event_id;
            """
        )
        records = df.to_dict("records")
        with engine.begin() as conn:
            conn.execute(
                update_stmt,
                [
                    {
                        "event_id": r["id"],
                        "label": r["label"],
                        "score": r["score"],
                    }
                    for r in records
                ],
            )

        print(f"[sentiment_dag] Updated {len(records)} disclosure_events rows.")

    # ---------------------------------------------------------------------------
    # Task execution
    # ---------------------------------------------------------------------------
    infer_and_update(start_date=datetime(2022, 7, 1), end_date=datetime(2022, 12, 31))
