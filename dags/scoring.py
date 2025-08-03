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
    def infer_and_update():
        """Infer sentiment for new events and update *disclosure_events*."""

        # -----------------------------------------------------------------
        # Build DB engine
        # -----------------------------------------------------------------
        POSTGRES_USER = os.getenv("POSTGRES_USER")
        POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
        POSTGRES_DB = os.getenv("POSTGRES_DB")
        POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres_events")
        POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")

        db_url = (
            f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@"
            f"{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
        )
        engine = sa.create_engine(db_url, pool_pre_ping=True, future=True)

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
            WHERE summary_kr IS NOT NULL AND label IS NULL;
            """
        )
        with engine.begin() as conn:
            df = pd.read_sql(fetch_stmt, conn)

        if df.empty:
            print("[sentiment_dag] No new disclosure events — nothing to do.")
            return

        # -----------------------------------------------------------------
        # Load tokenizer/model
        # -----------------------------------------------------------------
        tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME)
        model = AutoModelForSequenceClassification.from_pretrained(MODEL_NAME)
        device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        model.to(device).eval()

        # -----------------------------------------------------------------
        # Batch inference
        # -----------------------------------------------------------------
        sentences = df["summary_kr"].tolist()
        encodings = tokenizer(
            sentences, padding=True, truncation=True, return_tensors="pt"
        )
        encodings = {k: v.to(device) for k, v in encodings.items()}

        with torch.no_grad():
            logits = model(**encodings).logits
            probs = F.softmax(logits, dim=-1).cpu()

        id2label = model.config.id2label  # {0: 'negative', 1: 'neutral', 2: 'positive'}
        df["label"] = [id2label[p.argmax().item()] for p in probs]
        df["score"] = [round(p.max().item(), 4) for p in probs]

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

    # Bind task to DAG
    infer_and_update()
