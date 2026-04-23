from __future__ import annotations

import json
import os
from decimal import Decimal
from typing import Any

import pandas as pd
from sqlalchemy import create_engine, text

from .settings import PROCESSED_CSV_PATH, WAREHOUSE_DSN

GENERIC_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS scraped_records (
    id BIGSERIAL PRIMARY KEY,
    dataset_name TEXT NOT NULL,
    compartment TEXT NOT NULL DEFAULT 'default',
    source_url TEXT NOT NULL,
    source_domain TEXT NOT NULL,
    schema_path TEXT,
    payload JSONB NOT NULL,
    ai_cluster_id INTEGER,
    ai_cluster_label TEXT,
    ai_price_band TEXT,
    scraped_at TIMESTAMPTZ NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_scraped_records_dataset_name ON scraped_records (dataset_name);
CREATE INDEX IF NOT EXISTS idx_scraped_records_compartment ON scraped_records (compartment);
CREATE INDEX IF NOT EXISTS idx_scraped_records_source_domain ON scraped_records (source_domain);
CREATE INDEX IF NOT EXISTS idx_scraped_records_scraped_at ON scraped_records (scraped_at);
CREATE UNIQUE INDEX IF NOT EXISTS uq_scraped_records_dataset_compartment_url
ON scraped_records (dataset_name, compartment, source_url);
"""

BOOKS_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS scraped_books (
    id BIGSERIAL PRIMARY KEY,
    title TEXT NOT NULL,
    price NUMERIC(10, 2) NOT NULL,
    rating SMALLINT NOT NULL,
    stock_count INTEGER NOT NULL,
    availability_text TEXT,
    source_url TEXT NOT NULL,
    source_domain TEXT NOT NULL,
    compartment TEXT NOT NULL DEFAULT 'default',
    ai_cluster_id INTEGER,
    ai_cluster_label TEXT,
    ai_price_band TEXT,
    scraped_at TIMESTAMPTZ NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

ALTER TABLE scraped_books DROP CONSTRAINT IF EXISTS scraped_books_source_url_key;
ALTER TABLE scraped_books ADD COLUMN IF NOT EXISTS source_domain TEXT;
ALTER TABLE scraped_books ADD COLUMN IF NOT EXISTS compartment TEXT NOT NULL DEFAULT 'default';
ALTER TABLE scraped_books ADD COLUMN IF NOT EXISTS ai_cluster_id INTEGER;
ALTER TABLE scraped_books ADD COLUMN IF NOT EXISTS ai_cluster_label TEXT;
ALTER TABLE scraped_books ADD COLUMN IF NOT EXISTS ai_price_band TEXT;
CREATE INDEX IF NOT EXISTS idx_scraped_books_rating ON scraped_books (rating);
CREATE INDEX IF NOT EXISTS idx_scraped_books_scraped_at ON scraped_books (scraped_at);
CREATE INDEX IF NOT EXISTS idx_scraped_books_compartment ON scraped_books (compartment);
CREATE INDEX IF NOT EXISTS idx_scraped_books_source_domain ON scraped_books (source_domain);
CREATE INDEX IF NOT EXISTS idx_scraped_books_ai_cluster_id ON scraped_books (ai_cluster_id);
CREATE INDEX IF NOT EXISTS idx_scraped_books_ai_price_band ON scraped_books (ai_price_band);
CREATE UNIQUE INDEX IF NOT EXISTS uq_scraped_books_source_url_compartment
ON scraped_books (source_url, compartment);
"""

GENERIC_UPSERT_SQL = """
INSERT INTO scraped_records (
    dataset_name,
    compartment,
    source_url,
    source_domain,
    schema_path,
    payload,
    ai_cluster_id,
    ai_cluster_label,
    ai_price_band,
    scraped_at
) VALUES (
    :dataset_name,
    :compartment,
    :source_url,
    :source_domain,
    :schema_path,
    CAST(:payload AS JSONB),
    :ai_cluster_id,
    :ai_cluster_label,
    :ai_price_band,
    :scraped_at
)
ON CONFLICT (dataset_name, compartment, source_url)
DO UPDATE SET
    source_domain = EXCLUDED.source_domain,
    schema_path = EXCLUDED.schema_path,
    payload = EXCLUDED.payload,
    ai_cluster_id = EXCLUDED.ai_cluster_id,
    ai_cluster_label = EXCLUDED.ai_cluster_label,
    ai_price_band = EXCLUDED.ai_price_band,
    scraped_at = EXCLUDED.scraped_at,
    loaded_at = NOW();
"""

BOOKS_UPSERT_SQL = """
INSERT INTO scraped_books (
    title,
    price,
    rating,
    stock_count,
    availability_text,
    source_url,
    source_domain,
    compartment,
    ai_cluster_id,
    ai_cluster_label,
    ai_price_band,
    scraped_at
) VALUES (
    :title,
    :price,
    :rating,
    :stock_count,
    :availability_text,
    :source_url,
    :source_domain,
    :compartment,
    :ai_cluster_id,
    :ai_cluster_label,
    :ai_price_band,
    :scraped_at
)
ON CONFLICT (source_url, compartment)
DO UPDATE SET
    title = EXCLUDED.title,
    price = EXCLUDED.price,
    rating = EXCLUDED.rating,
    stock_count = EXCLUDED.stock_count,
    availability_text = EXCLUDED.availability_text,
    source_domain = EXCLUDED.source_domain,
    ai_cluster_id = EXCLUDED.ai_cluster_id,
    ai_cluster_label = EXCLUDED.ai_cluster_label,
    ai_price_band = EXCLUDED.ai_price_band,
    scraped_at = EXCLUDED.scraped_at,
    loaded_at = NOW();
"""


METADATA_FIELDS = {
    "dataset_name",
    "compartment",
    "source_url",
    "source_domain",
    "schema_path",
    "scraped_at",
    "ai_cluster_id",
    "ai_cluster_label",
    "ai_price_band",
}


def _json_safe(value: Any) -> Any:
    if pd.isna(value):
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    return value


def _dataset_view_name(dataset_name: str) -> str:
    normalized = "".join(ch if ch.isalnum() else "_" for ch in dataset_name.lower()).strip("_")
    if not normalized:
        normalized = "default_dataset"
    return f"vw_dataset_{normalized}"


def _record_payload(record: dict[str, Any]) -> dict[str, Any]:
    payload = {}
    for key, value in record.items():
        if key in METADATA_FIELDS:
            continue
        payload[key] = _json_safe(value)
    return payload


def _can_project_books(record: dict[str, Any]) -> bool:
    required = {"title", "price", "rating", "source_url", "source_domain", "scraped_at"}
    return required.issubset(record.keys())


def load_into_postgres(csv_path: str = PROCESSED_CSV_PATH, dsn: str = WAREHOUSE_DSN) -> dict:
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"Processed CSV not found at {csv_path}")

    df = pd.read_csv(csv_path, parse_dates=["scraped_at"])
    if df.empty:
        return {"loaded_rows": 0, "warehouse_rows": 0, "books_rows": 0, "books_projected_rows": 0}

    if "dataset_name" not in df.columns:
        df["dataset_name"] = "default_dataset"
    if "compartment" not in df.columns:
        df["compartment"] = "default"

    engine = create_engine(dsn)
    books_loaded = 0

    with engine.begin() as conn:
        conn.execute(text(GENERIC_SCHEMA_SQL))
        conn.execute(text(BOOKS_SCHEMA_SQL))

        for record in df.to_dict(orient="records"):
            base = {
                "dataset_name": str(record.get("dataset_name") or "default_dataset"),
                "compartment": str(record.get("compartment") or "default"),
                "source_url": record.get("source_url"),
                "source_domain": record.get("source_domain"),
                "schema_path": record.get("schema_path"),
                "payload": json.dumps(_record_payload(record), default=_json_safe),
                "ai_cluster_id": record.get("ai_cluster_id"),
                "ai_cluster_label": record.get("ai_cluster_label"),
                "ai_price_band": record.get("ai_price_band"),
                "scraped_at": record.get("scraped_at"),
            }
            conn.execute(text(GENERIC_UPSERT_SQL), base)

            if _can_project_books(record):
                book_row = {
                    "title": record.get("title"),
                    "price": record.get("price"),
                    "rating": int(record.get("rating")) if pd.notna(record.get("rating")) else 0,
                    "stock_count": int(record.get("stock_count") or 0),
                    "availability_text": record.get("availability_text"),
                    "source_url": record.get("source_url"),
                    "source_domain": record.get("source_domain"),
                    "compartment": record.get("compartment"),
                    "ai_cluster_id": record.get("ai_cluster_id"),
                    "ai_cluster_label": record.get("ai_cluster_label"),
                    "ai_price_band": record.get("ai_price_band"),
                    "scraped_at": record.get("scraped_at"),
                }
                conn.execute(text(BOOKS_UPSERT_SQL), book_row)
                books_loaded += 1

        dataset_names = [name for name in df["dataset_name"].dropna().astype(str).unique().tolist() if name]
        for dataset_name in dataset_names:
            view_name = _dataset_view_name(dataset_name)
            conn.execute(
                text(
                    f"""
                    CREATE OR REPLACE VIEW {view_name} AS
                    SELECT
                        id,
                        dataset_name,
                        compartment,
                        source_url,
                        source_domain,
                        schema_path,
                        payload,
                        ai_cluster_id,
                        ai_cluster_label,
                        ai_price_band,
                        scraped_at,
                        loaded_at
                    FROM scraped_records
                    WHERE dataset_name = :dataset_name
                    """
                ),
                {"dataset_name": dataset_name},
            )

        total_rows = conn.execute(text("SELECT COUNT(*) FROM scraped_records")).scalar_one()
        total_books_rows = conn.execute(text("SELECT COUNT(*) FROM scraped_books")).scalar_one()

    return {
        "loaded_rows": int(len(df)),
        "warehouse_rows": int(total_rows),
        "books_rows": int(total_books_rows),
        "books_projected_rows": int(books_loaded),
    }
