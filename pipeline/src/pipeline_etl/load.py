from __future__ import annotations

import os

import pandas as pd
from sqlalchemy import create_engine, text

from .settings import PROCESSED_CSV_PATH, WAREHOUSE_DSN

SCHEMA_SQL = """
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
    scraped_at TIMESTAMPTZ NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

ALTER TABLE scraped_books DROP CONSTRAINT IF EXISTS scraped_books_source_url_key;

ALTER TABLE scraped_books ADD COLUMN IF NOT EXISTS source_domain TEXT;
ALTER TABLE scraped_books ADD COLUMN IF NOT EXISTS compartment TEXT NOT NULL DEFAULT 'default';
UPDATE scraped_books
SET source_domain = LOWER(SPLIT_PART(SPLIT_PART(source_url, '//', 2), '/', 1))
WHERE source_domain IS NULL OR source_domain = '';
UPDATE scraped_books SET compartment = 'default' WHERE compartment IS NULL;
ALTER TABLE scraped_books ALTER COLUMN source_domain SET NOT NULL;
ALTER TABLE scraped_books ALTER COLUMN compartment SET NOT NULL;

CREATE INDEX IF NOT EXISTS idx_scraped_books_rating ON scraped_books (rating);
CREATE INDEX IF NOT EXISTS idx_scraped_books_scraped_at ON scraped_books (scraped_at);
CREATE INDEX IF NOT EXISTS idx_scraped_books_compartment ON scraped_books (compartment);
CREATE INDEX IF NOT EXISTS idx_scraped_books_source_domain ON scraped_books (source_domain);
CREATE UNIQUE INDEX IF NOT EXISTS uq_scraped_books_source_url_compartment
ON scraped_books (source_url, compartment);
"""

UPSERT_SQL = """
INSERT INTO scraped_books (
    title,
    price,
    rating,
    stock_count,
    availability_text,
    source_url,
    source_domain,
    compartment,
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
    scraped_at = EXCLUDED.scraped_at,
    loaded_at = NOW();
"""


def load_into_postgres(csv_path: str = PROCESSED_CSV_PATH, dsn: str = WAREHOUSE_DSN) -> dict:
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"Processed CSV not found at {csv_path}")

    df = pd.read_csv(csv_path, parse_dates=["scraped_at"])
    if df.empty:
        raise ValueError("Processed CSV is empty; load aborted")

    engine = create_engine(dsn)
    with engine.begin() as conn:
        conn.execute(text(SCHEMA_SQL))
        for record in df.to_dict(orient="records"):
            conn.execute(text(UPSERT_SQL), record)

        total_rows = conn.execute(text("SELECT COUNT(*) FROM scraped_books")).scalar_one()

    return {
        "loaded_rows": int(len(df)),
        "warehouse_rows": int(total_rows),
    }
