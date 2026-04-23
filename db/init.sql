CREATE DATABASE analytics;
CREATE DATABASE superset_metadata;
\connect analytics

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

CREATE INDEX IF NOT EXISTS idx_scraped_books_rating ON scraped_books (rating);
CREATE INDEX IF NOT EXISTS idx_scraped_books_scraped_at ON scraped_books (scraped_at);
CREATE INDEX IF NOT EXISTS idx_scraped_books_compartment ON scraped_books (compartment);
CREATE INDEX IF NOT EXISTS idx_scraped_books_source_domain ON scraped_books (source_domain);
CREATE UNIQUE INDEX IF NOT EXISTS uq_scraped_books_source_url_compartment
ON scraped_books (source_url, compartment);
