# Dockerized Web Crawler + Data Pipeline + Visualization

This project runs an end-to-end batch platform:
1. Crawl websites with Scrapy.
2. Transform raw CSV in Airflow.
3. Load into PostgreSQL (`analytics.scraped_books`).
4. Explore in Superset with pre-provisioned charts/dashboard.

Full operations guide: `USER_GUIDE.md`.

## Single Entry Point

- Control Center UI: `http://localhost:8501`
- Use it to submit crawl URLs/keywords/compartment, trigger runs, view schedule/frequency, and monitor run status.

## Key UIs and Credentials

- Control Center: `http://localhost:8501` (no login)
- Airflow: `http://localhost:8080` (`admin` / `admin`)
- Superset: `http://localhost:8088` (`admin` / `admin`)

## Architecture

```text
[Control Center UI]
        |
        v
[Airflow DAG: crawler_csv_to_postgres]
        |
        v
[Scrapy Crawl -> data/raw/books_raw_<run_id>.csv]
        |
        v
[Transform -> data/processed/books_clean_<run_id>.csv]
        |
        v
[PostgreSQL analytics.scraped_books (compartmented)]
        |
        v
[Superset Dashboard: /superset/dashboard/crawler-analytics/]
```

## Services

- `postgres`: Airflow metadata + analytics warehouse + Superset metadata DBs
- `airflow-init`: one-time DB migration and admin user creation
- `airflow-webserver`: Airflow UI/API
- `airflow-scheduler`: DAG scheduling and task orchestration
- `airflow-triggerer`: triggerer/deferrable task runtime (health visible in Cluster Activity)
- `superset-init`: one-time Superset bootstrap + chart/dashboard provisioning
- `superset`: visualization UI
- `control-ui`: unified operator UI
- `crawler`: optional standalone crawler (manual profile)

## Quick Start

```bash
cp .env.example .env
docker compose -f compose.yml up --build -d
docker compose -f compose.yml ps
```

Then open `http://localhost:8501` and trigger a run from the form.

First-run bootstrap behavior:
- On a fresh environment (no prior DAG runs), `airflow-init` auto-triggers one initial DAG run (`bootstrap__initial`) so Superset has data without manual steps.
- If runs already exist, bootstrap trigger is skipped.

Edit `.env` values to match your target sources and scrape purpose before startup when needed.

## Superset Auto Provisioning

Assets are created on startup by `superset/bootstrap_superset.py`:
- Database connection: `analytics_warehouse`
- Dataset: `scraped_books`
- Charts:
  - `Rows by Compartment`
  - `Rating Distribution (Pie)`
  - `Average Price by Rating`
  - `Average Price by Compartment`
  - `Rows by Scrape Day`
  - `Top Expensive Books`
- Dashboard:
  - `Crawler Analytics`
  - URL path: `/superset/dashboard/crawler-analytics/`

## Scheduling

- DAG ID: `crawler_csv_to_postgres`
- Default schedule: `@daily`
- Catchup: `false`
- Max active runs: `1`
- Manual runs from Control Center are supported and can override:
  - `start_urls`
  - `keywords`
  - `compartment`
  - `max_pages`
  - bootstrap controls via `.env`:
    - `AIRFLOW_BOOTSTRAP_ON_STARTUP`
    - `AIRFLOW_BOOTSTRAP_DAG_ID`

## Data Model

Target table: `analytics.scraped_books`
- `id` `BIGSERIAL` primary key
- `title` `TEXT` not null
- `price` `NUMERIC(10,2)` not null
- `rating` `SMALLINT` not null
- `stock_count` `INTEGER` not null
- `availability_text` `TEXT`
- `source_url` `TEXT` not null
- `source_domain` `TEXT` not null
- `compartment` `TEXT` not null
- `scraped_at` `TIMESTAMPTZ` not null
- `loaded_at` `TIMESTAMPTZ` default `NOW()`

Indexes:
- `idx_scraped_books_rating`
- `idx_scraped_books_scraped_at`
- `idx_scraped_books_compartment`
- `idx_scraped_books_source_domain`
- `uq_scraped_books_source_url_compartment` (composite unique key)
