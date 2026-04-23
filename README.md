# Data Ingestion and Analysis Platform

Containerized platform for configurable web crawling, orchestration, data warehousing, and dashboards.

## What You Get
- Config-driven crawler (YAML schema + selector mappings).
- Airflow DAG with CeleryExecutor: `crawl -> transform -> ai_enrich -> load`.
- CSV landing in `data/raw` and `data/processed`.
- PostgreSQL warehouse with:
  - generic table: `scraped_records`
  - projection table: `scraped_books` (for book-compatible fields)
  - auto-created dataset views: `vw_dataset_<dataset_name>`
- Superset auto-bootstrap:
  - `Crawler Analytics` dashboard (`/superset/dashboard/crawler-analytics/`)
  - `Crawler Platform Overview` dashboard (`/superset/dashboard/crawler-platform-overview/`)
- Control Center UI (`http://localhost:8501`) on NiceGUI as single entry point (triggered via Temporal workflows).

## Services
- `postgres`
- `redis`
- `airflow-init`
- `airflow-webserver`
- `airflow-scheduler`
- `airflow-worker`
- `airflow-triggerer`
- `crawler` (manual profile)
- `superset-init`
- `superset`
- `control-ui` (NiceGUI primary)
- `temporal` (workflow engine)
- `temporal-ui`
- `temporal-orchestrator` (API + worker that starts Temporal workflows and triggers Airflow DAG runs)
- `platform-bootstrap` (automatic first-run bootstrap workflow trigger for out-of-box data seeding)
- `control-ui-legacy` (Streamlit fallback, profile `legacy-ui`)

## Quick Start
```bash
cp .env.example .env
docker compose -f compose.yml up --build -d
docker compose -f compose.yml ps
```

Open:
- Control UI (NiceGUI): `http://localhost:8501`
- Airflow: `http://localhost:8080`
- Superset: `http://localhost:8088`
- Temporal UI: `http://localhost:8233`

Credentials:
- Airflow: `admin` / `admin`
- Superset: `admin` / `admin`


## Out-of-Box Behavior
- `docker compose -f compose.yml up --build -d` is enough for first run.
- `platform-bootstrap` waits for Airflow + Temporal orchestrator, then triggers one initial Temporal workflow **only when no DAG runs exist yet**.
- Superset assets are auto-created; data appears once the bootstrap DAG run finishes.
- Expected first startup time for full stack + initial data: about 2-6 minutes depending on machine/network.

## Orchestration Flow
- Control UI submits run config to `temporal-orchestrator` (`/workflows/crawl/trigger`).
- Temporal workflow (`CrawlPipelineWorkflow`) unpauses DAG, triggers run, and tracks workflow metadata.
- Airflow executes ETL tasks and writes to Postgres.
- Superset reads warehouse tables and dashboards update.

## Source Configuration (Schema-Driven)
Default schema: `crawler/schemas/default_schema.yml`

Example schema fields:
- `dataset_name`
- `record_selector`
- `detail_link_selector` (optional)
- `pagination_selector` (optional)
- `fields[]` with:
  - `name`
  - `data_type` (`string`, `integer`, `number`, `boolean`)
  - `selector` (`css` or `xpath`)
  - optional `regex`
  - optional `value_map`

## Ways to Pass Crawl Input
1. `.env` defaults (`CRAWLER_START_URLS`, `CRAWLER_*`).
2. Control Center UI runtime input (`start_urls`, `keywords`, `max_pages`, `compartment`, `source_config_path`).
3. Airflow API DAG trigger (`conf`).

## Data Model
Generic warehouse table (`analytics.scraped_records`):
- metadata: `dataset_name`, `compartment`, `source_url`, `source_domain`, `schema_path`, `scraped_at`
- AI fields: `ai_cluster_id`, `ai_cluster_label`, `ai_price_band`
- payload: `payload JSONB`

Books projection (`analytics.scraped_books`):
- typed analytic columns for charts: `title`, `price`, `rating`, `stock_count`, etc.

## End-to-End Check
```bash
curl -X POST 'http://localhost:8090/workflows/crawl/trigger' \
  -H 'Content-Type: application/json' \
  -d '{"dag_id":"crawler_csv_to_postgres","conf":{"start_urls":["https://books.toscrape.com/"],"max_pages":1,"compartment":"smoke_test","source_config_path":"/opt/platform/crawler/schemas/default_schema.yml"}}'

# Verify warehouse rows
docker compose -f compose.yml exec -T postgres \
  psql -U platform -d analytics -c "SELECT dataset_name, compartment, COUNT(*) FROM scraped_records GROUP BY dataset_name, compartment ORDER BY 3 DESC;"
```

## Documentation
- Full operator/developer guide: `USER_GUIDE.md`

## Legacy UI Fallback
```bash
docker compose -f compose.yml --profile legacy-ui up -d control-ui-legacy
```
Open `http://localhost:8502`.

## TODO (Tracked)
- Keep `USER_GUIDE.md` comprehensive and updated after each architecture or workflow change.
