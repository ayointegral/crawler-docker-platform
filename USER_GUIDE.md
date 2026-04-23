# User Guide: Dockerized Crawler Platform

## 1. Overview

This platform is a batch crawl-and-analytics stack with one operator entrypoint.

Flow:
1. Submit crawl inputs in Control Center.
2. Airflow triggers DAG `crawler_csv_to_postgres`.
3. Crawler writes per-run raw CSV.
4. Transform step cleans and normalizes data.
5. Load step upserts to PostgreSQL.
6. Superset dashboard reads the warehouse table.

## 2. Single Entry Point

Use `http://localhost:8501` (`Crawler Control Center`) for day-to-day operations:
- submit `start_urls`
- submit optional `keywords`
- submit `compartment` / purpose label
- set `max_pages`
- trigger manual DAG runs
- pause/unpause DAG
- view schedule/frequency and recent run states

## 3. Services and Ports

- `control-ui`: `8501` (operator UI)
- `airflow-webserver`: `8080` (scheduler UI/API)
- `airflow-scheduler`: no host port (worker/scheduler)
- `airflow-triggerer`: no host port (triggerer runtime)
- `superset`: `8088` (analytics UI)
- `postgres`: no host port (internal DB)
- `airflow-init`, `superset-init`: one-time bootstrap containers
- `crawler`: manual profile-only utility container

## 4. Credentials

- Airflow: `admin` / `admin`
- Superset: `admin` / `admin`
- Postgres app user: `platform` / `platform`

Postgres DB names:
- `airflow`
- `analytics`
- `superset_metadata`

## 5. Start and Health Check

Start:
```bash
cp .env.example .env
docker compose -f compose.yml up --build -d
```

Update `.env` first if you want different default URL lists, keywords, page limits, or compartment labels.

First-run bootstrap:
- If there are no runs yet for `crawler_csv_to_postgres`, `airflow-init` auto-triggers `bootstrap__initial`.
- This seeds initial data so Superset charts populate without manual triggering.
- If runs already exist, bootstrap is skipped.

Check service state:
```bash
docker compose -f compose.yml ps
```

Expected running services:
- `crawler-control-ui`
- `airflow-webserver`
- `airflow-scheduler`
- `airflow-triggerer`
- `crawler-postgres`
- `superset`

## 6. Daily Operations (Recommended)

1. Open `http://localhost:8501`.
2. Enter website URLs (comma or newline separated).
3. Add optional keywords (comma separated).
4. Set compartment/purpose (for example `pricing_watch`, `seo_research`).
5. Set max pages.
6. Click `Trigger Crawl Run`.
7. Monitor run state in the `Recent Runs` table.
8. Open Superset dashboard from the sidebar quick link.

## 7. Orchestration and Schedule Details

Airflow DAG:
- ID: `crawler_csv_to_postgres`
- schedule: `@daily`
- catchup: `false`
- max active runs: `1`
- retries: `2`
- retry delay: `2 minutes`

Task chain:
- `crawl_books_site` -> `transform_csv` -> `load_postgres`

Per-run file outputs:
- raw: `data/raw/books_raw_<run_id>.csv`
- processed: `data/processed/books_clean_<run_id>.csv`

## 8. Airflow CLI Operations

Unpause and trigger:
```bash
docker compose -f compose.yml exec -T airflow-webserver airflow dags unpause crawler_csv_to_postgres
docker compose -f compose.yml exec -T airflow-webserver airflow dags trigger crawler_csv_to_postgres
```

List runs:
```bash
docker compose -f compose.yml exec -T airflow-webserver \
  airflow dags list-runs -d crawler_csv_to_postgres --no-backfill --output table
```

Task states for a run:
```bash
docker compose -f compose.yml exec -T airflow-webserver \
  airflow tasks states-for-dag-run crawler_csv_to_postgres <RUN_ID>
```

## 9. Data Validation and Warehouse Checks

Inspect CSV outputs:
```bash
ls -lh data/raw data/processed
```

Warehouse row count:
```bash
docker compose -f compose.yml exec -T postgres \
  psql -U platform -d analytics -c "SELECT COUNT(*) AS rows_loaded FROM scraped_books;"
```

Recent loaded rows:
```bash
docker compose -f compose.yml exec -T postgres \
  psql -U platform -d analytics -c "SELECT title, price, rating, loaded_at FROM scraped_books ORDER BY loaded_at DESC LIMIT 20;"
```

## 10. Superset Provisioning and Dashboard

Superset assets are auto-seeded by `superset/bootstrap_superset.py`:
- Database: `analytics_warehouse`
- Dataset: `scraped_books`
- Charts:
  - `Rows by Compartment`
  - `Rating Distribution (Pie)`
  - `Average Price by Rating`
  - `Average Price by Compartment`
  - `Rows by Scrape Day`
  - `Top Expensive Books`
- Dashboard:
  - title: `Crawler Analytics`
  - slug: `crawler-analytics`
  - URL: `http://localhost:8088/superset/dashboard/crawler-analytics/`

Re-run bootstrap manually:
```bash
docker compose -f compose.yml exec -T superset python /app/pythonpath/bootstrap_superset.py
```

## 11. Runtime Inputs and Configuration

Runtime inputs from Control Center form:
- `start_urls` (list)
- `keywords` (list)
- `compartment` (string label used for data compartmentalization)
- `max_pages` (int)

Environment defaults in `compose.yml`:
- `CRAWLER_START_URLS`
- `CRAWLER_KEYWORDS`
- `CRAWLER_MAX_PAGES`
- `CRAWLER_COMPARTMENT`
- `AIRFLOW_BOOTSTRAP_ON_STARTUP`
- `AIRFLOW_BOOTSTRAP_DAG_ID`
- `OUTPUT_FILE`
- `RAW_CSV_PATH`
- `PROCESSED_CSV_PATH`
- `WAREHOUSE_DSN`

Behavior notes:
- Runtime form values override defaults for that DAG run.
- If no keywords are provided, all parsed items are eligible.
- Loader is idempotent via `ON CONFLICT (source_url, compartment) DO UPDATE`.

Use `.env` for default URL list and purpose:
```bash
cp .env.example .env
```

Example `.env` values:
```bash
CRAWLER_START_URLS=https://books.toscrape.com/
CRAWLER_KEYWORDS=
CRAWLER_MAX_PAGES=2
CRAWLER_COMPARTMENT=env_watch
AIRFLOW_BOOTSTRAP_ON_STARTUP=true
AIRFLOW_BOOTSTRAP_DAG_ID=crawler_csv_to_postgres
```

## 12. Compartmented Data Model

Warehouse table: `analytics.scraped_books`
- `source_domain` captures the source host (for example `books.toscrape.com`)
- `compartment` captures the scrape purpose/domain label
- uniqueness is enforced by `source_url + compartment`

Useful query:
```sql
SELECT compartment, source_domain, COUNT(*) AS rows
FROM scraped_books
GROUP BY compartment, source_domain
ORDER BY rows DESC;
```

## 13. End-to-End Validation (Verified)

Verified path A (`.env` defaults -> Airflow DAG):
```bash
docker compose -f compose.yml exec -T airflow-webserver env | egrep 'CRAWLER_START_URLS|CRAWLER_COMPARTMENT'
docker compose -f compose.yml exec -T airflow-webserver airflow dags trigger crawler_csv_to_postgres
```

Verified path B (Control Center UI -> Airflow DAG conf):
- Submit `start_urls`, `keywords`, `compartment`, `max_pages` in `http://localhost:8501`
- Confirm success message and run id in UI

Verified outputs:
```bash
ls -1t data/raw | head -n 3
ls -1t data/processed | head -n 3
```

Database proof query:
```bash
docker compose -f compose.yml exec -T postgres \
  psql -U platform -d analytics -c "SELECT compartment, source_domain, COUNT(*) AS rows FROM scraped_books GROUP BY compartment, source_domain ORDER BY rows DESC;"
```

Superset proof:
- Re-seed dashboard assets: `docker compose -f compose.yml exec -T superset python /app/pythonpath/bootstrap_superset.py`
- Open `http://localhost:8088/superset/dashboard/crawler-analytics/`
- Confirm compartment charts are visible:
  - `Rows by Compartment`
  - `Average Price by Compartment`

## 14. Logs and Monitoring

All logs:
```bash
docker compose -f compose.yml logs -f
```

Core service logs:
```bash
docker compose -f compose.yml logs -f control-ui
docker compose -f compose.yml logs -f airflow-scheduler
docker compose -f compose.yml logs -f airflow-triggerer
docker compose -f compose.yml logs -f airflow-webserver
docker compose -f compose.yml logs -f superset
docker compose -f compose.yml logs -f postgres
```

Recent error scan:
```bash
docker compose -f compose.yml logs --tail=300 \
  control-ui airflow-webserver airflow-scheduler airflow-triggerer postgres superset \
  | egrep -i "error|fatal|traceback|exception"
```

## 15. Troubleshooting

`Control Center` cannot trigger runs:
- check `airflow-webserver` status with `docker compose -f compose.yml ps`
- verify credentials in `compose.yml` for `control-ui`
- verify Airflow API auth backend is basic auth

DAG remains queued:
- confirm `airflow-scheduler` is running
- confirm `airflow-triggerer` is running
- unpause DAG from Control Center or Airflow UI
- check scheduler logs for task import errors

Airflow task logs return `403 FORBIDDEN` from `/log/...`:
- ensure all Airflow components share the same `AIRFLOW__WEBSERVER__SECRET_KEY`
- restart `airflow-webserver`, `airflow-scheduler`, and `airflow-triggerer` after secret changes
- verify host time is synchronized (required for signed log access tokens)

Superset opens but dashboard is missing charts:
- run bootstrap script again
- refresh dashboard page after bootstrap finishes
- check `superset` logs for metadata DB connectivity errors

Need clean reset:
```bash
docker compose -f compose.yml down -v
rm -f data/raw/*.csv data/processed/*.csv
docker compose -f compose.yml up --build -d
```

## 16. Stop and Restart

Stop stack:
```bash
docker compose -f compose.yml down
```

Restart running services:
```bash
docker compose -f compose.yml restart
```

## 17. Production Hardening Checklist

- Replace default credentials and secret keys.
- Move secrets to a secret manager.
- Add reverse proxy and TLS.
- Add metrics/alerts and central logging.
- Use Airflow remote logging backend.
- Configure Superset CSP and external rate limiter store.
- Add CI for lint, tests, and image scanning.
