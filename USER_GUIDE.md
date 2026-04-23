# User Guide: Data Ingestion and Analysis Platform

## 1. Overview
This platform is a reusable, schema-driven crawl and analytics stack.

Execution path:
1. Submit source details from the Control Center UI or Airflow API.
2. Airflow runs DAG `crawler_csv_to_postgres`.
3. Crawler writes raw CSV to `data/raw`.
4. Transform stage validates and normalizes records to `data/processed`.
5. AI stage enriches rows with clustering and price bands.
6. Load stage writes to PostgreSQL generic and projection tables.
7. Superset dashboards query PostgreSQL and visualize results.

## 2. Architecture
- Crawler: Scrapy-based, config/schema-driven (`crawler/schemas/*.yml`).
- Orchestration: Apache Airflow with CeleryExecutor.
- Queue/Broker: Redis.
- Warehouse: PostgreSQL.
- Visualization: Apache Superset with bootstrap provisioning.
- Operator Web UI: Streamlit Control Center.

## 3. Services and Ports
- `control-ui` -> `http://localhost:8501`
- `airflow-webserver` -> `http://localhost:8080`
- `superset` -> `http://localhost:8088`
- `postgres`, `redis`, `airflow-scheduler`, `airflow-worker`, `airflow-triggerer` run internal-only.

## 4. Credentials
- Airflow: `admin` / `admin`
- Superset: `admin` / `admin`
- Postgres app user: `platform` / `platform`

Databases:
- `airflow` (Airflow metadata)
- `analytics` (warehouse)
- `superset_metadata` (Superset metadata)

## 5. Start the Stack
```bash
cp .env.example .env
docker compose -f compose.yml up --build -d
docker compose -f compose.yml ps
```

Expected main running containers:
- `crawler-postgres`
- `crawler-redis`
- `airflow-webserver`
- `airflow-scheduler`
- `airflow-worker`
- `airflow-triggerer`
- `superset`
- `crawler-control-ui`

One-time containers:
- `airflow-init`
- `superset-init`

## 6. Environment Defaults
Always create `.env` from sample first:
```bash
cp .env.example .env
```

Core crawler defaults in `.env`:
- `CRAWLER_START_URLS`
- `CRAWLER_KEYWORDS`
- `CRAWLER_MAX_PAGES`
- `CRAWLER_COMPARTMENT`
- `CRAWLER_SOURCE_CONFIG`

Core auth/secrets:
- `AIRFLOW_ADMIN_USERNAME`, `AIRFLOW_ADMIN_PASSWORD`
- `AIRFLOW__WEBSERVER__SECRET_KEY`
- `SUPERSET_ADMIN_USERNAME`, `SUPERSET_ADMIN_PASSWORD`
- `SUPERSET_SECRET_KEY`

## 7. Schema-Driven Crawling
### 7.1 Schema location
- Default schema: `crawler/schemas/default_schema.yml`
- Example alternate schema: `crawler/schemas/articles_schema.yml`

### 7.2 Schema format
Required keys:
- `dataset_name`
- `record_selector`
- `fields`

Optional keys:
- `detail_link_selector`
- `pagination_selector`

Each field includes:
- `name`
- `data_type`
- `selector` (`css` or `xpath`)
- optional `regex`
- optional `value_map`

### 7.3 Example
`crawler/schemas/default_schema.yml` maps `books.toscrape.com` cards and extracts:
- `title`
- `price`
- `rating`
- `availability_text`

## 8. How to Trigger Runs
### 8.1 Control Center UI (recommended)
Open `http://localhost:8501` and use the form:
- Website URLs
- Keywords
- Max pages
- Compartment/purpose
- Source schema path

You can also tick `Use only .env defaults for this run`.

### 8.2 Airflow API
```bash
curl -u admin:admin -X POST 'http://localhost:8080/api/v1/dags/crawler_csv_to_postgres/dagRuns' \
  -H 'Content-Type: application/json' \
  -d '{
    "conf": {
      "start_urls": ["https://books.toscrape.com/"],
      "keywords": [],
      "max_pages": 1,
      "compartment": "manual_ui_test",
      "source_config_path": "/opt/platform/crawler/schemas/default_schema.yml"
    }
  }'
```

## 9. DAG and Scheduling
DAG: `crawler_csv_to_postgres`

Task chain:
- `crawl_books_site`
- `transform_csv`
- `ai_enrich_csv`
- `load_postgres`

Defaults:
- schedule: `@daily`
- retries: `2`
- retry delay: `2 minutes`
- catchup: `false`
- max active runs: `1`

## 10. Storage Layout
Per-run CSV naming includes dataset + run id:
- Raw: `data/raw/<dataset>_raw_<run_id>.csv`
- Processed: `data/processed/<dataset>_clean_<run_id>.csv`

## 11. Warehouse Model
### 11.1 Generic table
`analytics.scraped_records`
- `dataset_name`
- `compartment`
- `source_url`
- `source_domain`
- `schema_path`
- `payload` (`JSONB`)
- `ai_cluster_id`
- `ai_cluster_label`
- `ai_price_band`
- `scraped_at`
- `loaded_at`

Uniqueness:
- `(dataset_name, compartment, source_url)`

### 11.2 Projection table
`analytics.scraped_books`
- Populated when required book fields exist (`title`, `price`, `rating`, etc.)
- Used by book-focused charts.

### 11.3 Dataset views
Loader auto-creates:
- `vw_dataset_<dataset_name>`

Example:
- `vw_dataset_books`

## 12. Superset Provisioning
Bootstrap script: `superset/bootstrap_superset.py`

Creates/updates:
- Database connection: `analytics_warehouse`
- Datasets:
  - `scraped_records`
  - `scraped_books`
  - any `vw_dataset_*` views
- Dashboards:
  - `Crawler Analytics` (`/superset/dashboard/crawler-analytics/`)
  - `Crawler Platform Overview` (`/superset/dashboard/crawler-platform-overview/`)

Charts include bars, line charts, and pie/donut charts.

Re-run bootstrap:
```bash
docker compose -f compose.yml exec -T superset python /app/pythonpath/bootstrap_superset.py
```

## 13. Real Service-to-Service Validation (Executed)
Validation completed on April 23, 2026.

### 13.1 Health/API checks
```bash
curl -sSf http://localhost:8501 >/dev/null
curl -sSf http://localhost:8080/health >/dev/null
curl -sSf -u admin:admin http://localhost:8080/api/v1/dags/crawler_csv_to_postgres/details >/dev/null
curl -sSf http://localhost:8088/health >/dev/null
```

### 13.2 Triggered and completed manual run
Run id example:
- `manual__2026-04-23T09:48:47.620446+00:00`

State reached: `success`.

### 13.3 Warehouse proof
```sql
SELECT compartment, COUNT(*)
FROM scraped_records
GROUP BY compartment
ORDER BY 2 DESC;
```
Observed compartments include:
- `env_watch`
- `manual_ui_test`

### 13.4 Superset metadata proof
```sql
SELECT slug, dashboard_title FROM dashboards ORDER BY slug;
SELECT slice_name, viz_type FROM slices ORDER BY slice_name;
```
Observed dashboards:
- `crawler-analytics`
- `crawler-platform-overview`

Observed charts include:
- `Rows by Dataset`
- `Rows by Compartment (Pie)`
- `Rows by Source Domain`
- `Rows by AI Cluster`
- `Top Expensive Books`

## 14. Adding a New Use Case in ~10 Minutes
1. Create schema file in `crawler/schemas/` (for example `my_site.yml`).
2. Set `dataset_name` and selectors in schema.
3. Trigger DAG from UI with:
- your URLs
- compartment label
- `source_config_path=/opt/platform/crawler/schemas/my_site.yml`
4. Wait for DAG success.
5. Re-run Superset bootstrap.
6. Open `Crawler Platform Overview` to confirm dataset rows.

## 15. Operations and Logs
All logs:
```bash
docker compose -f compose.yml logs -f
```

Focused logs:
```bash
docker compose -f compose.yml logs -f airflow-scheduler
docker compose -f compose.yml logs -f airflow-worker
docker compose -f compose.yml logs -f airflow-webserver
docker compose -f compose.yml logs -f airflow-triggerer
docker compose -f compose.yml logs -f superset
docker compose -f compose.yml logs -f control-ui
docker compose -f compose.yml logs -f postgres
```

Scan for failures:
```bash
docker compose -f compose.yml logs --tail=400 \
  airflow-webserver airflow-scheduler airflow-worker airflow-triggerer superset control-ui postgres \
  | egrep -i "error|fatal|traceback|exception|forbidden|name or service not known"
```

## 16. Known Behavior
- If a run returns zero records (for example strict keyword filter), transform stage now writes an empty processed CSV and pipeline continues without hard failure.
- Superset dashboard route usually returns HTTP `302` until logged in.

## 17. Troubleshooting
### Airflow logs inaccessible (`403 FORBIDDEN`)
- Ensure all Airflow components share `AIRFLOW__WEBSERVER__SECRET_KEY`.
- Restart all Airflow services after secret change.
- Ensure host/container times are synchronized.

### DAG run stuck
- Check `airflow-scheduler` and `airflow-worker` logs.
- Confirm DAG is unpaused in Control UI or Airflow UI.

### Superset dashboards missing datasets/charts
- Ensure at least one successful load to warehouse.
- Re-run Superset bootstrap script.

### Want clean reset
```bash
docker compose -f compose.yml down -v
rm -f data/raw/*.csv data/processed/*.csv
docker compose -f compose.yml up --build -d
```

## 18. Stop/Restart
Stop:
```bash
docker compose -f compose.yml down
```

Restart:
```bash
docker compose -f compose.yml restart
```

## 19. Hardening Checklist
- Replace default credentials and secrets.
- Add CSP and Superset production configs.
- Use external secret manager.
- Add RBAC controls per dataset/domain.
- Add backup/retention for warehouse tables.

## 20. Documentation TODO
- Keep this guide comprehensive and aligned with current compose services, DAG flow, schemas, and Superset assets after every change.
