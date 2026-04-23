from __future__ import annotations

import logging
import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.append("/opt/platform")
sys.path.append("/opt/platform/crawler")
sys.path.append("/opt/airflow/src")

from app.config import load_config
from app.crawler import run_crawler
from pipeline_etl.ai_enrich import enrich_with_ai
from pipeline_etl.load import load_into_postgres
from pipeline_etl.settings import PROCESSED_CSV_PATH, RAW_CSV_PATH
from pipeline_etl.transform import transform_raw_csv

logger = logging.getLogger(__name__)


def _sanitize_id(value: str) -> str:
    return "".join(char if char.isalnum() else "_" for char in value)


def _coerce_conf_list(value: str | list[str] | None) -> list[str] | None:
    if value is None:
        return None
    if isinstance(value, list):
        parsed = [str(item).strip() for item in value if str(item).strip()]
    else:
        parsed = []
        for line in str(value).splitlines():
            parsed.extend(part.strip() for part in line.split(",") if part.strip())
    return parsed or None


def _build_output_paths(run_id: str, dataset_name: str) -> tuple[str, str]:
    safe_run = _sanitize_id(run_id)
    safe_dataset = _sanitize_id(dataset_name.lower()) or "dataset"
    raw_path = f"/opt/platform/data/raw/{safe_dataset}_raw_{safe_run}.csv"
    processed_path = f"/opt/platform/data/processed/{safe_dataset}_clean_{safe_run}.csv"
    return raw_path, processed_path


def run_crawler_task(**context) -> dict:
    dag_run = context["dag_run"]
    conf = dag_run.conf or {}

    overrides: dict = {}
    start_urls = _coerce_conf_list(conf.get("start_urls"))
    keywords = _coerce_conf_list(conf.get("keywords"))
    if start_urls:
        overrides["start_urls"] = start_urls
    if keywords is not None:
        overrides["keywords"] = keywords
    if conf.get("compartment"):
        overrides["compartment"] = str(conf["compartment"])
    if conf.get("max_pages") is not None:
        overrides["max_pages"] = int(conf["max_pages"])
    if conf.get("source_config_path"):
        overrides["source_config_path"] = str(conf["source_config_path"])
    if conf.get("schema_path"):
        overrides["schema_path"] = str(conf["schema_path"])

    config = load_config(overrides=overrides)
    raw_path, processed_path = _build_output_paths(dag_run.run_id, config.schema.dataset_name)
    run_overrides = {**overrides, "output_file": raw_path}

    config = load_config(overrides=run_overrides)
    logger.info("Crawler config: %s", config)
    run_crawler(config)

    if not os.path.exists(raw_path):
        raise FileNotFoundError(f"Crawler did not produce CSV at {raw_path}")

    return {
        "raw_csv_path": raw_path,
        "processed_csv_path": processed_path,
        "compartment": config.compartment,
        "dataset_name": config.schema.dataset_name,
        "schema_path": config.schema_path,
    }


def transform_task(**context) -> dict:
    paths = context["ti"].xcom_pull(task_ids="crawl_books_site") or {}
    result = transform_raw_csv(
        raw_path=paths.get("raw_csv_path", RAW_CSV_PATH),
        output_path=paths.get("processed_csv_path", PROCESSED_CSV_PATH),
        default_compartment=paths.get("compartment", "default"),
        default_dataset_name=paths.get("dataset_name", "default_dataset"),
        schema_path=paths.get("schema_path"),
    )
    logger.info("Transform result: %s", result)
    return result


def ai_enrich_task(**context) -> dict:
    transform_result = context["ti"].xcom_pull(task_ids="transform_csv") or {}
    result = enrich_with_ai(
        input_path=transform_result.get("output_path", PROCESSED_CSV_PATH),
        output_path=transform_result.get("output_path", PROCESSED_CSV_PATH),
    )
    logger.info("AI enrich result: %s", result)
    return result


def load_task(**context) -> dict:
    ai_result = context["ti"].xcom_pull(task_ids="ai_enrich_csv") or {}
    result = load_into_postgres(
        csv_path=ai_result.get("output_path", PROCESSED_CSV_PATH),
    )
    logger.info("Load result: %s", result)
    return result


default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="crawler_csv_to_postgres",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["crawler", "etl"],
) as dag:
    crawl = PythonOperator(
        task_id="crawl_books_site",
        python_callable=run_crawler_task,
        execution_timeout=timedelta(minutes=10),
    )

    transform = PythonOperator(
        task_id="transform_csv",
        python_callable=transform_task,
        execution_timeout=timedelta(minutes=5),
    )

    ai_enrich = PythonOperator(
        task_id="ai_enrich_csv",
        python_callable=ai_enrich_task,
        execution_timeout=timedelta(minutes=5),
    )

    load = PythonOperator(
        task_id="load_postgres",
        python_callable=load_task,
        execution_timeout=timedelta(minutes=5),
    )

    crawl >> transform >> ai_enrich >> load
