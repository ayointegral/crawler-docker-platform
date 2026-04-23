from __future__ import annotations

import logging
import os
import sys
from dataclasses import replace
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.append("/opt/platform")
sys.path.append("/opt/platform/crawler")
sys.path.append("/opt/airflow/src")

from app.config import load_config
from app.crawler import run_crawler
from pipeline_etl.load import load_into_postgres
from pipeline_etl.settings import PROCESSED_CSV_PATH, RAW_CSV_PATH
from pipeline_etl.transform import transform_raw_csv

logger = logging.getLogger(__name__)


def _sanitize_id(value: str) -> str:
    return "".join(char if char.isalnum() else "_" for char in value)


def _build_output_paths(run_id: str) -> tuple[str, str]:
    safe_run = _sanitize_id(run_id)
    raw_path = f"/opt/platform/data/raw/books_raw_{safe_run}.csv"
    processed_path = f"/opt/platform/data/processed/books_clean_{safe_run}.csv"
    return raw_path, processed_path


def _coerce_conf_list(value: str | list[str] | None) -> list[str] | None:
    if value is None:
        return None
    if isinstance(value, list):
        parsed = [str(item).strip() for item in value if str(item).strip()]
    else:
        parsed = [item.strip() for item in str(value).split(",") if item.strip()]
    return parsed or None


def run_crawler_task(**context) -> dict:
    dag_run = context["dag_run"]
    conf = dag_run.conf or {}
    raw_path, processed_path = _build_output_paths(dag_run.run_id)

    config = load_config()
    overrides = {"output_file": raw_path}

    start_urls = _coerce_conf_list(conf.get("start_urls"))
    keywords = _coerce_conf_list(conf.get("keywords"))
    compartment = (conf.get("compartment") or "").strip()
    if start_urls:
        overrides["start_urls"] = start_urls
    if keywords is not None:
        overrides["keyword_filters"] = [keyword.lower() for keyword in keywords]
    if compartment:
        overrides["compartment"] = compartment
    if conf.get("max_pages") is not None:
        overrides["max_pages"] = int(conf["max_pages"])

    config = replace(config, **overrides)
    logger.info("Crawler config: %s", config)
    run_crawler(config)

    if not os.path.exists(raw_path):
        raise FileNotFoundError(f"Crawler did not produce CSV at {raw_path}")

    return {
        "raw_csv_path": raw_path,
        "processed_csv_path": processed_path,
        "compartment": config.compartment,
    }


def transform_task(**context) -> dict:
    paths = context["ti"].xcom_pull(task_ids="crawl_books_site") or {}
    result = transform_raw_csv(
        raw_path=paths.get("raw_csv_path", RAW_CSV_PATH),
        output_path=paths.get("processed_csv_path", PROCESSED_CSV_PATH),
        default_compartment=paths.get("compartment", "default"),
    )
    logger.info("Transform result: %s", result)
    return result


def load_task(**context) -> dict:
    transform_result = context["ti"].xcom_pull(task_ids="transform_csv") or {}
    result = load_into_postgres(
        csv_path=transform_result.get("output_path", PROCESSED_CSV_PATH),
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

    load = PythonOperator(
        task_id="load_postgres",
        python_callable=load_task,
        execution_timeout=timedelta(minutes=5),
    )

    crawl >> transform >> load
