from __future__ import annotations

import os
from datetime import datetime, timezone

import pandas as pd
import requests
import streamlit as st
from requests.auth import HTTPBasicAuth

DAG_ID = os.getenv("AIRFLOW_DAG_ID", "crawler_csv_to_postgres")
AIRFLOW_BASE_URL = os.getenv("AIRFLOW_BASE_URL", "http://airflow-webserver:8080")
AIRFLOW_USERNAME = os.getenv("AIRFLOW_USERNAME", "admin")
AIRFLOW_PASSWORD = os.getenv("AIRFLOW_PASSWORD", "admin")
SUPERSET_DASHBOARD_URL = os.getenv(
    "SUPERSET_DASHBOARD_URL",
    "http://localhost:8088/superset/dashboard/crawler-analytics/",
)


def _airflow_request(method: str, path: str, **kwargs) -> requests.Response:
    url = f"{AIRFLOW_BASE_URL.rstrip('/')}{path}"
    timeout = kwargs.pop("timeout", 20)
    return requests.request(
        method,
        url,
        auth=HTTPBasicAuth(AIRFLOW_USERNAME, AIRFLOW_PASSWORD),
        timeout=timeout,
        **kwargs,
    )


def get_dag_details() -> dict:
    response = _airflow_request("GET", f"/api/v1/dags/{DAG_ID}/details")
    response.raise_for_status()
    return response.json()


def get_recent_runs(limit: int = 20) -> list[dict]:
    response = _airflow_request("GET", f"/api/v1/dags/{DAG_ID}/dagRuns?limit={limit}")
    response.raise_for_status()
    return response.json().get("dag_runs", [])


def set_pause_state(is_paused: bool) -> None:
    response = _airflow_request("PATCH", f"/api/v1/dags/{DAG_ID}", json={"is_paused": is_paused})
    response.raise_for_status()


def trigger_run(
    start_urls: list[str],
    keywords: list[str],
    max_pages: int | None,
    compartment: str,
    source_config_path: str,
) -> dict:
    payload = {
        "conf": {
            "start_urls": start_urls,
            "keywords": keywords,
            "max_pages": max_pages,
            "compartment": compartment,
            "source_config_path": source_config_path,
        }
    }
    response = _airflow_request("POST", f"/api/v1/dags/{DAG_ID}/dagRuns", json=payload)
    response.raise_for_status()
    return response.json()


def _format_time(value: str | None) -> str:
    if not value:
        return "-"
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return parsed.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    except ValueError:
        return value


def _parse_list_input(value: str) -> list[str]:
    return [item.strip() for line in value.splitlines() for item in line.split(",") if item.strip()]


st.set_page_config(page_title="Crawler Control Center", layout="wide")
st.title("Crawler Control Center")
st.caption("Single entry point for crawl input, schema config, scheduling, and pipeline status")

with st.sidebar:
    st.subheader("Quick Links")
    st.markdown("- Airflow: [http://localhost:8080](http://localhost:8080)")
    st.markdown(f"- Superset Dashboard: [{SUPERSET_DASHBOARD_URL}]({SUPERSET_DASHBOARD_URL})")
    st.markdown("- Credentials: admin / admin")

try:
    details = get_dag_details()
except Exception as exc:  # noqa: BLE001
    st.error(f"Unable to reach Airflow API: {exc}")
    st.stop()

col_a, col_b, col_c, col_d = st.columns(4)
col_a.metric("DAG", details.get("dag_id", DAG_ID))
col_b.metric("Paused", "Yes" if details.get("is_paused") else "No")
col_c.metric("Frequency", (details.get("schedule_interval") or {}).get("value", "n/a"))
col_d.metric("Next Run", _format_time(details.get("next_dagrun")))

st.caption(
    f"Schedule description: {details.get('timetable_description', 'n/a')} | "
    f"Catchup: {details.get('catchup')} | Max active runs: {details.get('max_active_runs')}"
)

controls_col1, controls_col2 = st.columns([1, 1])
if controls_col1.button("Unpause DAG", use_container_width=True):
    set_pause_state(False)
    st.success("DAG unpaused")
    st.rerun()
if controls_col2.button("Pause DAG", use_container_width=True):
    set_pause_state(True)
    st.warning("DAG paused")
    st.rerun()

st.divider()
st.subheader("Create Crawl Run")

with st.form("crawl_form"):
    use_env_defaults = st.checkbox(
        "Use only .env defaults for this run",
        value=False,
        help="If checked, runtime URL/keyword/page fields are ignored and Airflow uses compose/.env defaults.",
    )
    urls_input = st.text_area(
        "Website URLs (comma or newline separated)",
        value="https://books.toscrape.com/",
        help="Each URL becomes a crawl seed URL.",
        disabled=use_env_defaults,
    )
    keywords_input = st.text_input(
        "Search Keywords (comma separated)",
        value="",
        help="Optional keyword filter, usually against title/name-like fields.",
        disabled=use_env_defaults,
    )
    compartment_input = st.text_input(
        "Compartment / Purpose",
        value="default",
        help="Logical partition label: pricing_watch, seo_research, competitor_scan.",
    )
    max_pages = st.number_input(
        "Max pages per run",
        min_value=1,
        max_value=200,
        value=10,
        step=1,
        disabled=use_env_defaults,
    )
    source_config_path = st.text_input(
        "Crawler schema/source config path",
        value="/opt/platform/crawler/schemas/default_schema.yml",
        help="Path inside Airflow containers. Keep default for books demo or point to your own YAML schema.",
    )
    submitted = st.form_submit_button("Trigger Crawl Run", use_container_width=True)

if submitted:
    start_urls = [] if use_env_defaults else _parse_list_input(urls_input)
    keywords = [] if use_env_defaults else [item.strip() for item in keywords_input.split(",") if item.strip()]
    compartment = compartment_input.strip() or "default"

    if not use_env_defaults and not start_urls:
        st.error("Please provide at least one URL or check 'Use only .env defaults'.")
    else:
        try:
            run = trigger_run(
                start_urls=start_urls,
                keywords=keywords,
                max_pages=(None if use_env_defaults else int(max_pages)),
                compartment=compartment,
                source_config_path=source_config_path.strip(),
            )
            st.success(f"Triggered DAG run: {run.get('dag_run_id')}")
            st.json(run)
        except Exception as exc:  # noqa: BLE001
            st.error(f"Failed to trigger run: {exc}")

st.divider()
st.subheader("Recent Runs")

runs = get_recent_runs(limit=30)
if runs:
    rows = []
    for run in runs:
        conf = run.get("conf") or {}
        rows.append(
            {
                "run_id": run.get("dag_run_id"),
                "state": run.get("state"),
                "type": run.get("run_type"),
                "start": _format_time(run.get("start_date")),
                "end": _format_time(run.get("end_date")),
                "start_urls": ", ".join(conf.get("start_urls") or []),
                "keywords": ", ".join(conf.get("keywords") or []),
                "max_pages": conf.get("max_pages"),
                "compartment": conf.get("compartment", "default"),
                "source_config_path": conf.get("source_config_path", "(env/default)"),
            }
        )
    frame = pd.DataFrame(rows)
    st.dataframe(frame, use_container_width=True, hide_index=True)
else:
    st.info("No DAG runs found yet")

with st.expander("How It Works"):
    st.markdown(
        """
1. Submit URL list and a source schema path (or use `.env` defaults).
2. UI triggers Airflow DAG `crawler_csv_to_postgres` via Airflow REST API.
3. DAG runs `crawl -> transform -> ai_enrich -> load`.
4. Records load into generic warehouse table `scraped_records` (all datasets).
5. If records have book-compatible fields, they also project into `scraped_books`.
6. Superset reads warehouse tables and dashboards update from Postgres.
7. Frequency is controlled by DAG schedule (`@daily` by default).
        """.strip()
    )
