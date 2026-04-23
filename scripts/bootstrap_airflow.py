from __future__ import annotations

import base64
import json
import os
import time
from typing import Any
from urllib import error, parse, request

AIRFLOW_BASE_URL = os.getenv("AIRFLOW_BASE_URL", "http://airflow-webserver:8080").rstrip("/")
AIRFLOW_USERNAME = os.getenv("AIRFLOW_USERNAME", "admin")
AIRFLOW_PASSWORD = os.getenv("AIRFLOW_PASSWORD", "admin")
ORCHESTRATOR_BASE_URL = os.getenv("ORCHESTRATOR_BASE_URL", "http://temporal-orchestrator:8090").rstrip("/")
DAG_ID = os.getenv("AIRFLOW_DAG_ID", "crawler_csv_to_postgres")
BOOTSTRAP_ENABLED = os.getenv("AIRFLOW_BOOTSTRAP_ON_STARTUP", "true").strip().lower() in {"1", "true", "yes", "y"}
BOOTSTRAP_TIMEOUT_SECONDS = int(os.getenv("AIRFLOW_BOOTSTRAP_TIMEOUT_SECONDS", "300"))


def _auth_header() -> str:
    token = base64.b64encode(f"{AIRFLOW_USERNAME}:{AIRFLOW_PASSWORD}".encode("utf-8")).decode("utf-8")
    return f"Basic {token}"


def _airflow_request(method: str, path: str, payload: dict[str, Any] | None = None) -> tuple[int, dict[str, Any] | None, str]:
    body = None
    headers = {
        "Authorization": _auth_header(),
        "Accept": "application/json",
    }
    if payload is not None:
        body = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"

    req = request.Request(f"{AIRFLOW_BASE_URL}{path}", data=body, method=method, headers=headers)
    try:
        with request.urlopen(req, timeout=20) as resp:
            raw = resp.read().decode("utf-8")
            data = json.loads(raw) if raw else None
            return resp.getcode(), data, raw
    except error.HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="ignore")
        data = None
        try:
            data = json.loads(raw)
        except Exception:
            pass
        return exc.code, data, raw
    except error.URLError as exc:
        return 503, None, str(exc)


def _orchestrator_request(method: str, path: str, payload: dict[str, Any] | None = None) -> tuple[int, dict[str, Any] | None, str]:
    body = None
    headers = {"Accept": "application/json"}
    if payload is not None:
        body = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"

    req = request.Request(f"{ORCHESTRATOR_BASE_URL}{path}", data=body, method=method, headers=headers)
    try:
        with request.urlopen(req, timeout=25) as resp:
            raw = resp.read().decode("utf-8")
            data = json.loads(raw) if raw else None
            return resp.getcode(), data, raw
    except error.HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="ignore")
        data = None
        try:
            data = json.loads(raw)
        except Exception:
            pass
        return exc.code, data, raw
    except error.URLError as exc:
        return 503, None, str(exc)


def _wait_for_airflow() -> bool:
    deadline = time.time() + BOOTSTRAP_TIMEOUT_SECONDS
    while time.time() < deadline:
        code, _, _ = _airflow_request("GET", "/health")
        if code == 200:
            print("Airflow health endpoint is ready")
            return True
        time.sleep(3)
    return False


def _wait_for_orchestrator() -> bool:
    deadline = time.time() + BOOTSTRAP_TIMEOUT_SECONDS
    while time.time() < deadline:
        code, _, _ = _orchestrator_request("GET", "/health")
        if code == 200:
            print("Temporal orchestrator endpoint is ready")
            return True
        time.sleep(3)
    return False


def main() -> int:
    if not BOOTSTRAP_ENABLED:
        print("Bootstrap disabled via AIRFLOW_BOOTSTRAP_ON_STARTUP")
        return 0

    if not _wait_for_airflow():
        print("Timed out waiting for Airflow health")
        return 1

    if not _wait_for_orchestrator():
        print("Timed out waiting for temporal orchestrator health")
        return 1

    code, _, raw = _airflow_request("GET", f"/api/v1/dags/{parse.quote(DAG_ID)}")
    if code != 200:
        print(f"Cannot load DAG {DAG_ID}: HTTP {code} {raw}")
        return 1

    _airflow_request("PATCH", f"/api/v1/dags/{parse.quote(DAG_ID)}", {"is_paused": False})

    code, dag_runs, raw = _airflow_request("GET", f"/api/v1/dags/{parse.quote(DAG_ID)}/dagRuns?limit=1")
    if code != 200:
        print(f"Cannot list DAG runs: HTTP {code} {raw}")
        return 1

    total_entries = int((dag_runs or {}).get("total_entries", 0))
    if total_entries > 0:
        print(f"DAG already has {total_entries} run(s); skipping bootstrap trigger")
        return 0

    conf = {
        "start_urls": [
            value.strip()
            for value in os.getenv("CRAWLER_START_URLS", "https://books.toscrape.com/").split(",")
            if value.strip()
        ],
        "keywords": [
            value.strip() for value in os.getenv("CRAWLER_KEYWORDS", "").split(",") if value.strip()
        ],
        "max_pages": int(os.getenv("CRAWLER_MAX_PAGES", "2")),
        "compartment": os.getenv("CRAWLER_COMPARTMENT", "bootstrap").strip() or "bootstrap",
        "source_config_path": os.getenv(
            "CRAWLER_SOURCE_CONFIG", "/opt/platform/crawler/schemas/default_schema.yml"
        ),
    }

    payload = {
        "dag_id": DAG_ID,
        "conf": conf,
        "wait_for_completion": False,
    }
    code, data, raw = _orchestrator_request("POST", "/workflows/crawl/trigger", payload)
    if code not in {200, 201}:
        print(f"Failed to trigger bootstrap workflow via temporal orchestrator: HTTP {code} {raw}")
        return 1

    print(
        "Bootstrap workflow triggered successfully "
        f"(workflow_id={data.get('workflow_id')}, run_id={data.get('run_id')})"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
