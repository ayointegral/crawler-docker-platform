from __future__ import annotations

import asyncio
import contextlib
import os
import re
import uuid
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

import requests
from fastapi import FastAPI, HTTPException, Response
from pydantic import BaseModel, Field
from requests.auth import HTTPBasicAuth
from temporalio import activity, workflow
from temporalio.client import Client, WorkflowHandle
from temporalio.common import WorkflowIDReusePolicy
from temporalio.worker import Worker

AIRFLOW_BASE_URL = os.getenv("AIRFLOW_BASE_URL", "http://airflow-webserver:8080").rstrip("/")
AIRFLOW_USERNAME = os.getenv("AIRFLOW_USERNAME", "admin")
AIRFLOW_PASSWORD = os.getenv("AIRFLOW_PASSWORD", "admin")
AIRFLOW_DAG_ID = os.getenv("AIRFLOW_DAG_ID", "crawler_csv_to_postgres")

TEMPORAL_SERVER = os.getenv("TEMPORAL_SERVER", "temporal:7233")
TEMPORAL_NAMESPACE = os.getenv("TEMPORAL_NAMESPACE", "default")
TEMPORAL_TASK_QUEUE = os.getenv("TEMPORAL_TASK_QUEUE", "crawler-task-queue")


@dataclass
class AirflowApi:
    base_url: str
    username: str
    password: str

    def request(self, method: str, path: str, json_body: dict[str, Any] | None = None) -> dict[str, Any]:
        response = requests.request(
            method,
            f"{self.base_url}{path}",
            auth=HTTPBasicAuth(self.username, self.password),
            json=json_body,
            timeout=20,
        )
        response.raise_for_status()
        if not response.text:
            return {}
        return response.json()


api = AirflowApi(AIRFLOW_BASE_URL, AIRFLOW_USERNAME, AIRFLOW_PASSWORD)


class TriggerRequest(BaseModel):
    dag_id: str = Field(default=AIRFLOW_DAG_ID)
    conf: dict[str, Any] = Field(default_factory=dict)
    wait_for_completion: bool = Field(default=False)


@activity.defn
async def unpause_dag(dag_id: str) -> dict[str, Any]:
    return await asyncio.to_thread(api.request, "PATCH", f"/api/v1/dags/{dag_id}", {"is_paused": False})


@activity.defn
async def trigger_dag_run(dag_id: str, conf: dict[str, Any]) -> str:
    payload = {"conf": conf}
    response = await asyncio.to_thread(api.request, "POST", f"/api/v1/dags/{dag_id}/dagRuns", payload)
    dag_run_id = response.get("dag_run_id")
    if not dag_run_id:
        raise RuntimeError(f"Airflow trigger returned no dag_run_id: {response}")
    return str(dag_run_id)


@activity.defn
async def wait_for_dag_state(dag_id: str, dag_run_id: str, timeout_seconds: int = 1800) -> dict[str, Any]:
    deadline = asyncio.get_running_loop().time() + timeout_seconds
    while True:
        run = await asyncio.to_thread(api.request, "GET", f"/api/v1/dags/{dag_id}/dagRuns/{dag_run_id}")
        state = str(run.get("state") or "").lower()
        if state in {"success", "failed"}:
            return run
        if asyncio.get_running_loop().time() > deadline:
            raise TimeoutError(f"Timed out waiting for dag run {dag_run_id}")
        await asyncio.sleep(5)


@workflow.defn(sandboxed=False)
class CrawlPipelineWorkflow:
    @workflow.run
    async def run(self, dag_id: str, conf: dict[str, Any], wait_for_completion: bool = False) -> dict[str, Any]:
        await workflow.execute_activity(
            unpause_dag,
            dag_id,
            schedule_to_close_timeout=timedelta(minutes=2),
        )

        dag_run_id = await workflow.execute_activity(
            trigger_dag_run,
            args=[dag_id, conf],
            schedule_to_close_timeout=timedelta(minutes=3),
        )

        result: dict[str, Any] = {
            "dag_id": dag_id,
            "dag_run_id": dag_run_id,
            "state": "triggered",
        }

        if wait_for_completion:
            run = await workflow.execute_activity(
                wait_for_dag_state,
                args=[dag_id, dag_run_id, 1800],
                schedule_to_close_timeout=timedelta(minutes=31),
            )
            result["state"] = str(run.get("state") or "unknown")
            result["run"] = run

        return result


class TemporalRuntime:
    def __init__(self) -> None:
        self.client: Client | None = None
        self.worker_task: asyncio.Task | None = None
        self.worker: Worker | None = None
        self.connect_task: asyncio.Task | None = None
        self.ready = False
        self.last_error: str | None = None

    async def _connect_loop(self) -> None:
        while True:
            try:
                self.client = await Client.connect(TEMPORAL_SERVER, namespace=TEMPORAL_NAMESPACE)
                self.worker = Worker(
                    self.client,
                    task_queue=TEMPORAL_TASK_QUEUE,
                    workflows=[CrawlPipelineWorkflow],
                    activities=[unpause_dag, trigger_dag_run, wait_for_dag_state],
                )
                self.ready = True
                self.last_error = None
                self.worker_task = asyncio.create_task(self.worker.run())
                await self.worker_task
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                self.ready = False
                self.client = None
                self.worker = None
                self.worker_task = None
                self.last_error = str(exc)
                await asyncio.sleep(3)

    async def start(self) -> None:
        self.connect_task = asyncio.create_task(self._connect_loop())

    async def shutdown(self) -> None:
        if self.connect_task:
            self.connect_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self.connect_task
        if self.worker_task:
            self.worker_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self.worker_task


runtime = TemporalRuntime()
app = FastAPI(title="Temporal Orchestrator", version="1.0.0")


@app.on_event("startup")
async def startup_event() -> None:
    await runtime.start()


@app.on_event("shutdown")
async def shutdown_event() -> None:
    await runtime.shutdown()


@app.get("/health")
async def health(response: Response) -> dict[str, Any]:
    if not runtime.ready:
        response.status_code = 503
    return {
        "status": "ok" if runtime.ready else "starting",
        "ready": runtime.ready,
        "last_error": runtime.last_error,
        "temporal_server": TEMPORAL_SERVER,
        "task_queue": TEMPORAL_TASK_QUEUE,
        "airflow_base_url": AIRFLOW_BASE_URL,
    }


def _safe_workflow_id(prefix: str = "crawl") -> str:
    return f"{prefix}-{uuid.uuid4().hex[:16]}"


@app.post("/workflows/crawl/trigger")
async def trigger_workflow(payload: TriggerRequest) -> dict[str, Any]:
    if runtime.client is None:
        raise HTTPException(status_code=503, detail="Temporal client not ready")

    dag_id = payload.dag_id.strip() or AIRFLOW_DAG_ID
    if not re.fullmatch(r"[A-Za-z0-9_\-.]+", dag_id):
        raise HTTPException(status_code=400, detail="Invalid dag_id")

    workflow_id = _safe_workflow_id("crawl")
    handle: WorkflowHandle = await runtime.client.start_workflow(
        CrawlPipelineWorkflow.run,
        args=[dag_id, payload.conf, payload.wait_for_completion],
        id=workflow_id,
        task_queue=TEMPORAL_TASK_QUEUE,
        id_reuse_policy=WorkflowIDReusePolicy.ALLOW_DUPLICATE,
    )

    response: dict[str, Any] = {
        "workflow_id": workflow_id,
        "run_id": handle.first_execution_run_id,
        "task_queue": TEMPORAL_TASK_QUEUE,
        "dag_id": dag_id,
    }

    if payload.wait_for_completion:
        try:
            result = await handle.result()
            response["result"] = result
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(status_code=500, detail=f"Workflow failed: {exc}") from exc

    return response


@app.get("/workflows/{workflow_id}")
async def workflow_status(workflow_id: str) -> dict[str, Any]:
    if runtime.client is None:
        raise HTTPException(status_code=503, detail="Temporal client not ready")

    try:
        handle = runtime.client.get_workflow_handle(workflow_id)
        description = await handle.describe()
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=404, detail=f"Workflow not found: {exc}") from exc

    raw_status = getattr(description, "status", None)
    status = raw_status.name if hasattr(raw_status, "name") else str(raw_status)
    execution = getattr(description, "execution", None)
    run_id = getattr(description, "run_id", None) or getattr(execution, "run_id", None)
    workflow_type = getattr(description, "workflow_type", None)
    start_time = getattr(description, "start_time", None)
    return {
        "workflow_id": workflow_id,
        "run_id": run_id,
        "status": status,
        "task_queue": description.task_queue,
        "workflow_type": str(workflow_type) if workflow_type is not None else None,
        "start_time": str(start_time) if start_time is not None else None,
    }
