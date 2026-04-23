from __future__ import annotations

import os
from datetime import datetime, timezone

import pandas as pd
import requests
from nicegui import ui
from requests.auth import HTTPBasicAuth

DAG_ID = os.getenv("AIRFLOW_DAG_ID", "crawler_csv_to_postgres")
AIRFLOW_BASE_URL = os.getenv("AIRFLOW_BASE_URL", "http://airflow-webserver:8080")
AIRFLOW_USERNAME = os.getenv("AIRFLOW_USERNAME", "admin")
AIRFLOW_PASSWORD = os.getenv("AIRFLOW_PASSWORD", "admin")
SUPERSET_DASHBOARD_URL = os.getenv(
    "SUPERSET_DASHBOARD_URL",
    "http://localhost:8088/superset/dashboard/crawler-analytics/",
)


class AirflowClient:
    def __init__(self) -> None:
        self.auth = HTTPBasicAuth(AIRFLOW_USERNAME, AIRFLOW_PASSWORD)

    def _request(self, method: str, path: str, **kwargs) -> requests.Response:
        url = f"{AIRFLOW_BASE_URL.rstrip('/')}{path}"
        return requests.request(method, url, auth=self.auth, timeout=20, **kwargs)

    def dag_details(self) -> dict:
        response = self._request("GET", f"/api/v1/dags/{DAG_ID}/details")
        response.raise_for_status()
        return response.json()

    def recent_runs(self, limit: int = 30) -> list[dict]:
        response = self._request("GET", f"/api/v1/dags/{DAG_ID}/dagRuns?limit={limit}")
        response.raise_for_status()
        return response.json().get("dag_runs", [])

    def set_paused(self, paused: bool) -> None:
        response = self._request("PATCH", f"/api/v1/dags/{DAG_ID}", json={"is_paused": paused})
        response.raise_for_status()

    def trigger_run(self, conf: dict) -> dict:
        response = self._request("POST", f"/api/v1/dags/{DAG_ID}/dagRuns", json={"conf": conf})
        response.raise_for_status()
        return response.json()


def format_time(value: str | None) -> str:
    if not value:
        return "-"
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return parsed.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    except ValueError:
        return value


def parse_list_input(raw: str) -> list[str]:
    return [item.strip() for line in raw.splitlines() for item in line.split(",") if item.strip()]


def state_counts(runs: list[dict]) -> dict[str, int]:
    counts = {"success": 0, "running": 0, "queued": 0, "failed": 0}
    for run in runs:
        state = str(run.get("state") or "").lower()
        if state in counts:
            counts[state] += 1
    return counts


def state_chip(state: str) -> str:
    norm = state.lower()
    if norm in {"success", "running"}:
        cls = "chip-ok"
    elif norm in {"failed", "upstream_failed"}:
        cls = "chip-bad"
    else:
        cls = "chip-pending"
    return f"<span class='state-chip {cls}'>{norm or 'unknown'}</span>"


def build_css() -> None:
    ui.add_head_html(
        """
<style>
body { background: radial-gradient(1000px 500px at -5% -20%, rgba(56,189,248,.15), transparent 60%), radial-gradient(900px 500px at 110% -10%, rgba(20,184,166,.15), transparent 60%), #f3f7fc !important; }
.suite-hero { background: linear-gradient(125deg, #0f172a 0%, #123054 56%, #0b678a 100%); border-radius: 18px; color: #f8fafc; box-shadow: 0 22px 44px rgba(15,23,42,.24); }
.brand-chip { border: 1px solid #71d6ff; background: rgba(13,49,80,.85); color: #d8f5ff; border-radius: 999px; padding: 2px 10px; font-size: 12px; }
.suite-card { background: #fff; border: 1px solid #d8e3f1; border-radius: 14px; box-shadow: 0 10px 24px rgba(2,12,27,.07); }
.stat-card { background: linear-gradient(180deg, #fff 0%, #f8fbff 100%); border: 1px solid #d8e3f1; border-radius: 12px; }
.state-chip { border-radius: 999px; padding: 2px 10px; font-size: 12px; font-weight: 700; border: 1px solid transparent; }
.chip-ok { color: #166534; background: #dcfce7; border-color: #86efac; }
.chip-bad { color: #991b1b; background: #fee2e2; border-color: #fca5a5; }
.chip-pending { color: #92400e; background: #ffedd5; border-color: #fdba74; }
</style>
        """
    )


def app_ui() -> None:
    build_css()
    client = AirflowClient()

    ui.colors(primary="#0ea5e9", secondary="#14b8a6", accent="#0f172a")

    with ui.left_drawer(value=True, bordered=True).classes("bg-slate-50"):
        ui.label("Crawler Control Suite").classes("text-lg font-bold")
        ui.label("Single entry point for crawling + orchestration + analytics").classes("text-sm text-slate-600")
        ui.separator()
        ui.link("Airflow", "http://localhost:8080", new_tab=True)
        ui.link("Superset", SUPERSET_DASHBOARD_URL, new_tab=True)
        ui.separator()
        ui.label("Credentials: admin / admin").classes("text-sm")

    with ui.column().classes("w-full max-w-[1320px] mx-auto gap-4 p-4"):
        with ui.card().classes("suite-hero w-full p-4"):
            ui.html("<span class='brand-chip'>Crawler Control Suite</span>")
            ui.label("Crawler Control Center").classes("text-2xl font-bold mt-2")
            ui.label("NiceGUI edition: premium control surface for run intake, schedule visibility, and data pipeline handoff.").classes("text-base text-cyan-100")

        stat_labels: dict[str, ui.label] = {}
        with ui.row().classes("w-full gap-3"):
            for key in ["DAG", "Paused", "Frequency", "Next Run", "Recent Success"]:
                with ui.card().classes("stat-card p-3 min-w-[180px] grow"):
                    ui.label(key).classes("text-xs uppercase tracking-wide text-slate-500")
                    stat_labels[key] = ui.label("-").classes("text-xl font-bold text-slate-800")

        with ui.row().classes("w-full gap-4 items-start"):
            with ui.card().classes("suite-card p-4 grow"):
                ui.label("Create Crawl Run").classes("text-lg font-bold")
                use_env_defaults = ui.checkbox("Use only .env defaults for this run", value=False)
                urls_input = ui.textarea("Website URLs (comma or newline separated)", value="https://books.toscrape.com/").props("autogrow")
                keywords_input = ui.input("Search Keywords (comma separated)", value="")
                with ui.row().classes("w-full gap-3"):
                    compartment_input = ui.input("Compartment / Purpose", value="default").classes("grow")
                    max_pages_input = ui.number("Max pages per run", value=10, min=1, max=200, step=1).classes("w-40")
                schema_path_input = ui.input(
                    "Crawler schema/source config path",
                    value="/opt/platform/crawler/schemas/default_schema.yml",
                ).classes("w-full")

                def sync_disabled() -> None:
                    disabled = use_env_defaults.value
                    urls_input.set_enabled(not disabled)
                    keywords_input.set_enabled(not disabled)
                    max_pages_input.set_enabled(not disabled)

                use_env_defaults.on_value_change(lambda _: sync_disabled())
                sync_disabled()

                def on_pause() -> None:
                    try:
                        client.set_paused(True)
                        ui.notify("DAG paused", color="warning")
                        refresh_data()
                    except Exception as exc:  # noqa: BLE001
                        ui.notify(f"Pause failed: {exc}", color="negative")

                def on_unpause() -> None:
                    try:
                        client.set_paused(False)
                        ui.notify("DAG unpaused", color="positive")
                        refresh_data()
                    except Exception as exc:  # noqa: BLE001
                        ui.notify(f"Unpause failed: {exc}", color="negative")

                def on_trigger() -> None:
                    try:
                        start_urls = [] if use_env_defaults.value else parse_list_input(str(urls_input.value or ""))
                        if not use_env_defaults.value and not start_urls:
                            ui.notify("Provide at least one URL or use .env defaults", color="warning")
                            return

                        keywords = []
                        if not use_env_defaults.value:
                            keywords = [k.strip() for k in str(keywords_input.value or "").split(",") if k.strip()]

                        conf = {
                            "start_urls": start_urls,
                            "keywords": keywords,
                            "max_pages": None if use_env_defaults.value else int(max_pages_input.value or 10),
                            "compartment": str(compartment_input.value or "default").strip() or "default",
                            "source_config_path": str(schema_path_input.value or "").strip(),
                        }

                        run = client.trigger_run(conf)
                        ui.notify(f"Triggered: {run.get('dag_run_id')}", color="positive")
                        refresh_data()
                    except Exception as exc:  # noqa: BLE001
                        ui.notify(f"Trigger failed: {exc}", color="negative")

                with ui.row().classes("w-full gap-2"):
                    ui.button("Trigger Crawl Run", on_click=on_trigger).props("color=primary unelevated")
                    ui.button("Unpause DAG", on_click=on_unpause).props("outline color=secondary")
                    ui.button("Pause DAG", on_click=on_pause).props("outline color=warning")

            with ui.card().classes("suite-card p-4 w-[430px] max-w-full"):
                ui.label("Run Health").classes("text-lg font-bold")
                pie_chart = ui.echart(
                    {
                        "tooltip": {"trigger": "item"},
                        "series": [
                            {
                                "type": "pie",
                                "radius": ["50%", "74%"],
                                "label": {"show": True, "formatter": "{b}: {c}"},
                                "data": [],
                            }
                        ],
                    }
                ).classes("w-full h-64")
                latest_run_label = ui.html("<div class='text-slate-700 text-sm'>No runs yet</div>")

        with ui.card().classes("suite-card p-4 w-full"):
            ui.label("Recent Runs").classes("text-lg font-bold")
            runs_table = ui.table(
                columns=[
                    {"name": "run_id", "label": "Run ID", "field": "run_id", "align": "left"},
                    {"name": "state", "label": "State", "field": "state", "align": "left"},
                    {"name": "type", "label": "Type", "field": "type", "align": "left"},
                    {"name": "start", "label": "Start", "field": "start", "align": "left"},
                    {"name": "end", "label": "End", "field": "end", "align": "left"},
                    {"name": "compartment", "label": "Compartment", "field": "compartment", "align": "left"},
                    {"name": "urls", "label": "URLs", "field": "urls", "align": "left"},
                    {"name": "schema", "label": "Schema Path", "field": "schema", "align": "left"},
                ],
                rows=[],
                row_key="run_id",
                pagination=10,
            ).props("flat bordered wrap-cells")

        def refresh_data() -> None:
            try:
                details = client.dag_details()
                runs = client.recent_runs(limit=30)

                stat_labels["DAG"].set_text(str(details.get("dag_id", DAG_ID)))
                stat_labels["Paused"].set_text("Yes" if details.get("is_paused") else "No")
                stat_labels["Frequency"].set_text(str((details.get("schedule_interval") or {}).get("value", "n/a")))
                stat_labels["Next Run"].set_text(format_time(details.get("next_dagrun")))
                stat_labels["Recent Success"].set_text(str(sum(1 for r in runs if (r.get("state") or "").lower() == "success")))

                counts = state_counts(runs)
                pie_chart.options["series"][0]["data"] = [
                    {"name": "success", "value": counts["success"], "itemStyle": {"color": "#16a34a"}},
                    {"name": "running", "value": counts["running"], "itemStyle": {"color": "#0ea5e9"}},
                    {"name": "queued", "value": counts["queued"], "itemStyle": {"color": "#f59e0b"}},
                    {"name": "failed", "value": counts["failed"], "itemStyle": {"color": "#dc2626"}},
                ]
                pie_chart.update()

                rows = []
                for run in runs:
                    conf = run.get("conf") or {}
                    rows.append(
                        {
                            "run_id": run.get("dag_run_id"),
                            "state": str(run.get("state") or "").lower(),
                            "type": run.get("run_type"),
                            "start": format_time(run.get("start_date")),
                            "end": format_time(run.get("end_date")),
                            "compartment": conf.get("compartment", "default"),
                            "urls": ", ".join(conf.get("start_urls") or []),
                            "schema": conf.get("source_config_path", "(env/default)"),
                        }
                    )

                runs_table.rows = rows
                runs_table.update()

                if runs:
                    latest = runs[0]
                    latest_run_label.set_content(
                        f"<div class='text-sm'><b>Latest Run</b><br>"
                        f"Run ID: <code>{latest.get('dag_run_id','-')}</code><br>"
                        f"State: {state_chip(str(latest.get('state') or 'unknown'))}<br>"
                        f"Start: <code>{format_time(latest.get('start_date'))}</code><br>"
                        f"End: <code>{format_time(latest.get('end_date'))}</code></div>"
                    )
                else:
                    latest_run_label.set_content("<div class='text-slate-700 text-sm'>No runs yet</div>")
            except Exception as exc:  # noqa: BLE001
                ui.notify(f"Refresh failed: {exc}", color="negative")

        refresh_data()
        ui.timer(interval=15.0, callback=refresh_data)


app_ui()
ui.run(host="0.0.0.0", port=8501, title="Crawler Control Suite")
