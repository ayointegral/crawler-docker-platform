from __future__ import annotations

import os
from datetime import datetime, timezone

import pandas as pd
import plotly.express as px
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


def _custom_css(theme_name: str) -> str:
    if theme_name == "Dark":
        return """
<style>
:root {
  --bg: #0b1220;
  --ink: #e2e8f0;
  --muted: #93a3b8;
  --panel: #111c31;
  --line: #203250;
  --chip: #0f1a2d;
}

.stApp {
  background:
    radial-gradient(1000px 560px at -5% -20%, rgba(45, 212, 191, 0.16), transparent 60%),
    radial-gradient(900px 560px at 110% -10%, rgba(56, 189, 248, 0.16), transparent 60%),
    var(--bg);
  color: var(--ink);
}

.block-container { max-width: 1320px; padding-top: 1.4rem; padding-bottom: 1.4rem; }
.hero {
  background: linear-gradient(125deg, #08101d 0%, #0f2542 52%, #0c4767 100%);
  border: 1px solid rgba(148, 163, 184, 0.25);
  border-radius: 20px;
  padding: 1.35rem 1.5rem;
  color: #f8fafc;
  box-shadow: 0 24px 48px rgba(0, 0, 0, 0.28);
}
.hero h1 { margin: 0; font-size: 1.6rem; }
.hero p { margin: 0.35rem 0 0 0; color: #cde2ff; }
.brand {
  display: inline-block;
  border-radius: 999px;
  padding: 0.22rem 0.65rem;
  font-size: 0.78rem;
  border: 1px solid #2f597d;
  background: #0d223a;
  color: #8ce7ff;
  margin-bottom: 0.55rem;
}
.panel {
  background: var(--panel);
  border: 1px solid var(--line);
  border-radius: 16px;
  padding: 1rem;
  box-shadow: 0 12px 28px rgba(0,0,0,0.22);
}
.kpi {
  background: linear-gradient(180deg, #12233d 0%, #0f1d33 100%);
  border: 1px solid #2a3f60;
  border-radius: 14px;
  padding: 0.8rem 0.95rem;
}
.kpi .label { font-size: 0.75rem; color: var(--muted); text-transform: uppercase; letter-spacing: 0.05em; }
.kpi .value { margin-top: 0.25rem; font-size: 1.18rem; font-weight: 700; color: var(--ink); }
.badge { display: inline-block; border-radius: 999px; padding: 0.18rem 0.55rem; font-size: 0.74rem; font-weight: 700; border: 1px solid transparent; }
.badge-ok { color: #a7f3d0; background: #123525; border-color: #1f8c55; }
.badge-bad { color: #fecaca; background: #3a1414; border-color: #991b1b; }
.badge-pending { color: #fde68a; background: #3b2a12; border-color: #a16207; }
.stButton > button { border-radius: 12px; border: 1px solid #2e5d7f; background: linear-gradient(180deg, #15334f 0%, #10263d 100%); color: #e2e8f0; font-weight: 700; }
.stTextInput > div > div > input,
.stTextArea textarea,
.stNumberInput input { border-radius: 10px !important; border: 1px solid #30445f !important; background: #0f1c31 !important; color: #e2e8f0 !important; }
[data-testid="stSidebar"] { border-right: 1px solid #243753; background: linear-gradient(180deg, #0d1628 0%, #121f36 100%); }
[data-testid="stSidebar"] .stMarkdown p, [data-testid="stSidebar"] .stMarkdown li { color: #c2d3e7; }
</style>
"""

    return """
<style>
:root {
  --bg: #f4f7fb;
  --ink: #0f172a;
  --muted: #475569;
  --panel: #ffffff;
  --line: #dbe3ef;
}

.stApp {
  background:
    radial-gradient(1000px 560px at -5% -20%, rgba(20, 184, 166, 0.14), transparent 60%),
    radial-gradient(900px 560px at 110% -10%, rgba(14, 165, 233, 0.16), transparent 60%),
    var(--bg);
  color: var(--ink);
}

.block-container { max-width: 1320px; padding-top: 1.4rem; padding-bottom: 1.4rem; }
.hero {
  background: linear-gradient(125deg, #0f172a 0%, #132a4c 52%, #0b5f83 100%);
  border: 1px solid rgba(255, 255, 255, 0.08);
  border-radius: 20px;
  padding: 1.35rem 1.5rem;
  color: #f8fafc;
  box-shadow: 0 20px 45px rgba(15, 23, 42, 0.24);
}
.hero h1 { margin: 0; font-size: 1.6rem; }
.hero p { margin: 0.35rem 0 0 0; color: #cfe4ff; }
.brand {
  display: inline-block;
  border-radius: 999px;
  padding: 0.22rem 0.65rem;
  font-size: 0.78rem;
  border: 1px solid #6ec8ef;
  background: #0f3150;
  color: #dff7ff;
  margin-bottom: 0.55rem;
}
.panel { background: var(--panel); border: 1px solid var(--line); border-radius: 16px; padding: 1rem; box-shadow: 0 10px 25px rgba(2, 12, 27, 0.06); }
.kpi { background: linear-gradient(180deg, #ffffff 0%, #f9fcff 100%); border: 1px solid #d8e3f1; border-radius: 14px; padding: 0.8rem 0.95rem; }
.kpi .label { font-size: 0.75rem; color: var(--muted); text-transform: uppercase; letter-spacing: 0.05em; }
.kpi .value { margin-top: 0.25rem; font-size: 1.18rem; font-weight: 700; color: var(--ink); }
.badge { display: inline-block; border-radius: 999px; padding: 0.18rem 0.55rem; font-size: 0.74rem; font-weight: 700; border: 1px solid transparent; }
.badge-ok { color: #166534; background: #dcfce7; border-color: #86efac; }
.badge-bad { color: #991b1b; background: #fee2e2; border-color: #fca5a5; }
.badge-pending { color: #92400e; background: #ffedd5; border-color: #fdba74; }
.stButton > button { border-radius: 12px; border: 1px solid #a6dbf4; background: linear-gradient(180deg, #ecfbff 0%, #d5f2fb 100%); color: #0f172a; font-weight: 700; }
.stTextInput > div > div > input,
.stTextArea textarea,
.stNumberInput input { border-radius: 10px !important; border: 1px solid #c7d6ea !important; background: #ffffff !important; }
[data-testid="stSidebar"] { border-right: 1px solid #d4deea; background: linear-gradient(180deg, #ffffff 0%, #f5f8fd 100%); }
[data-testid="stSidebar"] .stMarkdown p, [data-testid="stSidebar"] .stMarkdown li { color: #334155; }
</style>
"""


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


def _state_badge(state: str | None) -> str:
    state_norm = (state or "").lower()
    if state_norm in {"success", "running"}:
        css = "badge-ok"
    elif state_norm in {"failed", "upstream_failed"}:
        css = "badge-bad"
    else:
        css = "badge-pending"
    text = state_norm or "unknown"
    return f"<span class='badge {css}'>{text}</span>"


def _safe_count(runs: list[dict], state: str) -> int:
    return sum(1 for run in runs if (run.get("state") or "").lower() == state.lower())


def _runs_frame(runs: list[dict]) -> pd.DataFrame:
    rows = []
    for run in runs:
        conf = run.get("conf") or {}
        rows.append(
            {
                "run_id": run.get("dag_run_id"),
                "state": (run.get("state") or "").lower(),
                "type": run.get("run_type"),
                "start": _format_time(run.get("start_date")),
                "end": _format_time(run.get("end_date")),
                "start_dt": run.get("start_date"),
                "urls": ", ".join(conf.get("start_urls") or []),
                "keywords": ", ".join(conf.get("keywords") or []),
                "max_pages": conf.get("max_pages"),
                "compartment": conf.get("compartment", "default"),
                "schema": conf.get("source_config_path", "(env/default)"),
            }
        )
    return pd.DataFrame(rows)


st.set_page_config(page_title="Crawler Control Center", page_icon="C", layout="wide")

with st.sidebar:
    st.markdown("### Crawler DataOps Suite")
    st.caption("Control, monitor, and route data from one UI")
    theme = st.radio("Theme", ["Light", "Dark"], index=0, horizontal=True)
    st.markdown("---")
    st.markdown("### Navigation")
    st.markdown("- **Control UI**: this page")
    st.markdown("- **Airflow**: [http://localhost:8080](http://localhost:8080)")
    st.markdown(f"- **Superset**: [{SUPERSET_DASHBOARD_URL}]({SUPERSET_DASHBOARD_URL})")
    st.markdown("---")
    st.markdown("### Credentials")
    st.code("admin / admin", language="text")

st.markdown(_custom_css(theme), unsafe_allow_html=True)

try:
    details = get_dag_details()
    runs = get_recent_runs(limit=30)
except Exception as exc:  # noqa: BLE001
    st.error(f"Unable to reach Airflow API: {exc}")
    st.stop()

st.markdown(
    """
<div class="hero">
  <span class="brand">Crawler DataOps Suite</span>
  <h1>Crawler Control Center</h1>
  <p>Premium operations UI for crawl intake, orchestration visibility, and analytics handoff.</p>
</div>
""",
    unsafe_allow_html=True,
)

frequency = (details.get("schedule_interval") or {}).get("value", "n/a")
next_run = _format_time(details.get("next_dagrun"))
paused = "Yes" if details.get("is_paused") else "No"

k1, k2, k3, k4, k5 = st.columns(5)
with k1:
    st.markdown(
        f"<div class='kpi'><div class='label'>DAG</div><div class='value'>{details.get('dag_id', DAG_ID)}</div></div>",
        unsafe_allow_html=True,
    )
with k2:
    st.markdown(
        f"<div class='kpi'><div class='label'>Paused</div><div class='value'>{paused}</div></div>",
        unsafe_allow_html=True,
    )
with k3:
    st.markdown(
        f"<div class='kpi'><div class='label'>Frequency</div><div class='value'>{frequency}</div></div>",
        unsafe_allow_html=True,
    )
with k4:
    st.markdown(
        f"<div class='kpi'><div class='label'>Next Run</div><div class='value'>{next_run}</div></div>",
        unsafe_allow_html=True,
    )
with k5:
    st.markdown(
        f"<div class='kpi'><div class='label'>Recent Success</div><div class='value'>{_safe_count(runs, 'success')}</div></div>",
        unsafe_allow_html=True,
    )

st.caption(
    f"Schedule description: {details.get('timetable_description', 'n/a')} | "
    f"Catchup: {details.get('catchup')} | Max active runs: {details.get('max_active_runs')}"
)

c1, c2 = st.columns([1, 1])
if c1.button("Unpause DAG", use_container_width=True):
    set_pause_state(False)
    st.success("DAG unpaused")
    st.rerun()
if c2.button("Pause DAG", use_container_width=True):
    set_pause_state(True)
    st.warning("DAG paused")
    st.rerun()

left, right = st.columns([1.12, 0.88])

with left:
    st.markdown("<div class='panel'>", unsafe_allow_html=True)
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

        col_x, col_y = st.columns(2)
        with col_x:
            compartment_input = st.text_input(
                "Compartment / Purpose",
                value="default",
                help="Logical partition label, e.g. pricing_watch or seo_research.",
            )
        with col_y:
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
            help="Path inside Airflow containers.",
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
    st.markdown("</div>", unsafe_allow_html=True)

with right:
    st.markdown("<div class='panel'>", unsafe_allow_html=True)
    st.subheader("Run Health")

    run_state_frame = pd.DataFrame(
        {
            "state": ["success", "running", "queued", "failed"],
            "count": [
                _safe_count(runs, "success"),
                _safe_count(runs, "running"),
                _safe_count(runs, "queued"),
                _safe_count(runs, "failed"),
            ],
        }
    )

    donut = px.pie(
        run_state_frame,
        names="state",
        values="count",
        hole=0.58,
        color="state",
        color_discrete_map={
            "success": "#16a34a",
            "running": "#0ea5e9",
            "queued": "#f59e0b",
            "failed": "#dc2626",
        },
    )
    donut.update_layout(margin=dict(l=10, r=10, t=10, b=10), legend_title_text="")
    st.plotly_chart(donut, use_container_width=True, config={"displayModeBar": False})

    if runs:
        trend_df = _runs_frame(runs)
        trend_df = trend_df[trend_df["start_dt"].notna()].copy()
        if not trend_df.empty:
            trend_df["start_dt"] = pd.to_datetime(trend_df["start_dt"], utc=True, errors="coerce")
            trend_df = trend_df.dropna(subset=["start_dt"]) 
            trend_df["day"] = trend_df["start_dt"].dt.date.astype(str)
            daily = trend_df.groupby(["day", "state"], as_index=False).size()
            line = px.line(
                daily,
                x="day",
                y="size",
                color="state",
                markers=True,
                color_discrete_map={
                    "success": "#16a34a",
                    "running": "#0ea5e9",
                    "queued": "#f59e0b",
                    "failed": "#dc2626",
                },
            )
            line.update_layout(margin=dict(l=10, r=10, t=10, b=10), legend_title_text="")
            st.plotly_chart(line, use_container_width=True, config={"displayModeBar": False})

    last_run = runs[0] if runs else {}
    st.markdown("**Latest Run Snapshot**")
    st.markdown(f"- Run ID: `{last_run.get('dag_run_id', '-')}`")
    st.markdown(f"- State: {_state_badge(last_run.get('state'))}", unsafe_allow_html=True)
    st.markdown(f"- Start: `{_format_time(last_run.get('start_date'))}`")
    st.markdown(f"- End: `{_format_time(last_run.get('end_date'))}`")
    st.markdown("</div>", unsafe_allow_html=True)

st.markdown("<div class='panel'>", unsafe_allow_html=True)
st.subheader("Recent Runs")

if runs:
    frame = _runs_frame(runs).drop(columns=["start_dt"])
    st.dataframe(
        frame,
        use_container_width=True,
        hide_index=True,
        column_config={
            "run_id": st.column_config.TextColumn("Run ID", width="large"),
            "state": st.column_config.TextColumn("State", width="small"),
            "type": st.column_config.TextColumn("Type", width="small"),
            "start": st.column_config.TextColumn("Start", width="medium"),
            "end": st.column_config.TextColumn("End", width="medium"),
            "urls": st.column_config.TextColumn("URLs", width="large"),
            "keywords": st.column_config.TextColumn("Keywords", width="medium"),
            "max_pages": st.column_config.NumberColumn("Pages", width="small"),
            "compartment": st.column_config.TextColumn("Compartment", width="medium"),
            "schema": st.column_config.TextColumn("Schema Path", width="large"),
        },
    )
else:
    st.info("No DAG runs found yet")

st.markdown("</div>", unsafe_allow_html=True)

with st.expander("How This Suite Works"):
    st.markdown(
        """
1. Submit URL list and schema path in this Control Center.
2. UI triggers Airflow DAG `crawler_csv_to_postgres` through the Airflow API.
3. DAG executes `crawl -> transform -> ai_enrich -> load`.
4. Data lands in `scraped_records` and optional typed projections.
5. Superset dashboards read the warehouse and update visual insights.
6. Scheduled freshness is controlled by DAG frequency (`@daily` by default).
        """.strip()
    )
