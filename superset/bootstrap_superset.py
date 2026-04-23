from __future__ import annotations

import json
import logging
from typing import Any

from flask_appbuilder.security.sqla.models import User
from sqlalchemy import create_engine, text
from superset import db
from superset.app import create_app

logger = logging.getLogger(__name__)

WAREHOUSE_URI = "postgresql+psycopg2://platform:platform@postgres:5432/analytics"
DATABASE_NAME = "analytics_warehouse"
BOOKS_TABLE = "scraped_books"
RECORDS_TABLE = "scraped_records"
BOOKS_DASHBOARD_SLUG = "crawler-analytics"
BOOKS_DASHBOARD_TITLE = "Crawler Analytics"
RECORDS_DASHBOARD_SLUG = "crawler-platform-overview"
RECORDS_DASHBOARD_TITLE = "Crawler Platform Overview"


def _table_exists(table_name: str) -> bool:
    engine = create_engine(WAREHOUSE_URI)
    with engine.begin() as conn:
        result = conn.execute(
            text(
                """
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = 'public' AND table_name = :table_name
                LIMIT 1
                """
            ),
            {"table_name": table_name},
        ).first()
    return bool(result)


def _dataset_views() -> list[str]:
    engine = create_engine(WAREHOUSE_URI)
    with engine.begin() as conn:
        rows = conn.execute(
            text(
                """
                SELECT table_name
                FROM information_schema.views
                WHERE table_schema = 'public'
                  AND table_name LIKE 'vw_dataset_%'
                ORDER BY table_name
                """
            )
        ).fetchall()
    return [row[0] for row in rows]


def _records_chart_params(datasource: str) -> list[dict[str, Any]]:
    return [
        {
            "name": "Rows by Dataset",
            "viz_type": "dist_bar",
            "params": {
                "datasource": datasource,
                "viz_type": "dist_bar",
                "groupby": ["dataset_name"],
                "metrics": [{"expressionType": "SIMPLE", "column": {"column_name": "id"}, "aggregate": "COUNT", "label": "COUNT(id)", "optionName": "metric_count_id"}],
                "row_limit": 1000,
                "order_desc": True,
                "color_scheme": "d3Category10",
            },
        },
        {
            "name": "Rows by Compartment (Pie)",
            "viz_type": "pie",
            "params": {
                "datasource": datasource,
                "viz_type": "pie",
                "groupby": ["compartment"],
                "query_mode": "aggregate",
                "metric": {"expressionType": "SIMPLE", "column": {"column_name": "id"}, "aggregate": "COUNT", "label": "COUNT(id)", "optionName": "metric_count_id"},
                "row_limit": 1000,
                "donut": True,
                "labels_outside": True,
                "color_scheme": "googleCategory10c",
            },
        },
        {
            "name": "Rows by Source Domain",
            "viz_type": "dist_bar",
            "params": {
                "datasource": datasource,
                "viz_type": "dist_bar",
                "groupby": ["source_domain"],
                "metrics": [{"expressionType": "SIMPLE", "column": {"column_name": "id"}, "aggregate": "COUNT", "label": "COUNT(id)", "optionName": "metric_count_id"}],
                "row_limit": 20,
                "order_desc": True,
                "color_scheme": "bnbColors",
            },
        },
        {
            "name": "Rows by Scrape Day (All Datasets)",
            "viz_type": "echarts_timeseries_line",
            "params": {
                "datasource": datasource,
                "viz_type": "echarts_timeseries_line",
                "groupby": ["dataset_name"],
                "metrics": [{"expressionType": "SIMPLE", "column": {"column_name": "id"}, "aggregate": "COUNT", "label": "COUNT(id)", "optionName": "metric_count_id"}],
                "granularity_sqla": "scraped_at",
                "time_grain_sqla": "P1D",
                "time_range": "No filter",
                "row_limit": 1000,
                "color_scheme": "supersetColors",
            },
        },
    ]


def _books_chart_params(datasource: str) -> list[dict[str, Any]]:
    return [
        {
            "name": "Rows by Compartment",
            "viz_type": "dist_bar",
            "params": {
                "datasource": datasource,
                "viz_type": "dist_bar",
                "groupby": ["compartment"],
                "metrics": [{"expressionType": "SIMPLE", "column": {"column_name": "id"}, "aggregate": "COUNT", "label": "COUNT(id)", "optionName": "metric_count_id"}],
                "row_limit": 1000,
                "order_desc": True,
                "color_scheme": "supersetColors",
            },
        },
        {
            "name": "Rows by AI Cluster",
            "viz_type": "dist_bar",
            "params": {
                "datasource": datasource,
                "viz_type": "dist_bar",
                "groupby": ["ai_cluster_label"],
                "metrics": [{"expressionType": "SIMPLE", "column": {"column_name": "id"}, "aggregate": "COUNT", "label": "COUNT(id)", "optionName": "metric_count_id"}],
                "row_limit": 1000,
                "order_desc": True,
                "color_scheme": "d3Category10",
            },
        },
        {
            "name": "Rating Distribution (Pie)",
            "viz_type": "pie",
            "params": {
                "datasource": datasource,
                "viz_type": "pie",
                "groupby": ["rating"],
                "query_mode": "aggregate",
                "metric": {"expressionType": "SIMPLE", "column": {"column_name": "id"}, "aggregate": "COUNT", "label": "COUNT(id)", "optionName": "metric_count_id"},
                "row_limit": 1000,
                "donut": True,
                "labels_outside": True,
                "color_scheme": "googleCategory10c",
            },
        },
        {
            "name": "Average Price by Rating",
            "viz_type": "dist_bar",
            "params": {
                "datasource": datasource,
                "viz_type": "dist_bar",
                "groupby": ["rating"],
                "metrics": [{"expressionType": "SIMPLE", "column": {"column_name": "price"}, "aggregate": "AVG", "label": "AVG(price)", "optionName": "metric_avg_price"}],
                "row_limit": 1000,
                "order_desc": False,
                "color_scheme": "supersetColors",
            },
        },
        {
            "name": "Average Price by Compartment",
            "viz_type": "dist_bar",
            "params": {
                "datasource": datasource,
                "viz_type": "dist_bar",
                "groupby": ["compartment"],
                "metrics": [{"expressionType": "SIMPLE", "column": {"column_name": "price"}, "aggregate": "AVG", "label": "AVG(price)", "optionName": "metric_avg_price"}],
                "row_limit": 1000,
                "order_desc": True,
                "color_scheme": "bnbColors",
            },
        },
        {
            "name": "Rows by Scrape Day",
            "viz_type": "echarts_timeseries_line",
            "params": {
                "datasource": datasource,
                "viz_type": "echarts_timeseries_line",
                "groupby": [],
                "metrics": [{"expressionType": "SIMPLE", "column": {"column_name": "id"}, "aggregate": "COUNT", "label": "COUNT(id)", "optionName": "metric_count_id"}],
                "granularity_sqla": "scraped_at",
                "time_grain_sqla": "P1D",
                "time_range": "No filter",
                "row_limit": 1000,
                "color_scheme": "supersetColors",
            },
        },
        {
            "name": "Top Expensive Books",
            "viz_type": "table",
            "params": {
                "datasource": datasource,
                "viz_type": "table",
                "query_mode": "raw",
                "all_columns": ["compartment", "source_domain", "ai_cluster_label", "ai_price_band", "title", "price", "rating", "stock_count"],
                "row_limit": 20,
                "server_page_length": 20,
                "order_by_cols": ['["price", false]'],
            },
        },
    ]


def _upsert_dataset(session, SqlaTable, database, admin, table_name: str):
    dataset = (
        session.query(SqlaTable)
        .filter_by(table_name=table_name, database_id=database.id, schema=None)
        .one_or_none()
    )
    if dataset is None:
        dataset = SqlaTable(
            table_name=table_name,
            database=database,
            schema=None,
            main_dttm_col="scraped_at",
            owners=[admin],
        )
        session.add(dataset)
        session.commit()
        logger.info("Created dataset: %s", table_name)

    dataset.fetch_metadata()
    session.commit()
    return dataset


def _upsert_dashboard(session, Dashboard, admin, title: str, slug: str):
    dashboard = session.query(Dashboard).filter_by(slug=slug).one_or_none()
    if dashboard is None:
        dashboard = Dashboard(
            dashboard_title=title,
            slug=slug,
            published=True,
            owners=[admin],
        )
        session.add(dashboard)
        session.commit()
        logger.info("Created dashboard: %s", title)
    return dashboard


def _upsert_charts(session, Slice, admin, dataset_id: int, datasource: str, charts: list[dict[str, Any]]):
    slices = []
    for chart in charts:
        slc = (
            session.query(Slice)
            .filter_by(slice_name=chart["name"], datasource_id=dataset_id, datasource_type="table")
            .one_or_none()
        )
        if slc is None:
            slc = Slice(
                slice_name=chart["name"],
                datasource_id=dataset_id,
                datasource_type="table",
                viz_type=chart["viz_type"],
                params=json.dumps(chart["params"]),
                owners=[admin],
            )
            session.add(slc)
            session.commit()
            logger.info("Created chart: %s", chart["name"])
        else:
            slc.viz_type = chart["viz_type"]
            slc.params = json.dumps(chart["params"])
            session.commit()
        slices.append(slc)
    return slices


def bootstrap() -> None:
    app = create_app()
    with app.app_context():
        from superset.connectors.sqla.models import SqlaTable
        from superset.models.core import Database
        from superset.models.dashboard import Dashboard
        from superset.models.slice import Slice

        session = db.session
        admin = session.query(User).filter_by(username="admin").one_or_none()
        if not admin:
            raise RuntimeError("Superset admin user not found; run superset init first")

        database = session.query(Database).filter_by(database_name=DATABASE_NAME).one_or_none()
        if database is None:
            database = Database(
                database_name=DATABASE_NAME,
                sqlalchemy_uri=WAREHOUSE_URI,
                expose_in_sqllab=True,
                allow_ctas=False,
                allow_dml=False,
            )
            database.set_sqlalchemy_uri(WAREHOUSE_URI)
            session.add(database)
            session.commit()
            logger.info("Created Superset database connection: %s", DATABASE_NAME)

        existing_views = _dataset_views()
        for view_name in existing_views:
            _upsert_dataset(session, SqlaTable, database, admin, view_name)

        if _table_exists(RECORDS_TABLE):
            records_dataset = _upsert_dataset(session, SqlaTable, database, admin, RECORDS_TABLE)
            records_datasource = f"{records_dataset.id}__table"
            records_charts = _upsert_charts(
                session,
                Slice,
                admin,
                records_dataset.id,
                records_datasource,
                _records_chart_params(records_datasource),
            )
            records_dashboard = _upsert_dashboard(
                session, Dashboard, admin, RECORDS_DASHBOARD_TITLE, RECORDS_DASHBOARD_SLUG
            )
            records_dashboard.slices = records_charts
            records_dashboard.dashboard_title = RECORDS_DASHBOARD_TITLE
            records_dashboard.published = True
            records_dashboard.position_json = None
            records_dashboard.json_metadata = None
            session.commit()

        if _table_exists(BOOKS_TABLE):
            books_dataset = _upsert_dataset(session, SqlaTable, database, admin, BOOKS_TABLE)
            books_datasource = f"{books_dataset.id}__table"
            books_charts = _upsert_charts(
                session,
                Slice,
                admin,
                books_dataset.id,
                books_datasource,
                _books_chart_params(books_datasource),
            )
            books_dashboard = _upsert_dashboard(session, Dashboard, admin, BOOKS_DASHBOARD_TITLE, BOOKS_DASHBOARD_SLUG)
            books_dashboard.slices = books_charts
            books_dashboard.dashboard_title = BOOKS_DASHBOARD_TITLE
            books_dashboard.published = True
            books_dashboard.position_json = None
            books_dashboard.json_metadata = None
            session.commit()

        logger.info("Superset assets bootstrapped successfully")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    bootstrap()
