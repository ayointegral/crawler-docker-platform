from __future__ import annotations

import json
import logging
from typing import Any

from flask_appbuilder.security.sqla.models import User
from superset import db
from superset.app import create_app

logger = logging.getLogger(__name__)

WAREHOUSE_URI = "postgresql+psycopg2://platform:platform@postgres:5432/analytics"
DATABASE_NAME = "analytics_warehouse"
DATASET_NAME = "scraped_books"
DASHBOARD_SLUG = "crawler-analytics"
DASHBOARD_TITLE = "Crawler Analytics"


def _chart_params(datasource: str) -> list[dict[str, Any]]:
    return [
        {
            "name": "Rows by Compartment",
            "viz_type": "dist_bar",
            "params": {
                "datasource": datasource,
                "viz_type": "dist_bar",
                "groupby": ["compartment"],
                "metrics": [
                    {
                        "expressionType": "SIMPLE",
                        "column": {"column_name": "id"},
                        "aggregate": "COUNT",
                        "label": "COUNT(id)",
                        "optionName": "metric_count_id",
                    }
                ],
                "adhoc_filters": [],
                "row_limit": 1000,
                "order_desc": True,
                "color_scheme": "supersetColors",
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
                "metric": {
                    "expressionType": "SIMPLE",
                    "column": {"column_name": "id"},
                    "aggregate": "COUNT",
                    "label": "COUNT(id)",
                    "optionName": "metric_count_id",
                },
                "adhoc_filters": [],
                "row_limit": 1000,
                "donut": True,
                "labels_outside": True,
                "color_scheme": "supersetColors",
            },
        },
        {
            "name": "Average Price by Rating",
            "viz_type": "dist_bar",
            "params": {
                "datasource": datasource,
                "viz_type": "dist_bar",
                "groupby": ["rating"],
                "metrics": [
                    {
                        "expressionType": "SIMPLE",
                        "column": {"column_name": "price"},
                        "aggregate": "AVG",
                        "label": "AVG(price)",
                        "optionName": "metric_avg_price",
                    }
                ],
                "adhoc_filters": [],
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
                "metrics": [
                    {
                        "expressionType": "SIMPLE",
                        "column": {"column_name": "price"},
                        "aggregate": "AVG",
                        "label": "AVG(price)",
                        "optionName": "metric_avg_price",
                    }
                ],
                "adhoc_filters": [],
                "row_limit": 1000,
                "order_desc": True,
                "color_scheme": "supersetColors",
            },
        },
        {
            "name": "Rows by Scrape Day",
            "viz_type": "echarts_timeseries_line",
            "params": {
                "datasource": datasource,
                "viz_type": "echarts_timeseries_line",
                "groupby": [],
                "metrics": [
                    {
                        "expressionType": "SIMPLE",
                        "column": {"column_name": "id"},
                        "aggregate": "COUNT",
                        "label": "COUNT(id)",
                        "optionName": "metric_count_id",
                    }
                ],
                "granularity_sqla": "scraped_at",
                "time_grain_sqla": "P1D",
                "time_range": "No filter",
                "adhoc_filters": [],
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
                "all_columns": ["compartment", "source_domain", "title", "price", "rating", "stock_count"],
                "row_limit": 20,
                "server_page_length": 20,
                "order_by_cols": ['["price", false]'],
            },
        },
    ]


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

        # Cleanup old/legacy seeded dashboard if present.
        legacy_dashboard = (
            session.query(Dashboard).filter_by(slug="crawler-analytics-legacy").one_or_none()
        )
        if legacy_dashboard is not None:
            session.delete(legacy_dashboard)
            session.commit()
            logger.info("Deleted legacy dashboard: crawler-analytics-legacy")

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

        dataset = (
            session.query(SqlaTable)
            .filter_by(table_name=DATASET_NAME, database_id=database.id, schema=None)
            .one_or_none()
        )
        if dataset is None:
            dataset = SqlaTable(
                table_name=DATASET_NAME,
                database=database,
                schema=None,
                main_dttm_col="scraped_at",
                owners=[admin],
            )
            session.add(dataset)
            session.commit()
            logger.info("Created dataset: %s", DATASET_NAME)

        dataset.fetch_metadata()
        session.commit()

        datasource = f"{dataset.id}__table"
        slices = []
        for chart in _chart_params(datasource):
            slc = (
                session.query(Slice)
                .filter_by(
                    slice_name=chart["name"],
                    datasource_id=dataset.id,
                    datasource_type="table",
                )
                .one_or_none()
            )
            if slc is None:
                slc = Slice(
                    slice_name=chart["name"],
                    datasource_id=dataset.id,
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

        dashboard = session.query(Dashboard).filter_by(slug=DASHBOARD_SLUG).one_or_none()
        if dashboard is None:
            dashboard = Dashboard(
                dashboard_title=DASHBOARD_TITLE,
                slug=DASHBOARD_SLUG,
                published=True,
                owners=[admin],
            )
            session.add(dashboard)
            session.commit()
            logger.info("Created dashboard: %s", DASHBOARD_TITLE)

        dashboard.slices = slices
        dashboard.dashboard_title = DASHBOARD_TITLE
        dashboard.published = True
        # Let Superset build and persist canonical dashboard layout in the UI editor.
        # Handcrafted position_json can break rendering across Superset versions.
        dashboard.position_json = None
        dashboard.json_metadata = None
        session.commit()
        logger.info("Superset assets bootstrapped successfully")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    bootstrap()
