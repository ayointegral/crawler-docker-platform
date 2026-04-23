import os

ROW_LIMIT = 5000
SQL_MAX_ROW = 100000
TALISMAN_ENABLED = False
WTF_CSRF_ENABLED = True
SECRET_KEY = os.getenv("SUPERSET_SECRET_KEY", "change_me_in_prod")
SQLALCHEMY_DATABASE_URI = os.getenv(
    "SQLALCHEMY_DATABASE_URI",
    "postgresql+psycopg2://platform:platform@postgres:5432/superset_metadata",
)

FEATURE_FLAGS = {
    "DASHBOARD_RBAC": True,
}
