import os

RAW_CSV_PATH = os.getenv("RAW_CSV_PATH", "/opt/platform/data/raw/books_raw.csv")
PROCESSED_CSV_PATH = os.getenv("PROCESSED_CSV_PATH", "/opt/platform/data/processed/books_clean.csv")
WAREHOUSE_DSN = os.getenv("WAREHOUSE_DSN", "postgresql+psycopg2://platform:platform@postgres:5432/analytics")
