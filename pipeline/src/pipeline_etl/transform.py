from __future__ import annotations

import os
from urllib.parse import urlparse

import pandas as pd
from pandas.errors import EmptyDataError

from app.config import load_schema

from .settings import PROCESSED_CSV_PATH, RAW_CSV_PATH


def _extract_domain(source_url: str) -> str:
    if not source_url:
        return ""
    return (urlparse(source_url).netloc or "").lower()


def _normalize_label(value: str | None, fallback: str) -> str:
    candidate = (value or "").strip().lower().replace(" ", "_")
    if candidate:
        return candidate
    normalized = fallback.strip().lower().replace(" ", "_")
    return normalized or "default"


def _trim_object_columns(df: pd.DataFrame) -> pd.DataFrame:
    for column in df.columns:
        if df[column].dtype == "object":
            df[column] = df[column].fillna("").astype(str).str.strip()
    return df


def _required_columns(schema_path: str | None) -> set[str]:
    if not schema_path:
        return set()
    schema = load_schema(schema_path)
    return {field.name for field in schema.fields}


def transform_raw_csv(
    raw_path: str = RAW_CSV_PATH,
    output_path: str = PROCESSED_CSV_PATH,
    default_compartment: str = "default",
    default_dataset_name: str = "default_dataset",
    schema_path: str | None = None,
) -> dict:
    if not os.path.exists(raw_path):
        raise FileNotFoundError(f"Raw CSV not found at {raw_path}")

    try:
        df = pd.read_csv(raw_path)
    except EmptyDataError:
        df = pd.DataFrame()

    if df.empty:
        empty_columns = ["dataset_name", "compartment", "source_url", "source_domain", "schema_path", "scraped_at"]
        for column in sorted(_required_columns(schema_path)):
            if column not in empty_columns:
                empty_columns.append(column)
        empty_df = pd.DataFrame(columns=empty_columns)
        output_dir = os.path.dirname(output_path)
        os.makedirs(output_dir, exist_ok=True)
        empty_df.to_csv(output_path, index=False)
        return {"raw_rows": 0, "clean_rows": 0, "output_path": output_path, "dataset_name": _normalize_label(default_dataset_name, default_dataset_name), "compartment": _normalize_label(default_compartment, default_compartment)}

    df = _trim_object_columns(df)

    required = _required_columns(schema_path)
    missing = [column for column in required if column not in df.columns]
    if missing:
        raise ValueError(f"Raw CSV missing required schema columns: {missing}")

    if "source_url" not in df.columns:
        raise ValueError("Raw CSV missing required metadata column: source_url")
    if "scraped_at" not in df.columns:
        raise ValueError("Raw CSV missing required metadata column: scraped_at")

    if "compartment" not in df.columns:
        df["compartment"] = default_compartment
    if "dataset_name" not in df.columns:
        df["dataset_name"] = default_dataset_name

    df["compartment"] = df["compartment"].apply(lambda value: _normalize_label(value, default_compartment))
    df["dataset_name"] = df["dataset_name"].apply(lambda value: _normalize_label(value, default_dataset_name))
    df["source_domain"] = df["source_url"].apply(_extract_domain)
    df["scraped_at"] = pd.to_datetime(df["scraped_at"], utc=True, errors="coerce")

    for numeric_column in ["price", "rating", "stock_count"]:
        if numeric_column in df.columns:
            df[numeric_column] = pd.to_numeric(df[numeric_column], errors="coerce")

    df = df.dropna(subset=["source_url", "source_domain", "scraped_at", "dataset_name", "compartment"])
    df = df.drop_duplicates(subset=["dataset_name", "compartment", "source_url"])

    metadata_cols = [
        "dataset_name",
        "compartment",
        "source_url",
        "source_domain",
        "schema_path",
        "scraped_at",
    ]
    ordered_columns = [column for column in metadata_cols if column in df.columns]
    ordered_columns.extend([column for column in df.columns if column not in ordered_columns])
    df = df[ordered_columns]

    output_dir = os.path.dirname(output_path)
    os.makedirs(output_dir, exist_ok=True)
    df.to_csv(output_path, index=False)

    return {
        "raw_rows": int(len(pd.read_csv(raw_path))),
        "clean_rows": int(len(df)),
        "output_path": output_path,
        "dataset_name": df["dataset_name"].iloc[0],
        "compartment": df["compartment"].iloc[0],
    }
