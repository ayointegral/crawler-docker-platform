from __future__ import annotations

import os
import re
from urllib.parse import urlparse

import pandas as pd

from .settings import PROCESSED_CSV_PATH, RAW_CSV_PATH


def _availability_to_count(value: str) -> int:
    match = re.search(r"(\d+)", value or "")
    if not match:
        return 0
    return int(match.group(1))


def _extract_domain(source_url: str) -> str:
    if not source_url:
        return ""
    return (urlparse(source_url).netloc or "").lower()


def _normalize_compartment(value: str | None, fallback: str) -> str:
    candidate = (value or "").strip().lower().replace(" ", "_")
    if candidate:
        return candidate
    return fallback.strip().lower().replace(" ", "_") or "default"


def transform_raw_csv(
    raw_path: str = RAW_CSV_PATH,
    output_path: str = PROCESSED_CSV_PATH,
    default_compartment: str = "default",
) -> dict:
    if not os.path.exists(raw_path):
        raise FileNotFoundError(f"Raw CSV not found at {raw_path}")

    df = pd.read_csv(raw_path)
    if df.empty:
        raise ValueError("Raw CSV is empty; nothing to transform")

    df["title"] = df["title"].fillna("").str.strip()
    df = df[df["title"] != ""]
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df["rating"] = pd.to_numeric(df["rating"], errors="coerce")
    df["stock_count"] = df["availability_text"].apply(_availability_to_count)
    df["scraped_at"] = pd.to_datetime(df["scraped_at"], utc=True, errors="coerce")
    if "compartment" not in df.columns:
        df["compartment"] = default_compartment
    df["compartment"] = df["compartment"].apply(
        lambda value: _normalize_compartment(value, default_compartment)
    )
    df["source_domain"] = df["source_url"].fillna("").apply(_extract_domain)

    df = df.dropna(subset=["price", "rating", "source_url", "scraped_at", "compartment"])
    df = df[df["source_domain"] != ""]
    df = df.drop_duplicates(subset=["source_url", "compartment"])

    output_dir = os.path.dirname(output_path)
    os.makedirs(output_dir, exist_ok=True)
    df.to_csv(output_path, index=False)

    return {
        "raw_rows": int(len(pd.read_csv(raw_path))),
        "clean_rows": int(len(df)),
        "output_path": output_path,
    }
