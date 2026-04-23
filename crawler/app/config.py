from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


@dataclass(frozen=True)
class SelectorSpec:
    type: str
    query: str


@dataclass(frozen=True)
class FieldSpec:
    name: str
    data_type: str
    selector: SelectorSpec
    regex: str | None = None
    value_map: dict[str, Any] | None = None


@dataclass(frozen=True)
class CrawlSchema:
    dataset_name: str
    record_selector: SelectorSpec
    detail_link_selector: SelectorSpec | None
    pagination_selector: SelectorSpec | None
    fields: list[FieldSpec]


@dataclass(frozen=True)
class CrawlConfig:
    start_urls: list[str]
    max_pages: int
    output_file: str
    keyword_filters: list[str] | None
    compartment: str
    schema_path: str
    schema: CrawlSchema


def _parse_selector(raw: dict[str, Any] | str | None, default_type: str = "css") -> SelectorSpec | None:
    if not raw:
        return None
    if isinstance(raw, str):
        return SelectorSpec(type=default_type, query=raw)
    return SelectorSpec(type=str(raw.get("type", default_type)).strip().lower(), query=str(raw["query"]).strip())


def _parse_field(raw: dict[str, Any]) -> FieldSpec:
    selector = _parse_selector(raw.get("selector"))
    if not selector:
        raise ValueError(f"Field '{raw.get('name')}' requires a selector")
    return FieldSpec(
        name=str(raw["name"]).strip(),
        data_type=str(raw.get("data_type", "string")).strip().lower(),
        selector=selector,
        regex=(str(raw.get("regex")).strip() if raw.get("regex") else None),
        value_map=raw.get("value_map") if isinstance(raw.get("value_map"), dict) else None,
    )


def load_schema(schema_path: str) -> CrawlSchema:
    path = Path(schema_path)
    if not path.exists():
        raise FileNotFoundError(f"Crawler schema not found: {schema_path}")

    with path.open("r", encoding="utf-8") as stream:
        raw = yaml.safe_load(stream) or {}

    dataset_name = str(raw.get("dataset_name", "default_dataset")).strip().lower().replace(" ", "_")
    record_selector = _parse_selector(raw.get("record_selector"))
    if not record_selector:
        raise ValueError("Schema must define record_selector")

    fields = [_parse_field(item) for item in raw.get("fields", [])]
    if not fields:
        raise ValueError("Schema must define at least one field")

    return CrawlSchema(
        dataset_name=dataset_name or "default_dataset",
        record_selector=record_selector,
        detail_link_selector=_parse_selector(raw.get("detail_link_selector")),
        pagination_selector=_parse_selector(raw.get("pagination_selector")),
        fields=fields,
    )


def _normalize_str_list(raw_value: str | list[str] | None) -> list[str]:
    if raw_value is None:
        return []
    if isinstance(raw_value, list):
        values = [str(item).strip() for item in raw_value]
    else:
        values = []
        for line in str(raw_value).splitlines():
            values.extend(part.strip() for part in line.split(","))
    return [value for value in values if value]


def _resolve_schema_path(overrides: dict[str, Any]) -> str:
    return (
        overrides.get("source_config_path")
        or overrides.get("schema_path")
        or os.getenv("CRAWLER_SOURCE_CONFIG")
        or os.getenv("CRAWLER_SCHEMA_PATH")
        or "/app/schemas/default_schema.yml"
    )


def load_config(overrides: dict[str, Any] | None = None) -> CrawlConfig:
    merged = dict(overrides or {})

    schema_path = _resolve_schema_path(merged)
    schema = load_schema(schema_path)

    start_urls = _normalize_str_list(merged.get("start_urls"))
    if not start_urls:
        start_urls = _normalize_str_list(os.getenv("CRAWLER_START_URLS") or os.getenv("CRAWLER_START_URL"))
    if not start_urls:
        start_urls = ["https://books.toscrape.com/"]

    keywords = _normalize_str_list(merged.get("keywords"))
    if not keywords:
        keywords = _normalize_str_list(os.getenv("CRAWLER_KEYWORDS", ""))

    compartment = str(merged.get("compartment") or os.getenv("CRAWLER_COMPARTMENT", "default")).strip()

    max_pages_raw = merged.get("max_pages", os.getenv("CRAWLER_MAX_PAGES", "10"))
    output_file = str(merged.get("output_file") or os.getenv("OUTPUT_FILE", "/app/data/raw/books_raw.csv"))

    return CrawlConfig(
        start_urls=start_urls,
        max_pages=max(1, int(max_pages_raw)),
        output_file=output_file,
        keyword_filters=[value.lower() for value in keywords] or None,
        compartment=(compartment or "default"),
        schema_path=schema_path,
        schema=schema,
    )


def parse_schema_override(raw_schema: str | None) -> dict[str, Any] | None:
    if not raw_schema:
        return None
    text = raw_schema.strip()
    if not text:
        return None
    try:
        return json.loads(text)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid schema JSON: {exc}") from exc
