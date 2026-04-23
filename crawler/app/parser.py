from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urljoin

from .config import FieldSpec, SelectorSpec


def _select_values(target, selector: SelectorSpec) -> list[str]:
    if selector.type == "xpath":
        result = target.xpath(selector.query).getall()
    else:
        result = target.css(selector.query).getall()
    return [str(value).strip() for value in result if str(value).strip()]


def _select_first(target, selector: SelectorSpec) -> str | None:
    values = _select_values(target, selector)
    return values[0] if values else None


def _cast_value(raw_value: str | None, spec: FieldSpec) -> Any:
    if raw_value is None:
        return None

    value = raw_value
    if spec.regex:
        match = re.search(spec.regex, value)
        value = match.group(1) if match else ""

    if spec.value_map and value in spec.value_map:
        return spec.value_map[value]

    data_type = spec.data_type
    if data_type in {"string", "text"}:
        return value
    if data_type in {"integer", "int"}:
        return int(value) if value else None
    if data_type in {"float", "number", "decimal"}:
        return float(value) if value else None
    if data_type in {"boolean", "bool"}:
        return value.lower() in {"1", "true", "yes", "y"}

    return value


def parse_record(
    response,
    item,
    fields: list[FieldSpec],
    detail_link_selector: SelectorSpec | None = None,
) -> dict[str, Any]:
    parsed: dict[str, Any] = {}
    for field in fields:
        raw = _select_first(item, field.selector)
        parsed[field.name] = _cast_value(raw, field)

    relative_url = _select_first(item, detail_link_selector) if detail_link_selector else None
    parsed["source_url"] = urljoin(response.url, relative_url) if relative_url else response.url
    parsed["scraped_at"] = datetime.now(timezone.utc).isoformat()
    return parsed


def find_next_page(response, selector: SelectorSpec | None) -> str | None:
    if not selector:
        return None
    return _select_first(response, selector)
