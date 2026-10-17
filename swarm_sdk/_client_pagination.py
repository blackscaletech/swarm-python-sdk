from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from urllib import parse


@dataclass(slots=True)
class CollectionPage:
    items: list[Any]
    next_cursor: str | None
    order: str | None
    headers: dict[str, str]


def _encode_query(query: dict[str, Any] | None) -> str:
    if not query:
        return ""
    def encode_value(value: Any) -> str:
        if isinstance(value, bool):
            return "true" if value else "false"
        return str(value)

    items: list[tuple[str, str]] = []
    for key, value in query.items():
        if value in (None, ""):
            continue
        if isinstance(value, (list, tuple)):
            for item in value:
                if item in (None, ""):
                    continue
                items.append((key, encode_value(item)))
            continue
        items.append((key, encode_value(value)))
    if not items:
        return ""
    return parse.urlencode(items, doseq=True)
