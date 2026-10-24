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


def _encode_query(query: dict[str, Any] | None, *, existing: str = "") -> str:
    if not query:
        return existing
    def encode_value(value: Any) -> str:
        if isinstance(value, bool):
            return "true" if value else "false"
        return str(value)

    items = parse.parse_qsl(existing, keep_blank_values=True)
    changed = False
    for key, value in query.items():
        if value in (None, ""):
            continue
        if isinstance(value, (list, tuple)):
            for item in value:
                if item in (None, ""):
                    continue
                items.append((key, encode_value(item)))
                changed = True
            continue
        # URLSearchParams.set replaces duplicates at their first position.
        index = next((index for index, item in enumerate(items) if item[0] == key), len(items))
        items = [item for item in items if item[0] != key]
        items.insert(index, (key, encode_value(value)))
        changed = True
    if not changed:
        return existing
    return parse.urlencode(items, doseq=True)
