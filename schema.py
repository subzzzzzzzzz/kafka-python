"""Schema normalization for heterogeneous aviation review CSV rows."""

from __future__ import annotations

import hashlib
import uuid
from datetime import datetime, timezone
from typing import Any

from config import CANONICAL_FIELDS


FIELD_ALIASES: dict[str, tuple[str, ...]] = {
    "record_id": ("record_id", "id", "review_id", "uuid"),
    "timestamp": ("timestamp", "date", "review_date", "published_date", "created_at"),
    "author": ("author", "reviewer", "name", "user", "customer_name"),
    "author_country": ("author_country", "country", "reviewer_country", "customer_country"),
    "overall_rating": ("overall_rating", "rating", "score", "overall", "stars"),
    "review_text": ("review_text", "review", "text", "content", "comments", "body"),
    "recommended": ("recommended", "recommend", "would_recommend", "is_recommended"),
    "entity_name": ("airline", "airline_name", "airport", "airport_name", "lounge", "lounge_name", "seat", "seat_name", "name"),
}

VALID_CATEGORIES = {"airline", "airport", "lounge", "seat"}
YES_VALUES = {"yes", "y", "true", "1", "recommended"}
NO_VALUES = {"no", "n", "false", "0", "not recommended"}
NULL_VALUES = {"", "none", "null", "nan", "n/a", "na"}


def normalize_row(source_category: str, row: dict[str, Any], row_number: int | None = None) -> dict[str, Any]:
    """Normalize a source CSV row into the common JSON envelope."""
    category = source_category.strip().lower()
    if category not in VALID_CATEGORIES:
        raise ValueError(f"Unsupported source category: {source_category}")

    normalized = {
        "source_category": category,
        "record_id": _first_value(row, FIELD_ALIASES["record_id"]),
        "timestamp": _normalize_timestamp(_first_value(row, FIELD_ALIASES["timestamp"])),
        "author": _coerce_string(_first_value(row, FIELD_ALIASES["author"])) or "unknown",
        "author_country": _nullable_string(_first_value(row, FIELD_ALIASES["author_country"])),
        "overall_rating": _normalize_rating(_first_value(row, FIELD_ALIASES["overall_rating"])),
        "review_text": _coerce_string(_first_value(row, FIELD_ALIASES["review_text"])),
        "recommended": _normalize_recommended(_first_value(row, FIELD_ALIASES["recommended"])),
        "original_data": dict(row),
    }

    if not normalized["record_id"]:
        normalized["record_id"] = _deterministic_record_id(category, row, row_number)

    return {field: normalized[field] for field in CANONICAL_FIELDS}


def entity_name(message: dict[str, Any]) -> str | None:
    original = message.get("original_data") or {}
    return _nullable_string(_first_value(original, FIELD_ALIASES["entity_name"]))


def _first_value(row: dict[str, Any], aliases: tuple[str, ...]) -> Any:
    lowered = {str(key).strip().lower(): value for key, value in row.items()}
    for alias in aliases:
        if alias.lower() in lowered:
            return lowered[alias.lower()]
    return None


def _coerce_string(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _nullable_string(value: Any) -> str | None:
    text = _coerce_string(value)
    if text.lower() in NULL_VALUES:
        return None
    return text


def _normalize_rating(value: Any) -> float | None:
    text = _coerce_string(value)
    if text.lower() in NULL_VALUES:
        return None
    try:
        return float(text)
    except ValueError as exc:
        raise ValueError(f"Invalid overall_rating: {value!r}") from exc


def _normalize_recommended(value: Any) -> str | None:
    text = _coerce_string(value).lower()
    if text in NULL_VALUES:
        return None
    if text in YES_VALUES:
        return "yes"
    if text in NO_VALUES:
        return "no"
    raise ValueError(f"Invalid recommended value: {value!r}")


def _normalize_timestamp(value: Any) -> str:
    text = _coerce_string(value)
    if text.lower() in NULL_VALUES:
        return datetime.now(timezone.utc).isoformat()

    candidates = (
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
        "%d/%m/%Y",
        "%m/%d/%Y",
    )
    for fmt in candidates:
        try:
            parsed = datetime.strptime(text, fmt)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return parsed.isoformat()
        except ValueError:
            continue

    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.isoformat()
    except ValueError as exc:
        raise ValueError(f"Invalid timestamp: {value!r}") from exc


def _deterministic_record_id(category: str, row: dict[str, Any], row_number: int | None) -> str:
    seed = f"{category}|{row_number}|{sorted((str(k), str(v)) for k, v in row.items())}"
    digest = hashlib.sha256(seed.encode("utf-8")).hexdigest()
    return str(uuid.UUID(digest[:32]))
