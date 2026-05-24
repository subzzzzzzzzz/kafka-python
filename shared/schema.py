"""Aviation review schemas for producer normalization and Spark parsing."""

from __future__ import annotations

import hashlib
import uuid
from datetime import datetime, timezone
from typing import Any

from pyspark.sql.types import DoubleType, StringType, StructField, StructType


FIELD_ALIASES = {
    "review_id": ("record_id", "review_id", "id", "uuid"),
    "event_time": ("timestamp", "date", "review_date", "published_date", "created_at"),
    "author": ("author", "reviewer", "name", "user", "customer_name"),
    "country": ("country", "author_country", "reviewer_country", "customer_country"),
    "rating": ("rating", "overall_rating", "score", "overall", "stars"),
    "review": ("review", "review_text", "text", "content", "comments", "body"),
    "recommended": ("recommended", "recommend", "would_recommend", "is_recommended"),
    "airline": ("airline", "airline_name"),
    "airport": ("airport", "airport_name"),
    "lounge": ("lounge", "lounge_name"),
    "seat": ("seat", "seat_name"),
}


AVIATION_REVIEW_SCHEMA = StructType(
    [
        StructField("source_category", StringType(), False),
        StructField("review_id", StringType(), False),
        StructField("event_time", StringType(), False),
        StructField("author", StringType(), True),
        StructField("country", StringType(), True),
        StructField("rating", DoubleType(), True),
        StructField("review", StringType(), True),
        StructField("recommended", StringType(), True),
        StructField("airline", StringType(), True),
        StructField("airport", StringType(), True),
        StructField("lounge", StringType(), True),
        StructField("seat", StringType(), True),
    ]
)


def normalize_record(source_category: str, row: dict[str, Any], row_number: int) -> dict[str, Any]:
    category = source_category.strip().lower()
    rating = _rating(_first(row, FIELD_ALIASES["rating"]))
    review = _text(_first(row, FIELD_ALIASES["review"]))

    if rating is None:
        raise ValueError("rating is required")
    if not review:
        raise ValueError("review text is required")

    record = {
        "source_category": category,
        "review_id": _text(_first(row, FIELD_ALIASES["review_id"]))
        or _stable_id(category, row, row_number),
        "event_time": _event_time(_first(row, FIELD_ALIASES["event_time"])),
        "author": _text(_first(row, FIELD_ALIASES["author"])) or "unknown",
        "country": _nullable(_first(row, FIELD_ALIASES["country"])),
        "rating": rating,
        "review": review,
        "recommended": _recommended(_first(row, FIELD_ALIASES["recommended"])),
        "airline": _entity_value(category, "airline", row),
        "airport": _entity_value(category, "airport", row),
        "lounge": _entity_value(category, "lounge", row),
        "seat": _entity_value(category, "seat", row),
    }
    return record


def _first(row: dict[str, Any], aliases: tuple[str, ...]) -> Any:
    normalized = {str(key).strip().lower(): value for key, value in row.items()}
    for alias in aliases:
        if alias in normalized:
            return normalized[alias]
    return None


def _text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _nullable(value: Any) -> str | None:
    text = _text(value)
    if text.lower() in {"", "none", "null", "nan", "n/a", "na"}:
        return None
    return text


def _rating(value: Any) -> float | None:
    text = _text(value)
    if not text:
        return None
    rating = float(text)
    if rating < 0 or rating > 10:
        raise ValueError(f"rating out of range: {rating}")
    return rating


def _recommended(value: Any) -> str | None:
    text = _text(value).lower()
    if text in {"", "none", "null", "nan", "n/a", "na"}:
        return None
    if text in {"yes", "y", "true", "1", "recommended"}:
        return "yes"
    if text in {"no", "n", "false", "0", "not recommended"}:
        return "no"
    return None


def _entity_value(category: str, entity: str, row: dict[str, Any]) -> str | None:
    if category != entity:
        return None
    return _nullable(_first(row, FIELD_ALIASES[entity]))


def _event_time(value: Any) -> str:
    text = _text(value)
    if not text:
        return datetime.now(timezone.utc).isoformat()
    for fmt in ("%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            parsed = datetime.strptime(text, fmt)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return parsed.isoformat()
        except ValueError:
            pass
    parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.isoformat()


def _stable_id(category: str, row: dict[str, Any], row_number: int) -> str:
    seed = f"{category}|{row_number}|{sorted((str(k), str(v)) for k, v in row.items())}"
    digest = hashlib.sha256(seed.encode("utf-8")).hexdigest()
    return str(uuid.UUID(digest[:32]))
