"""Validation helpers for CSV rows and normalized Kafka messages."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from config import CANONICAL_FIELDS
from schema import VALID_CATEGORIES


@dataclass
class ValidationReport:
    source_category: str
    total_rows: int = 0
    valid_rows: int = 0
    invalid_rows: int = 0
    errors: list[dict[str, Any]] = field(default_factory=list)

    def add_valid(self) -> None:
        self.total_rows += 1
        self.valid_rows += 1

    def add_invalid(self, row_number: int, reason: str, row: dict[str, Any] | None = None) -> None:
        self.total_rows += 1
        self.invalid_rows += 1
        self.errors.append({"row_number": row_number, "reason": reason, "row": row or {}})

    def as_dict(self) -> dict[str, Any]:
        return {
            "source_category": self.source_category,
            "total_rows": self.total_rows,
            "valid_rows": self.valid_rows,
            "invalid_rows": self.invalid_rows,
            "errors": self.errors,
        }


def validate_normalized_message(message: dict[str, Any]) -> list[str]:
    errors: list[str] = []

    for field in CANONICAL_FIELDS:
        if field not in message:
            errors.append(f"Missing field: {field}")

    category = message.get("source_category")
    if category not in VALID_CATEGORIES:
        errors.append(f"Invalid source_category: {category!r}")

    if not message.get("record_id"):
        errors.append("record_id is required")

    if not message.get("timestamp"):
        errors.append("timestamp is required")

    if not message.get("review_text"):
        errors.append("review_text is required")

    rating = message.get("overall_rating")
    if rating is None:
        errors.append("overall_rating is required")
    elif not isinstance(rating, (int, float)):
        errors.append("overall_rating must be numeric")
    elif rating < 0 or rating > 10:
        errors.append("overall_rating must be between 0 and 10")

    recommended = message.get("recommended")
    if recommended not in {"yes", "no", None}:
        errors.append("recommended must be yes, no, or null")

    original_data = message.get("original_data")
    if not isinstance(original_data, dict):
        errors.append("original_data must be an object")

    return errors


def validate_output_record(record: dict[str, Any], fields: tuple[str, ...]) -> None:
    missing = [field for field in fields if field not in record]
    if missing:
        raise ValueError(f"Output record missing fields: {missing}")
