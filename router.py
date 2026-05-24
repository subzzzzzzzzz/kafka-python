"""Deterministic output routing for normalized aviation review messages."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from config import (
    CATEGORY_FILES,
    CATEGORY_OUTPUT_DIR,
    COUNTRY_OUTPUT_DIR,
    FILTERED_OUTPUT_DIR,
    LOW_RATING_THRESHOLD,
    NEGATIVE_KEYWORDS,
    OUTPUT_DIR,
)
from file_writer import safe_filename


def output_record(message: dict[str, Any]) -> dict[str, Any]:
    return {
        "source_category": message["source_category"],
        "record_id": message["record_id"],
        "timestamp": message["timestamp"],
        "author": message["author"],
        "author_country": message.get("author_country"),
        "overall_rating": message["overall_rating"],
        "recommended": message.get("recommended"),
        "review_text": message["review_text"],
    }


def route_message(message: dict[str, Any]) -> list[Path]:
    destinations: list[Path] = []
    category = message["source_category"]

    destinations.append(CATEGORY_OUTPUT_DIR / CATEGORY_FILES[category])
    destinations.append(OUTPUT_DIR / "all_aviation_reviews.csv")

    country = message.get("author_country")
    if country:
        destinations.append(COUNTRY_OUTPUT_DIR / f"country_{safe_filename(str(country))}.csv")
    else:
        destinations.append(COUNTRY_OUTPUT_DIR / "unattributed_reviews.csv")

    review_text = str(message.get("review_text") or "").lower()
    if any(keyword in review_text for keyword in NEGATIVE_KEYWORDS):
        destinations.append(FILTERED_OUTPUT_DIR / "negative_signals.csv")

    rating = message.get("overall_rating")
    if isinstance(rating, (int, float)) and rating <= LOW_RATING_THRESHOLD:
        destinations.append(FILTERED_OUTPUT_DIR / "low_rated_reviews.csv")

    return destinations
