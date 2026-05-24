"""Central configuration for the aviation review streaming pipeline."""

from __future__ import annotations

import os
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent
DATASET_DIR = BASE_DIR / "datasets"
OUTPUT_DIR = BASE_DIR / "output"
CATEGORY_OUTPUT_DIR = OUTPUT_DIR / "category"
COUNTRY_OUTPUT_DIR = OUTPUT_DIR / "country"
FILTERED_OUTPUT_DIR = OUTPUT_DIR / "filtered"
ROTATED_OUTPUT_DIR = OUTPUT_DIR / "rotated"
DOCS_DIR = BASE_DIR / "docs"
ERROR_LOG_PATH = BASE_DIR / "error.log"

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "aviation-reviews")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "aviation-review-consumers")
KAFKA_TOPIC_PARTITIONS = int(os.getenv("KAFKA_TOPIC_PARTITIONS", "4"))
KAFKA_RETENTION_MS = int(os.getenv("KAFKA_RETENTION_MS", str(24 * 60 * 60 * 1000)))

PRODUCER_BATCH_SIZE = int(os.getenv("PRODUCER_BATCH_SIZE", "16384"))
PRODUCER_LINGER_MS = int(os.getenv("PRODUCER_LINGER_MS", "10"))
PRODUCER_THROTTLE_SECONDS = float(os.getenv("PRODUCER_THROTTLE_SECONDS", "0"))
PRODUCER_RETRIES = int(os.getenv("PRODUCER_RETRIES", "3"))
PRODUCER_RETRY_BACKOFF_SECONDS = float(os.getenv("PRODUCER_RETRY_BACKOFF_SECONDS", "0.5"))
PRODUCER_SEND_TIMEOUT_SECONDS = float(os.getenv("PRODUCER_SEND_TIMEOUT_SECONDS", "30"))

CONSUMER_MAX_POLL_RECORDS = int(os.getenv("CONSUMER_MAX_POLL_RECORDS", "100"))
CONSUMER_POLL_TIMEOUT_MS = int(os.getenv("CONSUMER_POLL_TIMEOUT_MS", "1000"))
CONSUMER_RETRY_ATTEMPTS = int(os.getenv("CONSUMER_RETRY_ATTEMPTS", "3"))
CONSUMER_RETRY_BACKOFF_SECONDS = float(os.getenv("CONSUMER_RETRY_BACKOFF_SECONDS", "0.5"))

FILE_ROTATION_BYTES = int(os.getenv("FILE_ROTATION_BYTES", str(5 * 1024 * 1024)))

SOURCE_DATASETS = {
    "airline": DATASET_DIR / "airlines.csv",
    "airport": DATASET_DIR / "airports.csv",
    "lounge": DATASET_DIR / "lounges.csv",
    "seat": DATASET_DIR / "seats.csv",
}

CATEGORY_FILES = {
    "airline": "airline_reviews.csv",
    "airport": "airport_reviews.csv",
    "lounge": "lounge_reviews.csv",
    "seat": "seat_reviews.csv",
}

NEGATIVE_KEYWORDS = ("delay", "cancelled", "rude", "dirty", "lost", "worst")
LOW_RATING_THRESHOLD = 3.0

CANONICAL_FIELDS = (
    "source_category",
    "record_id",
    "timestamp",
    "author",
    "author_country",
    "overall_rating",
    "review_text",
    "recommended",
    "original_data",
)

OUTPUT_FIELDS = (
    "source_category",
    "record_id",
    "timestamp",
    "author",
    "author_country",
    "overall_rating",
    "recommended",
    "review_text",
)


def ensure_directories() -> None:
    """Create project directories used at runtime."""
    for directory in (
        DATASET_DIR,
        CATEGORY_OUTPUT_DIR,
        COUNTRY_OUTPUT_DIR,
        FILTERED_OUTPUT_DIR,
        ROTATED_OUTPUT_DIR,
        DOCS_DIR,
    ):
        directory.mkdir(parents=True, exist_ok=True)
