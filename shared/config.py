"""Local configuration for the aviation Kafka streaming pipeline."""

from __future__ import annotations

import os
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data"
OUTPUT_DIR = PROJECT_ROOT / "output"
LOG_DIR = PROJECT_ROOT / "logs"

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "aviation-reviews")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "aviation-review-local-consumer")

PRODUCER_DELAY_SECONDS = float(os.getenv("PRODUCER_DELAY_SECONDS", "1.0"))
PRODUCER_RETRIES = int(os.getenv("PRODUCER_RETRIES", "3"))
PRODUCER_RETRY_BACKOFF_SECONDS = float(os.getenv("PRODUCER_RETRY_BACKOFF_SECONDS", "0.5"))
PRODUCER_SEND_TIMEOUT_SECONDS = float(os.getenv("PRODUCER_SEND_TIMEOUT_SECONDS", "30"))

CONSUMER_POLL_TIMEOUT_MS = int(os.getenv("CONSUMER_POLL_TIMEOUT_MS", "1000"))
CONSUMER_MAX_RECORDS = int(os.getenv("CONSUMER_MAX_RECORDS", "0"))
LOW_RATING_THRESHOLD = float(os.getenv("LOW_RATING_THRESHOLD", "5.0"))

SOURCE_DATASETS = {
    "airline": DATA_DIR / "airlines.csv",
    "airport": DATA_DIR / "airports.csv",
    "lounge": DATA_DIR / "lounges.csv",
    "seat": DATA_DIR / "seats.csv",
}

OUTPUT_FILES = {
    "all_reviews": OUTPUT_DIR / "all_aviation_reviews.csv",
    "airline": OUTPUT_DIR / "airline_reviews.csv",
    "airport": OUTPUT_DIR / "airport_reviews.csv",
    "lounge": OUTPUT_DIR / "lounge_reviews.csv",
    "seat": OUTPUT_DIR / "seat_reviews.csv",
    "low_rated": OUTPUT_DIR / "low_rated_reviews.csv",
    "negative": OUTPUT_DIR / "negative_reviews.csv",
    "analytics_summary": OUTPUT_DIR / "analytics_summary.csv",
}

NEGATIVE_KEYWORDS = ("delay", "cancelled", "rude", "dirty", "lost", "worst")

MESSAGE_FIELDS = [
    "source_category",
    "review_id",
    "event_time",
    "author",
    "country",
    "rating",
    "review",
    "recommended",
    "airline",
    "airport",
    "lounge",
    "seat",
]


def ensure_directories() -> None:
    for directory in (DATA_DIR, OUTPUT_DIR, LOG_DIR):
        directory.mkdir(parents=True, exist_ok=True)
