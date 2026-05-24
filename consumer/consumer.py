"""Local Kafka consumer for routing and analytics."""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from pathlib import Path
from typing import Any

import pandas as pd
from kafka import KafkaConsumer
from kafka.errors import KafkaError, NoBrokersAvailable

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from analytics.analytics import AviationAnalytics  # noqa: E402
from shared.config import (  # noqa: E402
    CONSUMER_MAX_RECORDS,
    CONSUMER_POLL_TIMEOUT_MS,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_GROUP_ID,
    KAFKA_TOPIC,
    LOW_RATING_THRESHOLD,
    MESSAGE_FIELDS,
    NEGATIVE_KEYWORDS,
    OUTPUT_FILES,
    ensure_directories,
)
from shared.logger import configure_logger  # noqa: E402
from shared.schema import validate_message  # noqa: E402


logger = configure_logger("consumer")


def create_consumer() -> KafkaConsumer:
    return KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=KAFKA_GROUP_ID,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        value_deserializer=lambda value: value.decode("utf-8"),
        key_deserializer=lambda value: value.decode("utf-8") if value else None,
        consumer_timeout_ms=CONSUMER_POLL_TIMEOUT_MS,
    )


def parse_message(raw_value: str) -> dict[str, Any]:
    try:
        record = json.loads(raw_value)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Malformed JSON: {exc}") from exc

    if not isinstance(record, dict):
        raise ValueError("Kafka message must be a JSON object")

    errors = validate_message(record)
    if errors:
        raise ValueError(f"Invalid message schema: {'; '.join(errors)}")

    return record


def append_csv(path: Path, record: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame = pd.DataFrame([{field: record.get(field) for field in MESSAGE_FIELDS}])
    write_header = not path.exists() or path.stat().st_size == 0
    frame.to_csv(path, mode="a", header=write_header, index=False)


def is_negative_review(record: dict[str, Any]) -> bool:
    review = str(record.get("review") or "").lower()
    return any(keyword in review for keyword in NEGATIVE_KEYWORDS)


def route_record(record: dict[str, Any]) -> list[Path]:
    destinations = [OUTPUT_FILES["all_reviews"]]
    category = record["source_category"]
    if category in OUTPUT_FILES:
        destinations.append(OUTPUT_FILES[category])
    if float(record["rating"]) < LOW_RATING_THRESHOLD:
        destinations.append(OUTPUT_FILES["low_rated"])
    if is_negative_review(record):
        destinations.append(OUTPUT_FILES["negative"])
    return destinations


def process_record(record: dict[str, Any], analytics: AviationAnalytics) -> list[str]:
    written = []
    for destination in route_record(record):
        append_csv(destination, record)
        written.append(str(destination))
    analytics.update(record, LOW_RATING_THRESHOLD)
    return written


def write_analytics_summary(analytics: AviationAnalytics) -> None:
    frame = analytics.to_dataframe()
    OUTPUT_FILES["analytics_summary"].parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(OUTPUT_FILES["analytics_summary"], index=False)


def run_consumer(max_records: int | None = None) -> dict[str, Any]:
    ensure_directories()
    analytics = AviationAnalytics()
    consumer = create_consumer()
    processed = 0
    malformed = 0
    start = time.perf_counter()

    logger.info(
        "consumer_started",
        extra={
            "topic": KAFKA_TOPIC,
            "bootstrap_servers": KAFKA_BOOTSTRAP_SERVERS,
            "group_id": KAFKA_GROUP_ID,
        },
    )

    try:
        for message in consumer:
            try:
                record = parse_message(message.value)
                destinations = process_record(record, analytics)
                consumer.commit()
                processed += 1
                logger.info(
                    "message_processed",
                    extra={
                        "topic": message.topic,
                        "partition": message.partition,
                        "offset": message.offset,
                        "review_id": record["review_id"],
                        "destinations": destinations,
                    },
                )
            except ValueError as exc:
                malformed += 1
                consumer.commit()
                logger.error(
                    "malformed_message_skipped",
                    extra={
                        "topic": message.topic,
                        "partition": message.partition,
                        "offset": message.offset,
                        "error": str(exc),
                    },
                    exc_info=True,
                )
            except Exception as exc:
                logger.error(
                    "message_processing_failed",
                    extra={"topic": message.topic, "partition": message.partition, "offset": message.offset, "error": str(exc)},
                    exc_info=True,
                )
                raise

            if max_records and processed >= max_records:
                break
    finally:
        write_analytics_summary(analytics)
        consumer.close()

    elapsed = max(time.perf_counter() - start, 0.000001)
    summary = {
        "messages_processed": processed,
        "malformed_messages": malformed,
        "elapsed_seconds": round(elapsed, 4),
        "messages_per_second": round(processed / elapsed, 2),
        "analytics": analytics.snapshot(),
    }
    logger.info("consumer_summary", extra=summary)
    return summary


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Consume local Kafka aviation messages.")
    parser.add_argument("--max-records", type=int, default=CONSUMER_MAX_RECORDS, help="Stop after this many valid records.")
    parser.add_argument("--log-level", default="INFO", help="Logging level.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    logger.setLevel(getattr(logging, args.log_level.upper(), logging.INFO))
    max_records = args.max_records if args.max_records > 0 else None

    try:
        run_consumer(max_records=max_records)
    except NoBrokersAvailable:
        logger.error(
            "kafka_unavailable",
            extra={"bootstrap_servers": KAFKA_BOOTSTRAP_SERVERS, "hint": "Start local Kafka before running consumer."},
            exc_info=True,
        )
        raise
    except KafkaError as exc:
        logger.error("consumer_kafka_error", extra={"error": str(exc)}, exc_info=True)
        raise


if __name__ == "__main__":
    main()
