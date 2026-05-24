"""Kafka producer for real-time aviation review ingestion."""

from __future__ import annotations

import argparse
import csv
import json
import logging
import time
from pathlib import Path
from typing import Any, Iterable

from kafka import KafkaProducer
from kafka.errors import KafkaError

from config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    PRODUCER_BATCH_SIZE,
    PRODUCER_LINGER_MS,
    PRODUCER_RETRIES,
    PRODUCER_RETRY_BACKOFF_SECONDS,
    PRODUCER_SEND_TIMEOUT_SECONDS,
    PRODUCER_THROTTLE_SECONDS,
    SOURCE_DATASETS,
    ensure_directories,
)
from logger_config import configure_logging, get_logger
from schema import normalize_row
from validator import ValidationReport, validate_normalized_message


logger = get_logger("producer")


def create_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda value: json.dumps(value, sort_keys=True).encode("utf-8"),
        key_serializer=lambda value: value.encode("utf-8"),
        acks="all",
        retries=0,
        batch_size=PRODUCER_BATCH_SIZE,
        linger_ms=PRODUCER_LINGER_MS,
        max_in_flight_requests_per_connection=1,
    )


def iter_csv_messages(source_category: str, path: Path) -> Iterable[tuple[dict[str, Any], ValidationReport]]:
    report = ValidationReport(source_category=source_category)
    if not path.exists():
        report.add_invalid(0, f"Dataset file not found: {path}")
        logger.error("dataset_missing", extra={"source_category": source_category, "path": str(path)})
        yield from ()
        yield {}, report
        return

    with path.open("r", encoding="utf-8-sig", newline="") as file_obj:
        reader = csv.DictReader(file_obj)
        if not reader.fieldnames:
            report.add_invalid(0, "CSV header is missing")
            logger.error("csv_header_missing", extra={"source_category": source_category, "path": str(path)})
            yield {}, report
            return

        for row_number, row in enumerate(reader, start=2):
            try:
                message = normalize_row(source_category, row, row_number=row_number)
                errors = validate_normalized_message(message)
                if errors:
                    report.add_invalid(row_number, "; ".join(errors), row)
                    logger.error(
                        "invalid_csv_row",
                        extra={"source_category": source_category, "row_number": row_number, "errors": errors},
                    )
                    continue
                report.add_valid()
                yield message, report
            except Exception as exc:
                report.add_invalid(row_number, str(exc), row)
                logger.error(
                    "csv_row_normalization_failed",
                    extra={"source_category": source_category, "row_number": row_number, "error": str(exc)},
                    exc_info=True,
                )

    yield {}, report


def send_with_retry(producer: KafkaProducer, message: dict[str, Any]) -> None:
    key = f"{message['source_category']}:{message['record_id']}"
    last_error: Exception | None = None

    for attempt in range(1, PRODUCER_RETRIES + 1):
        try:
            future = producer.send(KAFKA_TOPIC, key=key, value=message)
            future.get(timeout=PRODUCER_SEND_TIMEOUT_SECONDS)
            logger.info(
                "message_sent",
                extra={
                    "topic": KAFKA_TOPIC,
                    "source_category": message["source_category"],
                    "record_id": message["record_id"],
                    "attempt": attempt,
                },
            )
            return
        except (KafkaError, json.JSONDecodeError, TypeError, ValueError) as exc:
            last_error = exc
            delay = PRODUCER_RETRY_BACKOFF_SECONDS * (2 ** (attempt - 1))
            logger.error(
                "producer_send_failed",
                extra={
                    "topic": KAFKA_TOPIC,
                    "record_id": message.get("record_id"),
                    "attempt": attempt,
                    "max_attempts": PRODUCER_RETRIES,
                    "retry_delay_seconds": delay,
                    "error": str(exc),
                },
                exc_info=True,
            )
            if attempt < PRODUCER_RETRIES:
                time.sleep(delay)

    raise RuntimeError(f"Failed to send message after {PRODUCER_RETRIES} attempts") from last_error


def run_producer(dry_run: bool = False) -> dict[str, Any]:
    ensure_directories()
    start = time.perf_counter()
    sent = 0
    reports: dict[str, dict[str, Any]] = {}
    producer = None if dry_run else create_producer()

    try:
        for source_category, path in SOURCE_DATASETS.items():
            latest_report: ValidationReport | None = None
            for message, report in iter_csv_messages(source_category, path):
                latest_report = report
                if not message:
                    continue
                if dry_run:
                    logger.info("dry_run_message_validated", extra={"record_id": message["record_id"]})
                else:
                    send_with_retry(producer, message)
                sent += 1
                if PRODUCER_THROTTLE_SECONDS > 0:
                    time.sleep(PRODUCER_THROTTLE_SECONDS)

            if latest_report:
                reports[source_category] = latest_report.as_dict()

        if producer:
            producer.flush(timeout=PRODUCER_SEND_TIMEOUT_SECONDS)
    finally:
        if producer:
            producer.close()

    elapsed = max(time.perf_counter() - start, 0.000001)
    summary = {
        "sent_messages": sent,
        "elapsed_seconds": round(elapsed, 4),
        "throughput_messages_per_second": round(sent / elapsed, 2),
        "validation_reports": reports,
    }
    logger.info("producer_summary", extra=summary)
    return summary


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Produce normalized aviation reviews to Kafka.")
    parser.add_argument("--dry-run", action="store_true", help="Validate datasets without sending to Kafka.")
    parser.add_argument("--log-level", default="INFO", help="Python logging level.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    configure_logging(getattr(logging, args.log_level.upper(), logging.INFO))
    run_producer(dry_run=args.dry_run)


if __name__ == "__main__":
    main()
