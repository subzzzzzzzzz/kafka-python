"""Local Kafka producer for aviation review CSV datasets."""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from pathlib import Path
from typing import Any

import pandas as pd
from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from shared.config import (  # noqa: E402
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    PRODUCER_DELAY_SECONDS,
    PRODUCER_RETRIES,
    PRODUCER_RETRY_BACKOFF_SECONDS,
    PRODUCER_SEND_TIMEOUT_SECONDS,
    SOURCE_DATASETS,
    ensure_directories,
)
from shared.logger import configure_logger  # noqa: E402
from shared.schema import normalize_record, validate_message  # noqa: E402


logger = configure_logger("producer")


def create_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda value: json.dumps(value, sort_keys=True).encode("utf-8"),
        key_serializer=lambda value: value.encode("utf-8"),
        acks="all",
        retries=0,
        linger_ms=10,
        batch_size=16384,
    )


def iter_records(max_records: int | None = None) -> Any:
    emitted = 0
    for source_category, path in SOURCE_DATASETS.items():
        if not path.exists():
            logger.error("dataset_missing", extra={"source_category": source_category, "path": str(path)})
            continue

        frame = pd.read_csv(path).fillna("")
        logger.info(
            "dataset_loaded",
            extra={"source_category": source_category, "path": str(path), "rows": len(frame)},
        )

        for row_index, row in frame.iterrows():
            row_number = int(row_index) + 2
            try:
                record = normalize_record(source_category, row.to_dict(), row_number)
                errors = validate_message(record)
                if errors:
                    logger.error(
                        "record_validation_failed",
                        extra={"source_category": source_category, "row_number": row_number, "errors": errors},
                    )
                    continue
                yield record
                emitted += 1
                if max_records and emitted >= max_records:
                    return
            except Exception as exc:
                logger.error(
                    "record_normalization_failed",
                    extra={"source_category": source_category, "row_number": row_number, "error": str(exc)},
                    exc_info=True,
                )


def send_with_retry(producer: KafkaProducer, topic: str, record: dict[str, Any]) -> None:
    key = f"{record['source_category']}:{record['review_id']}"
    last_error: Exception | None = None

    for attempt in range(1, PRODUCER_RETRIES + 1):
        try:
            future = producer.send(topic, key=key, value=record)
            metadata = future.get(timeout=PRODUCER_SEND_TIMEOUT_SECONDS)
            logger.info(
                "message_sent",
                extra={
                    "topic": metadata.topic,
                    "partition": metadata.partition,
                    "offset": metadata.offset,
                    "review_id": record["review_id"],
                    "source_category": record["source_category"],
                    "attempt": attempt,
                },
            )
            return
        except KafkaError as exc:
            last_error = exc
            delay = PRODUCER_RETRY_BACKOFF_SECONDS * (2 ** (attempt - 1))
            logger.error(
                "producer_send_failed",
                extra={
                    "review_id": record.get("review_id"),
                    "attempt": attempt,
                    "max_attempts": PRODUCER_RETRIES,
                    "retry_delay_seconds": delay,
                    "error": str(exc),
                },
                exc_info=True,
            )
            if attempt < PRODUCER_RETRIES:
                time.sleep(delay)

    raise RuntimeError(f"Failed sending record after {PRODUCER_RETRIES} attempts") from last_error


def run_producer(delay_seconds: float, max_records: int | None = None, dry_run: bool = False) -> dict[str, Any]:
    ensure_directories()
    producer = None if dry_run else create_producer()
    sent = 0
    start = time.perf_counter()

    try:
        for record in iter_records(max_records=max_records):
            if dry_run:
                logger.info("dry_run_record_ready", extra={"review_id": record["review_id"]})
            else:
                send_with_retry(producer, KAFKA_TOPIC, record)
            sent += 1

            if delay_seconds > 0:
                time.sleep(delay_seconds)
    finally:
        if producer:
            producer.flush(timeout=PRODUCER_SEND_TIMEOUT_SECONDS)
            producer.close()

    elapsed = max(time.perf_counter() - start, 0.000001)
    summary = {
        "messages_sent": sent,
        "elapsed_seconds": round(elapsed, 4),
        "messages_per_second": round(sent / elapsed, 2),
        "topic": KAFKA_TOPIC,
        "bootstrap_servers": KAFKA_BOOTSTRAP_SERVERS,
        "dry_run": dry_run,
    }
    logger.info("producer_summary", extra=summary)
    return summary


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Stream local aviation CSV records into Kafka.")
    parser.add_argument("--delay", type=float, default=PRODUCER_DELAY_SECONDS, help="Delay between sends in seconds.")
    parser.add_argument("--max-records", type=int, default=0, help="Optional maximum records to send.")
    parser.add_argument("--dry-run", action="store_true", help="Validate and print records without Kafka.")
    parser.add_argument("--log-level", default="INFO", help="Logging level.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    logger.setLevel(getattr(logging, args.log_level.upper(), logging.INFO))
    max_records = args.max_records if args.max_records > 0 else None

    try:
        run_producer(delay_seconds=args.delay, max_records=max_records, dry_run=args.dry_run)
    except NoBrokersAvailable:
        logger.error(
            "kafka_unavailable",
            extra={"bootstrap_servers": KAFKA_BOOTSTRAP_SERVERS, "hint": "Start local Kafka before running producer."},
            exc_info=True,
        )
        raise


if __name__ == "__main__":
    main()
