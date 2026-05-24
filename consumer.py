"""Kafka consumer with manual commits, routing, and rolling analytics."""

from __future__ import annotations

import argparse
import json
import logging
import time
from typing import Any

from kafka import KafkaConsumer, OffsetAndMetadata, TopicPartition
from kafka.errors import KafkaError

from analytics import RollingAnalytics
from config import (
    CONSUMER_MAX_POLL_RECORDS,
    CONSUMER_POLL_TIMEOUT_MS,
    CONSUMER_RETRY_ATTEMPTS,
    CONSUMER_RETRY_BACKOFF_SECONDS,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_GROUP_ID,
    KAFKA_TOPIC,
    ensure_directories,
)
from file_writer import RotatingCsvWriter
from logger_config import configure_logging, get_logger
from router import output_record, route_message
from validator import validate_normalized_message


logger = get_logger("consumer")


def create_consumer() -> KafkaConsumer:
    return KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=KAFKA_GROUP_ID,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        max_poll_records=CONSUMER_MAX_POLL_RECORDS,
        value_deserializer=lambda value: value.decode("utf-8"),
        consumer_timeout_ms=0,
    )


def parse_message(raw_value: str) -> dict[str, Any]:
    try:
        message = json.loads(raw_value)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Malformed JSON message: {exc}") from exc

    if not isinstance(message, dict):
        raise ValueError("Kafka message value must decode to a JSON object")

    errors = validate_normalized_message(message)
    if errors:
        raise ValueError(f"Invalid message schema: {'; '.join(errors)}")

    return message


def process_message(message: dict[str, Any], writer: RotatingCsvWriter, analytics: RollingAnalytics) -> list[str]:
    record = output_record(message)
    written_paths: list[str] = []

    for destination in route_message(message):
        _write_with_retry(writer, destination, record)
        written_paths.append(str(destination))

    analytics.update(message)
    return written_paths


def _write_with_retry(writer: RotatingCsvWriter, destination, record: dict[str, Any]) -> None:
    last_error: Exception | None = None
    for attempt in range(1, CONSUMER_RETRY_ATTEMPTS + 1):
        try:
            writer.write(destination, record)
            return
        except Exception as exc:
            last_error = exc
            delay = CONSUMER_RETRY_BACKOFF_SECONDS * (2 ** (attempt - 1))
            logger.error(
                "file_write_failed",
                extra={
                    "destination": str(destination),
                    "attempt": attempt,
                    "max_attempts": CONSUMER_RETRY_ATTEMPTS,
                    "retry_delay_seconds": delay,
                    "error": str(exc),
                },
                exc_info=True,
            )
            if attempt < CONSUMER_RETRY_ATTEMPTS:
                time.sleep(delay)
    raise RuntimeError(f"Failed writing {destination}") from last_error


def commit_message(consumer: KafkaConsumer, record) -> None:
    topic_partition = TopicPartition(record.topic, record.partition)
    offsets = {topic_partition: OffsetAndMetadata(record.offset + 1, None, -1)}
    consumer.commit(offsets=offsets)


def run_consumer(max_messages: int | None = None) -> dict[str, Any]:
    ensure_directories()
    writer = RotatingCsvWriter()
    analytics = RollingAnalytics()
    consumer = create_consumer()
    processed = 0
    malformed = 0
    failed = 0
    start = time.perf_counter()

    logger.info(
        "consumer_started",
        extra={
            "topic": KAFKA_TOPIC,
            "group_id": KAFKA_GROUP_ID,
            "max_poll_records": CONSUMER_MAX_POLL_RECORDS,
        },
    )

    try:
        while True:
            records = consumer.poll(timeout_ms=CONSUMER_POLL_TIMEOUT_MS, max_records=CONSUMER_MAX_POLL_RECORDS)
            if not records:
                if max_messages is not None and processed >= max_messages:
                    break
                continue

            for partition_records in records.values():
                for record in partition_records:
                    try:
                        message = parse_message(record.value)
                        written_paths = process_message(message, writer, analytics)
                        commit_message(consumer, record)
                        processed += 1
                        logger.info(
                            "message_processed",
                            extra={
                                "topic": record.topic,
                                "partition": record.partition,
                                "offset": record.offset,
                                "record_id": message["record_id"],
                                "written_paths": written_paths,
                            },
                        )
                    except ValueError as exc:
                        malformed += 1
                        logger.error(
                            "corrupted_message_handled",
                            extra={
                                "topic": record.topic,
                                "partition": record.partition,
                                "offset": record.offset,
                                "error": str(exc),
                            },
                            exc_info=True,
                        )
                        commit_message(consumer, record)
                    except Exception as exc:
                        failed += 1
                        logger.error(
                            "message_processing_failed",
                            extra={
                                "topic": record.topic,
                                "partition": record.partition,
                                "offset": record.offset,
                                "error": str(exc),
                            },
                            exc_info=True,
                        )
                        raise

                    if max_messages is not None and processed >= max_messages:
                        return _summary(start, processed, malformed, failed, analytics)
    except KeyboardInterrupt:
        logger.info("consumer_shutdown_requested")
    except KafkaError as exc:
        failed += 1
        logger.error("consumer_kafka_error", extra={"error": str(exc)}, exc_info=True)
        raise
    finally:
        consumer.close()
        summary = _summary(start, processed, malformed, failed, analytics)
        logger.info("consumer_summary", extra=summary)

    return _summary(start, processed, malformed, failed, analytics)


def _summary(
    start: float,
    processed: int,
    malformed: int,
    failed: int,
    analytics: RollingAnalytics,
) -> dict[str, Any]:
    elapsed = max(time.perf_counter() - start, 0.000001)
    return {
        "processed_messages": processed,
        "malformed_messages": malformed,
        "failed_messages": failed,
        "elapsed_seconds": round(elapsed, 4),
        "throughput_messages_per_second": round(processed / elapsed, 2),
        "analytics": analytics.snapshot(),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Consume aviation reviews from Kafka.")
    parser.add_argument("--max-messages", type=int, default=None, help="Stop after processing this many valid messages.")
    parser.add_argument("--log-level", default="INFO", help="Python logging level.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    configure_logging(getattr(logging, args.log_level.upper(), logging.INFO))
    run_consumer(max_messages=args.max_messages)


if __name__ == "__main__":
    main()
