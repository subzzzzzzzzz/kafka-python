# Databricks notebook source
# MAGIC %md
# MAGIC # Aviation Reviews Producer
# MAGIC
# MAGIC Databricks producer notebook that reads aviation review CSV files, normalizes each row to JSON,
# MAGIC and streams messages into a Confluent Cloud Kafka topic using `kafka-python`.

# COMMAND ----------

# MAGIC %pip install -r ../requirements.txt

# COMMAND ----------

import csv
import json
import sys
import time
from pathlib import Path
from typing import Any

from kafka import KafkaProducer
from kafka.errors import KafkaError

repo_root = Path.cwd()
if str(repo_root) not in sys.path:
    sys.path.append(str(repo_root))

from shared.config import ConfluentKafkaConfig, PipelinePaths, SOURCE_DATASETS
from shared.logger import configure_logger
from shared.schema import normalize_record

# COMMAND ----------

# Configure these in Databricks as environment variables or secret-backed notebook variables.
# Example:
# spark.conf.set("spark.databricks.cluster.profile", "serverless")
# CONFLUENT_BOOTSTRAP_SERVERS = "pkc-xxxxx.region.provider.confluent.cloud:9092"
# CONFLUENT_TOPIC = "aviation-reviews"
# CONFLUENT_API_KEY = dbutils.secrets.get("aviation-kafka", "api-key")
# CONFLUENT_API_SECRET = dbutils.secrets.get("aviation-kafka", "api-secret")

paths = PipelinePaths.from_env()
paths.local_log_dir.mkdir(parents=True, exist_ok=True)
logger = configure_logger("databricks_producer", paths.local_log_dir / "producer.log")

if "dbutils" in globals():
    dbutils.widgets.text("producer_delay_seconds", "0.25")
    dbutils.widgets.text("max_records", "0")
    producer_delay_seconds = float(dbutils.widgets.get("producer_delay_seconds"))
    max_records = int(dbutils.widgets.get("max_records"))
else:
    producer_delay_seconds = 0.25
    max_records = 0

# COMMAND ----------

def create_producer(kafka_config: ConfluentKafkaConfig) -> KafkaProducer:
    return KafkaProducer(
        **kafka_config.kafka_python_producer_options(),
        value_serializer=lambda value: json.dumps(value, sort_keys=True).encode("utf-8"),
        key_serializer=lambda value: value.encode("utf-8"),
        linger_ms=20,
        batch_size=32768,
    )


def iter_source_records(data_dir: Path) -> Any:
    for source_category, filename in SOURCE_DATASETS.items():
        path = data_dir / filename
        if not path.exists():
            logger.error("dataset_missing", extra={"source_category": source_category, "path": str(path)})
            continue

        with path.open("r", encoding="utf-8-sig", newline="") as file_obj:
            reader = csv.DictReader(file_obj)
            for row_number, row in enumerate(reader, start=2):
                try:
                    yield normalize_record(source_category, row, row_number)
                except Exception as exc:
                    logger.error(
                        "record_normalization_failed",
                        extra={
                            "source_category": source_category,
                            "row_number": row_number,
                            "error": str(exc),
                        },
                        exc_info=True,
                    )


def send_with_retry(
    producer: KafkaProducer,
    topic: str,
    record: dict[str, Any],
    retries: int = 3,
    backoff_seconds: float = 0.5,
) -> None:
    key = f"{record['source_category']}:{record['review_id']}"
    last_error: Exception | None = None

    for attempt in range(1, retries + 1):
        try:
            future = producer.send(topic, key=key, value=record)
            metadata = future.get(timeout=30)
            logger.info(
                "kafka_message_sent",
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
            delay = backoff_seconds * (2 ** (attempt - 1))
            logger.error(
                "kafka_send_failed",
                extra={
                    "review_id": record.get("review_id"),
                    "attempt": attempt,
                    "max_attempts": retries,
                    "retry_delay_seconds": delay,
                    "error": str(exc),
                },
                exc_info=True,
            )
            if attempt < retries:
                time.sleep(delay)

    raise RuntimeError(f"Failed to send record after {retries} attempts") from last_error

# COMMAND ----------

kafka_config = ConfluentKafkaConfig.from_env()
producer = create_producer(kafka_config)

sent = 0
start_time = time.perf_counter()

try:
    for record in iter_source_records(paths.local_data_dir):
        send_with_retry(producer, kafka_config.topic, record)
        sent += 1

        if max_records and sent >= max_records:
            break
        if producer_delay_seconds > 0:
            time.sleep(producer_delay_seconds)
finally:
    producer.flush(timeout=30)
    producer.close()

elapsed = max(time.perf_counter() - start_time, 0.000001)
summary = {
    "messages_sent": sent,
    "elapsed_seconds": round(elapsed, 4),
    "messages_per_second": round(sent / elapsed, 2),
    "topic": kafka_config.topic,
    "bootstrap_servers": kafka_config.bootstrap_servers,
}
logger.info("producer_benchmark_summary", extra=summary)
summary
