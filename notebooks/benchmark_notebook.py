# Databricks notebook source
# MAGIC %md
# MAGIC # Aviation Pipeline Benchmark Notebook
# MAGIC
# MAGIC Measures producer send throughput and gives a lightweight way to observe streaming query progress.

# COMMAND ----------

# MAGIC %pip install -r ../requirements.txt

# COMMAND ----------

import csv
import json
import sys
import time
from pathlib import Path

from kafka import KafkaProducer

repo_root = Path.cwd()
if str(repo_root) not in sys.path:
    sys.path.append(str(repo_root))

from shared.config import ConfluentKafkaConfig, PipelinePaths, SOURCE_DATASETS
from shared.logger import configure_logger
from shared.schema import normalize_record

# COMMAND ----------

paths = PipelinePaths.from_env()
logger = configure_logger("databricks_benchmark", paths.local_log_dir / "benchmark.log")
kafka_config = ConfluentKafkaConfig.from_env()

if "dbutils" in globals():
    dbutils.widgets.text("benchmark_records", "100")
    benchmark_records = int(dbutils.widgets.get("benchmark_records"))
else:
    benchmark_records = 100

# COMMAND ----------

def load_records(limit: int) -> list[dict]:
    records = []
    for source_category, filename in SOURCE_DATASETS.items():
        path = paths.local_data_dir / filename
        with path.open("r", encoding="utf-8-sig", newline="") as file_obj:
            reader = csv.DictReader(file_obj)
            for row_number, row in enumerate(reader, start=2):
                records.append(normalize_record(source_category, row, row_number))
                if len(records) >= limit:
                    return records
    return records


def create_benchmark_producer() -> KafkaProducer:
    return KafkaProducer(
        **kafka_config.kafka_python_producer_options(),
        value_serializer=lambda value: json.dumps(value, sort_keys=True).encode("utf-8"),
        key_serializer=lambda value: value.encode("utf-8"),
        linger_ms=50,
        batch_size=32768,
    )

# COMMAND ----------

records = load_records(benchmark_records)
producer = create_benchmark_producer()
start = time.perf_counter()

for record in records:
    key = f"benchmark:{record['source_category']}:{record['review_id']}"
    producer.send(kafka_config.topic, key=key, value=record)

producer.flush(timeout=60)
producer.close()

elapsed = max(time.perf_counter() - start, 0.000001)
result = {
    "records_sent": len(records),
    "elapsed_seconds": round(elapsed, 4),
    "messages_per_second": round(len(records) / elapsed, 2),
    "topic": kafka_config.topic,
}

logger.info("producer_benchmark_completed", extra=result)
result

# COMMAND ----------

# MAGIC %md
# MAGIC ## Streaming Query Progress
# MAGIC
# MAGIC After starting `consumer_notebook.py`, inspect `spark.streams.active` from the same cluster.

# COMMAND ----------

[
    {
        "name": query.name,
        "id": str(query.id),
        "isActive": query.isActive,
        "lastProgress": query.lastProgress,
    }
    for query in spark.streams.active
]
