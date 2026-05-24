"""Configuration helpers for Databricks + Confluent Cloud Kafka."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[1]
LOCAL_DATA_DIR = PROJECT_ROOT / "data"

DEFAULT_DBFS_BASE = "dbfs:/FileStore/aviation-pipeline"
DEFAULT_LOCAL_DBFS_BASE = "/dbfs/FileStore/aviation-pipeline"


def env(name: str, default: str | None = None, required: bool = False) -> str | None:
    value = os.getenv(name, default)
    if required and not value:
        raise ValueError(f"Missing required environment variable: {name}")
    return value


@dataclass(frozen=True)
class ConfluentKafkaConfig:
    bootstrap_servers: str
    topic: str
    api_key: str
    api_secret: str
    security_protocol: str = "SASL_SSL"
    sasl_mechanism: str = "PLAIN"
    client_id: str = "aviation-databricks-pipeline"

    @classmethod
    def from_env(cls) -> "ConfluentKafkaConfig":
        return cls(
            bootstrap_servers=env("CONFLUENT_BOOTSTRAP_SERVERS", required=True) or "",
            topic=env("CONFLUENT_TOPIC", "aviation-reviews") or "aviation-reviews",
            api_key=env("CONFLUENT_API_KEY", required=True) or "",
            api_secret=env("CONFLUENT_API_SECRET", required=True) or "",
            security_protocol=env("CONFLUENT_SECURITY_PROTOCOL", "SASL_SSL") or "SASL_SSL",
            sasl_mechanism=env("CONFLUENT_SASL_MECHANISM", "PLAIN") or "PLAIN",
            client_id=env("CONFLUENT_CLIENT_ID", "aviation-databricks-pipeline")
            or "aviation-databricks-pipeline",
        )

    def kafka_python_producer_options(self) -> dict[str, Any]:
        return {
            "bootstrap_servers": self.bootstrap_servers,
            "security_protocol": self.security_protocol,
            "sasl_mechanism": self.sasl_mechanism,
            "sasl_plain_username": self.api_key,
            "sasl_plain_password": self.api_secret,
            "client_id": self.client_id,
            "acks": "all",
            "retries": 0,
            "max_in_flight_requests_per_connection": 1,
        }

    def spark_read_options(self) -> dict[str, str]:
        jaas = (
            "org.apache.kafka.common.security.plain.PlainLoginModule required "
            f'username="{self.api_key}" password="{self.api_secret}";'
        )
        return {
            "kafka.bootstrap.servers": self.bootstrap_servers,
            "subscribe": self.topic,
            "startingOffsets": env("KAFKA_STARTING_OFFSETS", "earliest") or "earliest",
            "kafka.security.protocol": self.security_protocol,
            "kafka.sasl.mechanism": self.sasl_mechanism,
            "kafka.sasl.jaas.config": jaas,
            "failOnDataLoss": "false",
        }


@dataclass(frozen=True)
class PipelinePaths:
    dbfs_base: str = DEFAULT_DBFS_BASE
    local_dbfs_base: str = DEFAULT_LOCAL_DBFS_BASE
    local_data_dir: Path = LOCAL_DATA_DIR

    @classmethod
    def from_env(cls) -> "PipelinePaths":
        dbfs_base = env("AVIATION_DBFS_BASE", DEFAULT_DBFS_BASE) or DEFAULT_DBFS_BASE
        local_dbfs_base = dbfs_base.replace("dbfs:", "/dbfs", 1)
        return cls(dbfs_base=dbfs_base, local_dbfs_base=local_dbfs_base)

    @property
    def checkpoint_base(self) -> str:
        return f"{self.dbfs_base}/checkpoints"

    @property
    def delta_base(self) -> str:
        return f"{self.dbfs_base}/delta"

    @property
    def csv_output_base(self) -> str:
        return f"{self.dbfs_base}/csv"

    @property
    def local_log_dir(self) -> Path:
        return Path(self.local_dbfs_base) / "logs"


SOURCE_DATASETS = {
    "airline": "airlines.csv",
    "airport": "airports.csv",
    "lounge": "lounges.csv",
    "seat": "seats.csv",
}

NEGATIVE_KEYWORDS = ("delay", "cancelled", "rude", "dirty", "lost", "worst")
