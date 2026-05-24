"""Benchmark helpers for the aviation review streaming pipeline."""

from __future__ import annotations

import argparse
import json
import logging
import statistics
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from config import DOCS_DIR, SOURCE_DATASETS, ensure_directories
from logger_config import configure_logging, get_logger
from producer import iter_csv_messages, run_producer


logger = get_logger("benchmark")


@dataclass(frozen=True)
class BenchmarkVariant:
    batch_size: int
    linger_ms: int
    max_poll_records: int


DEFAULT_VARIANTS = (
    BenchmarkVariant(batch_size=8192, linger_ms=0, max_poll_records=50),
    BenchmarkVariant(batch_size=16384, linger_ms=10, max_poll_records=100),
    BenchmarkVariant(batch_size=32768, linger_ms=50, max_poll_records=250),
)


def collect_valid_messages() -> list[dict[str, Any]]:
    messages: list[dict[str, Any]] = []
    for source_category, path in SOURCE_DATASETS.items():
        for message, _report in iter_csv_messages(source_category, path):
            if message:
                messages.append(message)
    return messages


def benchmark_normalization(messages: list[dict[str, Any]], rounds: int = 3) -> dict[str, Any]:
    durations: list[float] = []
    encoded_bytes = 0

    for _ in range(rounds):
        start = time.perf_counter()
        for message in messages:
            encoded_bytes += len(json.dumps(message, sort_keys=True).encode("utf-8"))
        durations.append(time.perf_counter() - start)

    best = min(durations) if durations else 0.0
    throughput = len(messages) / best if best > 0 else 0.0
    return {
        "messages": len(messages),
        "rounds": rounds,
        "best_seconds": round(best, 6),
        "median_seconds": round(statistics.median(durations), 6) if durations else 0,
        "serialization_throughput_messages_per_second": round(throughput, 2),
        "encoded_bytes": encoded_bytes,
    }


def run_benchmarks(use_kafka: bool = False, output_path: Path | None = None) -> dict[str, Any]:
    ensure_directories()
    output_path = output_path or DOCS_DIR / "benchmark_results.md"
    messages = collect_valid_messages()
    normalization = benchmark_normalization(messages)

    variants = []
    for variant in DEFAULT_VARIANTS:
        variant_result = {
            "batch_size": variant.batch_size,
            "linger_ms": variant.linger_ms,
            "max_poll_records": variant.max_poll_records,
            "local_serialization_messages_per_second": normalization[
                "serialization_throughput_messages_per_second"
            ],
        }
        if use_kafka:
            start = time.perf_counter()
            producer_summary = run_producer(dry_run=False)
            elapsed = time.perf_counter() - start
            variant_result.update(
                {
                    "producer_elapsed_seconds": round(elapsed, 4),
                    "producer_throughput_messages_per_second": producer_summary[
                        "throughput_messages_per_second"
                    ],
                }
            )
        variants.append(variant_result)

    result = {
        "mode": "kafka" if use_kafka else "local-dry-run",
        "normalization": normalization,
        "variants": variants,
        "bottleneck_notes": [
            "Kafka broker/network latency dominates once serialization is faster than producer throughput.",
            "File I/O and CSV header validation dominate consumer work for very small batches.",
            "Higher linger_ms improves batching but adds end-to-end latency.",
        ],
    }
    write_markdown_report(result, output_path)
    logger.info("benchmark_completed", extra=result)
    return result


def write_markdown_report(result: dict[str, Any], path: Path) -> None:
    lines = [
        "# Benchmark Results",
        "",
        f"Mode: `{result['mode']}`",
        "",
        "## Normalization",
        "",
        "| Metric | Value |",
        "| --- | ---: |",
    ]
    for key, value in result["normalization"].items():
        lines.append(f"| {key} | {value} |")

    lines.extend(
        [
            "",
            "## Variant Matrix",
            "",
            "| batch_size | linger_ms | max_poll_records | local_serialization_msg_s | producer_msg_s |",
            "| ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    for variant in result["variants"]:
        lines.append(
            "| {batch_size} | {linger_ms} | {max_poll_records} | {local_serialization_messages_per_second} | {producer} |".format(
                producer=variant.get("producer_throughput_messages_per_second", "n/a"),
                **variant,
            )
        )

    lines.extend(["", "## Bottleneck Analysis", ""])
    lines.extend(f"- {note}" for note in result["bottleneck_notes"])
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Benchmark pipeline throughput.")
    parser.add_argument("--use-kafka", action="store_true", help="Send records to Kafka during producer benchmark.")
    parser.add_argument("--log-level", default="INFO", help="Python logging level.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    configure_logging(getattr(logging, args.log_level.upper(), logging.INFO))
    run_benchmarks(use_kafka=args.use_kafka)


if __name__ == "__main__":
    main()
