"""Local benchmark utility for normalization and producer throughput."""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from producer.producer import run_producer  # noqa: E402
from shared.logger import configure_logger  # noqa: E402


logger = configure_logger("benchmark")


def run_benchmark(max_records: int, delay: float, dry_run: bool) -> dict:
    start = time.perf_counter()
    summary = run_producer(delay_seconds=delay, max_records=max_records, dry_run=dry_run)
    elapsed = max(time.perf_counter() - start, 0.000001)
    result = {
        "max_records": max_records,
        "delay_seconds": delay,
        "dry_run": dry_run,
        "elapsed_seconds": round(elapsed, 4),
        "messages_per_second": summary["messages_per_second"],
    }
    logger.info("benchmark_completed", extra=result)
    return result


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Benchmark the local aviation producer.")
    parser.add_argument("--max-records", type=int, default=100)
    parser.add_argument("--delay", type=float, default=0.0)
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    result = run_benchmark(args.max_records, args.delay, args.dry_run)
    print(result)


if __name__ == "__main__":
    main()
