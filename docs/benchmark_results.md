# Local Benchmark Results

Run benchmarks from the project root.

Dry run without Kafka:

```powershell
python analytics\benchmark.py --dry-run --delay 0
```

Kafka producer benchmark:

```powershell
python analytics\benchmark.py --delay 0
```

## Metrics Captured

| Metric | Meaning |
| --- | --- |
| max_records | Maximum records attempted |
| delay_seconds | Artificial delay between messages |
| dry_run | Whether Kafka was bypassed |
| elapsed_seconds | Total wall-clock time |
| messages_per_second | Producer throughput |

## Example

```json
{
  "max_records": 100,
  "delay_seconds": 0.0,
  "dry_run": true,
  "elapsed_seconds": 0.12,
  "messages_per_second": 66.7
}
```
