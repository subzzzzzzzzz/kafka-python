# Benchmark Results

Run `notebooks/benchmark_notebook.py` in Databricks after configuring Confluent Cloud credentials.

## Metrics Captured

| Metric | Description |
| --- | --- |
| records_sent | Number of normalized records sent to Confluent Cloud Kafka |
| elapsed_seconds | Wall-clock producer benchmark time |
| messages_per_second | Producer send throughput |
| spark.streams.active | Active Structured Streaming query progress |

## Example Output

```json
{
  "records_sent": 100,
  "elapsed_seconds": 4.82,
  "messages_per_second": 20.75,
  "topic": "aviation-reviews"
}
```

## Bottleneck Analysis

- Producer throughput depends mainly on Confluent Cloud network latency, `linger_ms`, batch size, and acknowledgements.
- Consumer throughput depends on Spark micro-batch scheduling, Kafka source offsets, checkpoint storage, and sink type.
- Memory sinks are useful for demonstrations; Delta sinks are better for durable analytics.
