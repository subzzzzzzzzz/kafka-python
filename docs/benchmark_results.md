# Benchmark Results

Mode: `local-dry-run`

## Normalization

| Metric | Value |
| --- | ---: |
| messages | 8 |
| rounds | 3 |
| best_seconds | 4.8e-05 |
| median_seconds | 5.1e-05 |
| serialization_throughput_messages_per_second | 167364.02 |
| encoded_bytes | 11634 |

## Variant Matrix

| batch_size | linger_ms | max_poll_records | local_serialization_msg_s | producer_msg_s |
| ---: | ---: | ---: | ---: | ---: |
| 8192 | 0 | 50 | 167364.02 | n/a |
| 16384 | 10 | 100 | 167364.02 | n/a |
| 32768 | 50 | 250 | 167364.02 | n/a |

## Bottleneck Analysis

- Kafka broker/network latency dominates once serialization is faster than producer throughput.
- File I/O and CSV header validation dominate consumer work for very small batches.
- Higher linger_ms improves batching but adds end-to-end latency.
