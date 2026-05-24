# Real-Time Aviation Review Streaming Analytics Pipeline

Built a real-time aviation review streaming analytics pipeline using Databricks, Apache Kafka, Confluent Cloud, PySpark Structured Streaming, and Python.

## Target Architecture

```text
CSV Files
  -> Databricks Producer Notebook
  -> Confluent Cloud Kafka Topic
  -> Databricks Streaming Consumer Notebook
  -> PySpark Structured Streaming Analytics
  -> Delta Tables / Memory Tables / CSV Outputs
```

![Architecture](docs/architecture.png)

Kafka remains the central streaming backbone. Databricks is used for both producing test streams and consuming/analyzing the stream with Spark Structured Streaming.

## Project Structure

```text
aviation-pipeline/
├── notebooks/
│   ├── producer_notebook.py
│   ├── consumer_notebook.py
│   └── benchmark_notebook.py
├── shared/
│   ├── config.py
│   ├── logger.py
│   └── schema.py
├── data/
│   ├── airlines.csv
│   ├── airports.csv
│   ├── lounges.csv
│   └── seats.csv
├── output/
├── logs/
├── docs/
│   ├── architecture.png
│   └── benchmark_results.md
├── requirements.txt
├── .env.example
└── README.md
```

## What The Pipeline Does

The producer notebook reads aviation review CSV files from `data/`, normalizes different CSV layouts into a common JSON event, and sends each event to Confluent Cloud Kafka with a configurable delay to simulate real-time ingestion.

The consumer notebook uses:

```python
spark.readStream.format("kafka")
```

It reads Kafka messages, casts `value` to string, parses JSON with a `StructType`, and creates structured streaming DataFrames for analytics and routing.

## Confluent Cloud Setup

Create a Confluent Cloud Kafka cluster and topic:

```text
Topic: aviation-reviews
Partitions: 4
Retention: 24 hours
```

Create a Confluent Cloud API key and secret for the cluster.

You need:

```text
Bootstrap server
API key
API secret
Topic name
```

The code connects with:

```python
security_protocol = "SASL_SSL"
sasl_mechanism = "PLAIN"
```

## Databricks Setup

Use Databricks Repos or upload this project folder into a Databricks workspace.

Attach the notebooks to a Databricks cluster with PySpark support.

Install Python dependencies:

```python
%pip install -r ../requirements.txt
```

Restart Python if Databricks asks you to.

If your Databricks runtime does not already include the Spark Kafka connector, attach this Maven library to the cluster:

```text
org.apache.spark:spark-sql-kafka-0-10_2.12:<your-spark-version>
```

Most modern Databricks runtimes include Kafka support, but this is the first thing to check if `format("kafka")` is not found.

## Configure Secrets

Recommended: store Confluent credentials in Databricks Secrets.

Example secret scope:

```text
Scope: aviation-kafka
Keys:
  api-key
  api-secret
```

In Databricks, set environment variables before running notebooks. You can do this in a notebook cell:

```python
import os

os.environ["CONFLUENT_BOOTSTRAP_SERVERS"] = "pkc-xxxxx.region.provider.confluent.cloud:9092"
os.environ["CONFLUENT_TOPIC"] = "aviation-reviews"
os.environ["CONFLUENT_API_KEY"] = dbutils.secrets.get("aviation-kafka", "api-key")
os.environ["CONFLUENT_API_SECRET"] = dbutils.secrets.get("aviation-kafka", "api-secret")
os.environ["CONFLUENT_SECURITY_PROTOCOL"] = "SASL_SSL"
os.environ["CONFLUENT_SASL_MECHANISM"] = "PLAIN"
os.environ["AVIATION_DBFS_BASE"] = "dbfs:/FileStore/aviation-pipeline"
```

Do not hard-code API secrets in committed files.

## Run Order

Run in this order:

1. Create Confluent Cloud Kafka topic.
2. Open `notebooks/consumer_notebook.py` in Databricks and run it.
3. Open `notebooks/producer_notebook.py` in Databricks and run it.
4. Watch streaming memory tables and output paths update.
5. Run `notebooks/benchmark_notebook.py` for throughput measurements.

## Producer Notebook

File:

```text
notebooks/producer_notebook.py
```

Responsibilities:

- Read `data/airlines.csv`, `airports.csv`, `lounges.csv`, and `seats.csv`
- Normalize heterogeneous CSV rows
- Serialize each row as JSON
- Send records to Confluent Cloud Kafka using `kafka-python`
- Use retry handling around Kafka sends
- Log send metadata including topic, partition, offset, and review ID
- Measure producer throughput in messages/sec

The producer simulates real-time ingestion using `producer_delay_seconds`.

## Consumer Notebook

File:

```text
notebooks/consumer_notebook.py
```

Core Kafka read:

```python
kafka_raw_df = (
    spark.readStream.format("kafka")
    .options(**kafka_config.spark_read_options())
    .load()
)
```

Parsing flow:

```text
Kafka stream
  -> CAST(value AS STRING)
  -> from_json(value, AVIATION_REVIEW_SCHEMA)
  -> structured columns
  -> streaming analytics and outputs
```

## Streaming Analytics

Implemented analytics include:

- Average rating per airline
- Country-wise review counts
- Category-wise review counts
- Low-rated review filtering
- Negative keyword filtering
- Top airlines by rating
- Malformed Kafka message capture

Examples:

```python
avg_airline_ratings_df = (
    reviews_df.filter(col("airline").isNotNull())
    .groupBy("airline")
    .agg(avg("rating").alias("average_rating"), count("*").alias("review_count"))
)

low_rated_reviews_df = reviews_df.filter(col("rating") < 5)

country_counts_df = (
    reviews_df.groupBy("country")
    .agg(count("*").alias("review_count"))
)
```

## Outputs

The consumer writes to:

```text
dbfs:/FileStore/aviation-pipeline/delta/all_reviews
dbfs:/FileStore/aviation-pipeline/delta/malformed_messages
dbfs:/FileStore/aviation-pipeline/csv/low_rated_reviews
dbfs:/FileStore/aviation-pipeline/csv/negative_reviews
```

It also creates memory tables:

```text
avg_airline_ratings
country_review_counts
category_review_counts
top_airlines_by_rating
```

Inspect them in Databricks SQL cells:

```sql
SELECT * FROM avg_airline_ratings ORDER BY average_rating DESC;
SELECT * FROM country_review_counts ORDER BY review_count DESC;
SELECT * FROM category_review_counts ORDER BY review_count DESC;
```

## Logging

Structured JSON logs are written to:

```text
dbfs:/FileStore/aviation-pipeline/logs
```

Examples:

```json
{"level":"INFO","logger":"databricks_producer","message":"kafka_message_sent","topic":"aviation-reviews","partition":1,"offset":42,"review_id":"a1"}
{"level":"INFO","logger":"databricks_streaming_consumer","message":"streaming_batch_processed","batch_id":3,"row_count":8}
```

## Benchmarking

Use:

```text
notebooks/benchmark_notebook.py
```

It measures:

- Records sent
- Elapsed time
- Producer messages/sec
- Active streaming query progress

## Why Localhost Kafka Is Not Used

Databricks cannot connect to Kafka running on your laptop as `localhost:9092`. Inside Databricks, `localhost` means the Databricks driver node, not your machine.

That is why this project uses Confluent Cloud Kafka as the central broker.

## Evaluator Summary

This project implements a cloud-native streaming analytics system. A Databricks producer notebook reads aviation review CSV datasets and publishes normalized JSON events to Confluent Cloud Kafka using SASL_SSL authentication. A Databricks consumer notebook uses Spark Structured Streaming to consume the Kafka topic, parse JSON using a strict schema, perform real-time aggregations and filtering, and write outputs to Delta, CSV, and memory sinks.

## Future Improvements

- Add Delta Live Tables for managed streaming pipelines
- Add schema evolution handling
- Add data quality expectations
- Add dashboarding over Delta output tables
- Add CI checks for shared modules
- Add automated Confluent topic provisioning
