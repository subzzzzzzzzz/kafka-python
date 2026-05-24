# Databricks notebook source
# MAGIC %md
# MAGIC # Aviation Reviews Structured Streaming Consumer
# MAGIC
# MAGIC Databricks consumer notebook that reads from Confluent Cloud Kafka using Spark Structured Streaming,
# MAGIC parses JSON aviation review events, performs streaming analytics, and writes memory, Delta, and CSV outputs.

# COMMAND ----------

# MAGIC %pip install -r ../requirements.txt

# COMMAND ----------

import sys
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql.functions import avg, col, count, current_timestamp, expr, from_json, lower, when

repo_root = Path.cwd()
if str(repo_root) not in sys.path:
    sys.path.append(str(repo_root))

from shared.config import ConfluentKafkaConfig, NEGATIVE_KEYWORDS, PipelinePaths
from shared.logger import configure_logger
from shared.schema import AVIATION_REVIEW_SCHEMA

# COMMAND ----------

paths = PipelinePaths.from_env()
paths.local_log_dir.mkdir(parents=True, exist_ok=True)
logger = configure_logger("databricks_streaming_consumer", paths.local_log_dir / "consumer.log")
kafka_config = ConfluentKafkaConfig.from_env()

logger.info(
    "consumer_notebook_started",
    extra={
        "topic": kafka_config.topic,
        "bootstrap_servers": kafka_config.bootstrap_servers,
        "checkpoint_base": paths.checkpoint_base,
        "delta_base": paths.delta_base,
    },
)

# COMMAND ----------

kafka_raw_df = (
    spark.readStream.format("kafka")
    .options(**kafka_config.spark_read_options())
    .load()
)

json_df = kafka_raw_df.select(
    col("topic"),
    col("partition"),
    col("offset"),
    col("timestamp").alias("kafka_timestamp"),
    col("key").cast("string").alias("kafka_key"),
    col("value").cast("string").alias("json_value"),
)

parsed_df = json_df.withColumn("parsed", from_json(col("json_value"), AVIATION_REVIEW_SCHEMA))

malformed_df = (
    parsed_df.filter(col("parsed").isNull())
    .select("topic", "partition", "offset", "kafka_timestamp", "kafka_key", "json_value")
    .withColumn("processing_time", current_timestamp())
)

reviews_df = (
    parsed_df.filter(col("parsed").isNotNull())
    .select(
        "topic",
        "partition",
        "offset",
        "kafka_timestamp",
        "kafka_key",
        col("parsed.*"),
    )
    .withColumn("processing_time", current_timestamp())
    .withColumn(
        "entity_name",
        when(col("source_category") == "airline", col("airline"))
        .when(col("source_category") == "airport", col("airport"))
        .when(col("source_category") == "lounge", col("lounge"))
        .when(col("source_category") == "seat", col("seat")),
    )
)

# COMMAND ----------

avg_airline_ratings_df = (
    reviews_df.filter(col("airline").isNotNull())
    .groupBy("airline")
    .agg(avg("rating").alias("average_rating"), count("*").alias("review_count"))
    .orderBy(col("average_rating").desc())
)

country_counts_df = (
    reviews_df.groupBy("country")
    .agg(count("*").alias("review_count"))
    .orderBy(col("review_count").desc())
)

low_rated_reviews_df = reviews_df.filter(col("rating") < 5)

negative_expression = " OR ".join(
    [f"lower(review) LIKE '%{keyword}%'" for keyword in NEGATIVE_KEYWORDS]
)
negative_reviews_df = reviews_df.filter(expr(negative_expression))

top_airlines_by_rating_df = avg_airline_ratings_df.filter(col("review_count") >= 1)

category_counts_df = reviews_df.groupBy("source_category").agg(count("*").alias("review_count"))

# COMMAND ----------

def log_batch_metrics(batch_df: DataFrame, batch_id: int) -> None:
    row_count = batch_df.count()
    logger.info(
        "streaming_batch_processed",
        extra={"batch_id": batch_id, "row_count": row_count},
    )


raw_events_query = (
    reviews_df.writeStream.foreachBatch(log_batch_metrics)
    .queryName("aviation_reviews_batch_logger")
    .option("checkpointLocation", f"{paths.checkpoint_base}/batch_logger")
    .start()
)

all_reviews_delta_query = (
    reviews_df.writeStream.format("delta")
    .outputMode("append")
    .queryName("aviation_all_reviews_delta")
    .option("checkpointLocation", f"{paths.checkpoint_base}/all_reviews_delta")
    .start(f"{paths.delta_base}/all_reviews")
)

low_rated_csv_query = (
    low_rated_reviews_df.writeStream.format("csv")
    .outputMode("append")
    .queryName("aviation_low_rated_csv")
    .option("checkpointLocation", f"{paths.checkpoint_base}/low_rated_csv")
    .option("path", f"{paths.csv_output_base}/low_rated_reviews")
    .option("header", "true")
    .start()
)

negative_csv_query = (
    negative_reviews_df.writeStream.format("csv")
    .outputMode("append")
    .queryName("aviation_negative_reviews_csv")
    .option("checkpointLocation", f"{paths.checkpoint_base}/negative_reviews_csv")
    .option("path", f"{paths.csv_output_base}/negative_reviews")
    .option("header", "true")
    .start()
)

malformed_delta_query = (
    malformed_df.writeStream.format("delta")
    .outputMode("append")
    .queryName("aviation_malformed_messages")
    .option("checkpointLocation", f"{paths.checkpoint_base}/malformed_delta")
    .start(f"{paths.delta_base}/malformed_messages")
)

# COMMAND ----------

avg_airline_memory_query = (
    avg_airline_ratings_df.writeStream.format("memory")
    .queryName("avg_airline_ratings")
    .outputMode("complete")
    .start()
)

country_counts_memory_query = (
    country_counts_df.writeStream.format("memory")
    .queryName("country_review_counts")
    .outputMode("complete")
    .start()
)

category_counts_memory_query = (
    category_counts_df.writeStream.format("memory")
    .queryName("category_review_counts")
    .outputMode("complete")
    .start()
)

top_airlines_memory_query = (
    top_airlines_by_rating_df.writeStream.format("memory")
    .queryName("top_airlines_by_rating")
    .outputMode("complete")
    .start()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inspect Live Analytics
# MAGIC
# MAGIC Run these SQL cells while the producer notebook is sending records.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM avg_airline_ratings ORDER BY average_rating DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM country_review_counts ORDER BY review_count DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM category_review_counts ORDER BY review_count DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM top_airlines_by_rating ORDER BY average_rating DESC

# COMMAND ----------

active_queries = [
    raw_events_query,
    all_reviews_delta_query,
    low_rated_csv_query,
    negative_csv_query,
    malformed_delta_query,
    avg_airline_memory_query,
    country_counts_memory_query,
    category_counts_memory_query,
    top_airlines_memory_query,
]

logger.info(
    "streaming_queries_started",
    extra={"query_names": [query.name for query in active_queries]},
)

active_queries

# COMMAND ----------

# To stop all streams during evaluation:
# for query in active_queries:
#     query.stop()
