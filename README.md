# Real-Time Aviation Review Streaming Pipeline

A local Kafka streaming/data engineering project for learning producer-consumer architecture, JSON event streaming, routing, filtering, analytics, logging, and benchmarking.

This project runs entirely on your Windows machine.

No Docker. No Databricks. No Confluent Cloud. No Spark cluster.

## Architecture

```text
CSV Files
  -> Local Python Producer
  -> Local Apache Kafka Broker
  -> Local Python Consumer
  -> Analytics / Routing / Output CSVs
```

![Architecture](docs/architecture.png)

## Tech Stack

- Python
- kafka-python
- pandas
- Local Apache Kafka
- JSON messages
- CSV input/output
- Python logging

## Folder Structure

```text
aviation-pipeline/
├── producer/
│   └── producer.py
├── consumer/
│   └── consumer.py
├── analytics/
│   ├── analytics.py
│   └── benchmark.py
├── data/
│   ├── airlines.csv
│   ├── airports.csv
│   ├── lounges.csv
│   └── seats.csv
├── output/
├── logs/
├── shared/
│   ├── config.py
│   ├── schema.py
│   └── logger.py
├── requirements.txt
└── README.md
```

## What The Project Does

The producer reads CSV datasets with pandas, converts each row into a normalized JSON message, and sends messages gradually into Kafka.

The consumer reads messages from Kafka, validates/deserializes JSON, routes records into output CSV files, filters low-rated and negative reviews, and maintains analytics counters.

## Prerequisites

Install these locally:

1. Python 3.10+
2. Java JDK 8, 11, or 17
3. Apache Kafka binary distribution for Windows

Install Python dependencies:

```powershell
cd C:\Users\jahna\Downloads\kafka-python
python -m pip install -r requirements.txt
```

## Start Kafka Locally On Windows

Open a terminal in your Kafka installation folder.

Start ZooKeeper:

```powershell
.\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties
```

Open a second terminal in the Kafka folder.

Start Kafka broker:

```powershell
.\bin\windows\kafka-server-start.bat .\config\server.properties
```

Kafka should now be running on:

```text
localhost:9092
```

## Create Kafka Topic

Open a third terminal in the Kafka folder:

```powershell
.\bin\windows\kafka-topics.bat --bootstrap-server localhost:9092 --create --topic aviation-reviews --partitions 1 --replication-factor 1
```

Verify the topic:

```powershell
.\bin\windows\kafka-topics.bat --bootstrap-server localhost:9092 --describe --topic aviation-reviews
```

## Run Producer

Open a terminal in the project folder:

```powershell
cd C:\Users\jahna\Downloads\kafka-python
python producer\producer.py
```

The producer will:

- Read CSV files from `data/`
- Normalize each row
- Convert the row to JSON
- Send the message to Kafka topic `aviation-reviews`
- Sleep between records to simulate real-time streaming
- Log throughput and send metadata

For a faster demo:

```powershell
python producer\producer.py --delay 0.2
```

Validate records without Kafka:

```powershell
python producer\producer.py --dry-run --delay 0
```

## Run Consumer

Open another project terminal:

```powershell
cd C:\Users\jahna\Downloads\kafka-python
python consumer\consumer.py
```

For the sample data, stop after 8 valid records:

```powershell
python consumer\consumer.py --max-records 8
```

The consumer will:

- Read Kafka messages
- Deserialize JSON
- Validate message fields
- Route records to output CSVs
- Filter low-rated reviews
- Filter negative keyword reviews
- Update analytics counters
- Commit Kafka offsets after successful processing

## Recommended Demo Order

Use four terminals:

1. Kafka terminal: start ZooKeeper
2. Kafka terminal: start Kafka broker
3. Project terminal: start consumer
4. Project terminal: start producer

Commands:

```powershell
python consumer\consumer.py --max-records 8
python producer\producer.py --delay 0.5
```

## Output Files

Generated files appear in `output/`:

```text
output/all_aviation_reviews.csv
output/airline_reviews.csv
output/airport_reviews.csv
output/lounge_reviews.csv
output/seat_reviews.csv
output/low_rated_reviews.csv
output/negative_reviews.csv
output/analytics_summary.csv
```

## Analytics Implemented

The consumer maintains in-memory analytics while messages stream in:

- Total review count
- Review count by category
- Country-wise review counts
- Average rating per airline
- Top airlines by rating
- Low-rated review count

The final snapshot is written to:

```text
output/analytics_summary.csv
```

## Benchmarking

Dry-run benchmark without Kafka:

```powershell
python analytics\benchmark.py --dry-run --delay 0
```

Kafka producer benchmark:

```powershell
python analytics\benchmark.py --delay 0
```

Metrics include:

- elapsed time
- messages sent
- messages/sec

## Logging

Structured logs are written to:

```text
logs/producer.log
logs/consumer.log
logs/benchmark.log
logs/error.log
```

Example log:

```json
{"level":"INFO","logger":"producer","message":"message_sent","topic":"aviation-reviews","partition":0,"offset":12,"review_id":"a1"}
```

## Screenshots Placeholder

Add screenshots here for your evaluator:

- Kafka terminal running
- Producer logs sending messages
- Consumer logs processing messages
- Output CSV files
- Analytics summary CSV

## Troubleshooting

If producer or consumer says Kafka is unavailable:

- Confirm ZooKeeper is running
- Confirm Kafka broker is running
- Confirm topic `aviation-reviews` exists
- Confirm `localhost:9092` is correct

If the topic already exists:

```powershell
.\bin\windows\kafka-topics.bat --bootstrap-server localhost:9092 --list
```

If you want to consume from the beginning again, change `KAFKA_GROUP_ID` in `shared/config.py` or reset Kafka offsets.

## Future Enhancements

- Add chart generation with matplotlib
- Add file rotation for large outputs
- Add a simple dashboard over output CSVs
- Add unit tests for schema normalization
- Add multiple Kafka topics by review category
