# Checkpoint 1 Submission

## Overview

This checkpoint contains the implementation completed for:

- **Part 1:** Batch data processing and Parquet file generation
- **Part 2:** Kafka streaming setup, including producer and consumer components

---

## Requirements

- Python 3.x
- Java 17 or later
- Docker Desktop
- Apache Spark (PySpark)

Install Python dependencies:

```bash
pip install -r requirements.txt
```

---

# Part 1: Data Processing

## Run

Execute the data processing script:

```bash
python convert.py
python benchmark.py
```

## Output

convert.py converts an input of CSV rows into a Parquet files with the format while benchmark.py outputs the comparison of the CSV VS Parquet sizes

---

# Part 2: Kafka Streaming

## Start Kafka

Launch Kafka using Docker:

```bash
docker compose up -d
```

Verify Kafka is running:

```bash
docker ps
```

---

## Start Consumer

Open a terminal and run:

```bash
python stream/consumer.py
```

---

## Start Producer

Open a second terminal and run:

```bash
python stream/producer.py
```

The producer sends records to Kafka and the consumer receives and processes them.

---

## Stopping Kafka

When finished:

```bash
docker compose down
```

---

## Notes

- 