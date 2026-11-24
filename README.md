## Overview
Dataset: https://open.toronto.ca/dataset/parking-tickets/
Licence: https://open.toronto.ca/open-data-licence/ 

Parking Tickets in Toronto for 2025
Our Questions
1. Which ten streets get the most parking tickets?
2. What hour of the day are the most tickets issued? Which day of the week has the most?
3. What is the average fine, and which infractions cost the most?

This checkpoint contains the implementation completed for:

- **Part 1:** Batch data processing and Parquet file generation
- **Part 2:** Kafka streaming setup, including producer and consumer components
- **Part 3:** PostgreSQL serving database holding the batch and stream results

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

## Quick start (three commands)

Download the 2025 parking tickets CSV from the dataset link above and save it as `data/raw/file.csv`. Then, from the repository root:

```bash
pip install -r requirements.txt
docker compose up -d
python run_all.py
```

`docker compose up -d` starts Kafka and PostgreSQL. `run_all.py` runs every part in order: the quality checks and the CSV to Parquet conversion, the Spark jobs, the load into PostgreSQL, a Kafka replay of the first 50,000 rows into PostgreSQL, and finally the three questions answered by the database with their response times. Use `python run_all.py --stream-rows 0` to stream every row, or `--skip-batch` to reuse an earlier load.

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

# Part 3: Serve (PostgreSQL)

All batch and stream results live in one PostgreSQL database, started by `docker compose up -d` (user, password and database are all `tickets`, on port 5432). The tables are created by `serve/init.sql`.

| Table | Filled by | Holds |
|---|---|---|
| `tickets` | `serve/load_batch.py` | every ticket from the Parquet, plus `street`, `hour_of_day` and `weekday` worked out once at load time |
| `batch_q1_top_streets`, `batch_q2_by_hour`, `batch_q2_by_weekday`, `batch_q3_average_fine`, `batch_q3_infractions` | `serve/load_batch.py` | the Spark answers from `results/*.csv` |
| `stream_street_month` | `serve/consumer_db.py` | the stream's running result: tickets per street per month |
| `stream_offsets`, `stream_progress` | `serve/consumer_db.py` | the Kafka offset reached, and rows received so far |

Run the parts by hand (after Part 1 and the Spark jobs):

```bash
python serve/load_batch.py              # Parquet + Spark results -> PostgreSQL
python serve/consumer_db.py             # terminal 1: Kafka -> PostgreSQL, Ctrl+C to stop
python stream/producer.py               # terminal 2 (PRODUCER_LIMIT, PRODUCER_DELAY, PRODUCER_LOG_EVERY are optional)
python serve/query.py --compare-indexes # answers the three questions with response times
```

`serve/query.py` answers each question with live SQL over every ticket and with a lookup in the Spark result tables, and Q1 also from the stream's running result. Each time is the median of 10 runs. With `--compare-indexes` it times the live SQL without the indexes in `serve/indexes.sql`, then with them. It checks that the database and Spark give the same answers, and saves the times to `results/serve_timings.csv`.

The consumer writes the counts and the Kafka offset in one transaction, so stopping and restarting it neither loses nor double counts rows.

To look inside the database:

```bash
docker compose exec postgres psql -U tickets -c "SELECT * FROM batch_q1_top_streets"
```

---

## Stopping Kafka

When finished (this stops Kafka and PostgreSQL; `docker compose down -v` also deletes their data):

```bash
docker compose down
```

---

## Notes

- 
