## Overview
Dataset: https://open.toronto.ca/dataset/parking-tickets/
Licence: https://open.toronto.ca/open-data-licence/ 

Parking Tickets in Toronto for 2025
Our Questions
1. Which ten streets get the most parking tickets?
2. What hour of the day are the most tickets issued? Which day of the week has the most?
3. What is the average fine, and which infractions cost the most?

The platform has four parts:

- **Part 1, store and batch:** the CSV converted to Parquet, and Spark jobs answering the three questions
- **Part 2, stream:** a producer replaying tickets into Kafka in event time order, and a consumer keeping tickets per street per month
- **Part 3, serve:** one PostgreSQL database holding the batch and stream results, answering the three questions with response times
- **Part 4, quality and monitoring:** checks on every row, row counts at each stage, and bad rows set aside

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

# Part 1: Store and batch

`run_all.py` runs these for you. To run them by hand:

```bash
python convert.py      # quality checks, then CSV -> Parquet (unpartitioned, and partitioned by year/month)
python q1.py           # Spark: ten streets with the most tickets
python q2.py           # Spark: tickets by hour of the day and by weekday
python q3.py           # Spark: average fine, and the costliest infractions
python benchmark.py    # Spark: the three questions timed on CSV vs Parquet
```

`convert.py` prints the size of the CSV and each Parquet layout, and the rows in and out. The Spark answers are saved in `results/`.

In Q3, an infraction needs at least 100 tickets to be ranked by average fine, so a code issued only once can't top the list.

---

# Part 2: Stream

`run_all.py` replays the first 50,000 tickets into Kafka and stores the running result in PostgreSQL (see Part 3). To watch the stream in the terminal instead, start Kafka with `docker compose up -d`, then:

```bash
python stream/consumer.py   # terminal 1: live top 10 tickets per street per month
python stream/producer.py   # terminal 2: replays the tickets in event time order
```

---

# Part 3: Serve (PostgreSQL)

All batch and stream results live in one PostgreSQL database, started by `docker compose up -d` (user, password and database are all `tickets`, on host port 5433 so it doesn't clash with a PostgreSQL already installed on 5432). The tables are created by `serve/init.sql`.

| Table | Filled by | Holds |
|---|---|---|
| `tickets` | `serve/load_batch.py` | every ticket from the Parquet, plus `street`, `hour_of_day` and `weekday` worked out once at load time |
| `batch_q1_top_streets`, `batch_q2_by_hour`, `batch_q2_by_weekday`, `batch_q3_average_fine`, `batch_q3_infractions` | `serve/load_batch.py` | the Spark answers from `results/*.csv` |
| `stream_street_month` | `serve/consumer_db.py` | the stream's running result: tickets per street per month |
| `stream_offsets`, `stream_progress` | `serve/consumer_db.py` | the Kafka offset reached, and rows received so far |

Run the parts by hand (after Part 1 and the Spark jobs):

```bash
python serve/load_batch.py              # Parquet + Spark results -> PostgreSQL
python serve/consumer_db.py --reset     # empty the stream tables and the Kafka topic before a new replay
python serve/consumer_db.py             # terminal 1: Kafka -> PostgreSQL, Ctrl+C to stop
python stream/producer.py               # terminal 2 (PRODUCER_LIMIT, PRODUCER_DELAY, PRODUCER_LOG_EVERY are optional)
python serve/query.py --compare-indexes # answers the three questions with response times
```

`serve/query.py` answers each question with live SQL over every ticket and with a lookup in the Spark result tables, and Q1 also from the stream's running result. Each time is the median of 10 runs. With `--compare-indexes` it times the live SQL without the indexes in `serve/indexes.sql`, then with them. It checks that the database and Spark give the same answers, and saves the times to `results/serve_timings.csv`.

The consumer writes the counts and the Kafka offset in one transaction, so stopping and restarting it neither loses nor double counts rows. `run_all.py` runs `--reset` before every replay, so running the pipeline again doesn't count the same tickets twice.

To look inside the database:

```bash
docker compose exec postgres psql -U tickets -c "SELECT * FROM batch_q1_top_streets"
```

---

# Part 4: Quality and monitoring

`convert.py` runs `quality.py` first. It checks every row of the CSV for a missing date or time, an invalid time, an invalid fine and a missing location, and prints how many rows failed each check.

`convert.py` then sets aside rows without a usable date and time in `data/rejected/bad_event_time.csv`, with the reason, instead of writing them to Parquet. It stops if more than 1% of rows fail, and checks that rows in = rows out + rows set aside.

---

## Stopping

When finished (this stops Kafka and PostgreSQL; `docker compose down -v` also deletes their data):

```bash
docker compose down
```

