"""Run the whole platform end to end, after `docker compose up -d`.

  1. Store and batch: convert.py (with the quality checks), then the Spark jobs q1.py, q2.py, q3.py
  2. Serve, batch:    serve/load_batch.py loads every ticket and the Spark answers into PostgreSQL
  3. Stream:          stream/producer.py replays rows into Kafka while serve/consumer_db.py
                      writes the running result into PostgreSQL
  4. Serve, answers:  serve/query.py answers the three questions from PostgreSQL with response times

  python run_all.py                       # everything, streaming the first 50,000 rows
  python run_all.py --stream-rows 0       # stream every row (slow: about 2M rows)
  python run_all.py --skip-batch          # reuse the Parquet and the database load from a previous run
"""
import argparse
import os
import socket
import subprocess
import sys
import time

PY = sys.executable
RAW_CSV = "data/raw/file.csv"


def step(title):
    print(f"\n{'=' * 70}\n{title}\n{'=' * 70}", flush=True)


def run(*args, env=None):
    print(f"$ python {' '.join(args)}", flush=True)
    subprocess.run([PY, *args], check=True, env=env)


def wait_for_port(host, port, name, seconds=90):
    deadline = time.monotonic() + seconds
    while True:
        try:
            with socket.create_connection((host, port), timeout=2):
                return
        except OSError:
            if time.monotonic() > deadline:
                raise SystemExit(f"{name} is not reachable on {host}:{port}. Did `docker compose up -d` finish?")
            time.sleep(1)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--skip-batch", action="store_true", help="skip convert, Spark and the database load")
    ap.add_argument("--skip-stream", action="store_true", help="skip the Kafka replay")
    ap.add_argument("--stream-rows", type=int, default=50_000, help="rows to replay through Kafka (0 = all)")
    ap.add_argument("--stream-delay", type=float, default=0.0, help="seconds between streamed rows")
    ap.add_argument("--runs", type=int, default=10, help="runs per response time measurement")
    args = ap.parse_args()

    wait_for_port("localhost", 5433, "PostgreSQL")

    if not args.skip_batch:
        if not os.path.exists(RAW_CSV):
            raise SystemExit(f"Put the 2025 parking tickets CSV at {RAW_CSV} first (see README).")
        step("Part 1 + 4: convert CSV to Parquet, with the quality checks")
        run("convert.py")
        step("Part 1: Spark batch jobs")
        for q in ("q1.py", "q2.py", "q3.py"):
            run(q)
        step("Part 3: load the batch output into PostgreSQL")
        run("serve/load_batch.py")

    if not args.skip_stream:
        step(f"Part 2 + 3: stream {'all' if not args.stream_rows else f'{args.stream_rows:,}'} rows into Kafka -> PostgreSQL")
        wait_for_port("localhost", 9092, "Kafka")
        # Each run is a fresh replay: without this, a second run adds the same tickets again.
        for attempt in range(1, 6):               # Kafka can take a few seconds to accept clients after the port opens
            try:
                run("serve/consumer_db.py", "--reset")
                break
            except subprocess.CalledProcessError:
                if attempt == 5:
                    raise
                print("Kafka not ready yet, retrying in 5s ...")
                time.sleep(5)
        consumer = subprocess.Popen([PY, "serve/consumer_db.py", "--idle-exit", "20"])
        env = dict(os.environ, PRODUCER_LIMIT=str(args.stream_rows), PRODUCER_DELAY=str(args.stream_delay),
                   PRODUCER_LOG_EVERY="10000")
        for attempt in range(1, 6):               # Kafka can take a few seconds to accept clients after the port opens
            try:
                run("stream/producer.py", env=env)
                break
            except subprocess.CalledProcessError:
                if attempt == 5:
                    consumer.terminate()
                    raise
                print("Producer could not reach Kafka yet, retrying in 5s ...")
                time.sleep(5)
        print("Producer done; waiting for the consumer to store the last rows ...", flush=True)
        consumer.wait()

    step("Part 3: answer the three questions from PostgreSQL")
    run("serve/query.py", "--compare-indexes", "--runs", str(args.runs))


if __name__ == "__main__":
    main()
