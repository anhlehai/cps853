"""Part 3 Serve: Kafka consumer that writes the stream's running result into PostgreSQL.

Reads the `tickets` topic (filled by stream/producer.py) and keeps tickets per street per month
in the stream_street_month table, the same running result stream/consumer.py prints.

Every batch is applied in one transaction together with the Kafka offset it reached
(stream_offsets). On restart the consumer seeks to the stored offsets, so stopping it
(Ctrl+C) and starting it again neither loses nor double counts rows.

Run from the repository root:  python serve/consumer_db.py
Stop after N idle seconds:     python serve/consumer_db.py --idle-exit 20
"""
import argparse
import json
import os
import sys
import time
from collections import defaultdict
from datetime import datetime
from pathlib import Path

from kafka import KafkaConsumer, TopicPartition

sys.path.insert(0, str(Path(__file__).resolve().parent))
from db import connect, street_from_address  # noqa: E402

TOPIC = os.environ.get("KAFKA_TOPIC", "tickets")
BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
FLUSH_ROWS = 2_000          # write to the database every 2,000 rows ...
FLUSH_SECONDS = 1.0         # ... or every second, whichever comes first


def month_bucket(event_time):
    try:
        return datetime.fromisoformat(str(event_time)).strftime("%Y-%m")
    except ValueError:
        return "Unknown"


def assign_and_seek(consumer, conn):
    """Take every partition of the topic and continue from the offsets stored in the database."""
    deadline = time.monotonic() + 60
    while not (parts := consumer.partitions_for_topic(TOPIC)):
        if time.monotonic() > deadline:
            raise SystemExit(f"Topic '{TOPIC}' not found on {BOOTSTRAP}. Start the producer, or check Kafka.")
        consumer.poll(timeout_ms=500)  # refresh metadata
        time.sleep(0.5)
    tps = [TopicPartition(TOPIC, p) for p in sorted(parts)]
    consumer.assign(tps)
    stored = dict(conn.execute("SELECT partition, next_offset FROM stream_offsets WHERE topic = %s", (TOPIC,)).fetchall())
    for tp in tps:
        if tp.partition in stored:
            consumer.seek(tp, stored[tp.partition])
        else:
            consumer.seek_to_beginning(tp)
    print(f"Reading {TOPIC} partitions {sorted(parts)}; resuming at "
          + (", ".join(f"p{p}={o:,}" for p, o in sorted(stored.items())) or "the beginning"))


def flush(conn, counts, offsets, rows, last_event):
    """Apply one batch of counts and the offsets it reached, in a single transaction."""
    with conn.transaction():
        with conn.cursor() as cur:
            cur.executemany(
                """INSERT INTO stream_street_month (street, month, tickets, updated_at)
                   VALUES (%s, %s, %s, now())
                   ON CONFLICT (street, month)
                   DO UPDATE SET tickets = stream_street_month.tickets + EXCLUDED.tickets, updated_at = now()""",
                [(s, m, n) for (s, m), n in counts.items()])
            cur.executemany(
                """INSERT INTO stream_offsets (topic, partition, next_offset) VALUES (%s, %s, %s)
                   ON CONFLICT (topic, partition) DO UPDATE SET next_offset = EXCLUDED.next_offset""",
                [(TOPIC, p, o) for p, o in offsets.items()])
            cur.execute(
                """INSERT INTO stream_progress (id, rows_seen, last_event_time, updated_at) VALUES (1, %s, %s, now())
                   ON CONFLICT (id) DO UPDATE SET rows_seen = stream_progress.rows_seen + EXCLUDED.rows_seen,
                       last_event_time = EXCLUDED.last_event_time, updated_at = now()""",
                (rows, last_event))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--idle-exit", type=float, default=0,
                    help="stop after this many seconds without new rows (0 = run until Ctrl+C)")
    args = ap.parse_args()

    conn = connect(autocommit=True)
    consumer = KafkaConsumer(
        bootstrap_servers=BOOTSTRAP,
        enable_auto_commit=False,          # the database holds the offsets, not Kafka
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    )
    assign_and_seek(consumer, conn)

    counts, offsets = defaultdict(int), {}
    pending, last_event = 0, None
    total = conn.execute("SELECT coalesce(max(rows_seen), 0) FROM stream_progress").fetchone()[0]
    start = last_flush = last_row = time.monotonic()
    session_rows = 0
    try:
        while True:
            for tp, messages in consumer.poll(timeout_ms=200, max_records=5_000).items():
                for msg in messages:
                    t = msg.value
                    street = street_from_address(t.get("location2")) or "Unknown"
                    counts[(street, month_bucket(t.get("event_time")))] += 1
                    offsets[tp.partition] = msg.offset + 1
                    last_event = t.get("event_time")
                    pending += 1
                last_row = time.monotonic()

            now = time.monotonic()
            if pending and (pending >= FLUSH_ROWS or now - last_flush >= FLUSH_SECONDS):
                flush(conn, counts, offsets, pending, last_event)
                total += pending
                session_rows += pending
                rate = session_rows / max(now - start, 1e-9)
                print(f"Stored {total:,} rows in PostgreSQL  (last event {last_event}, {rate:,.0f} rows/s)", flush=True)
                counts.clear()
                pending, last_flush = 0, now

            if args.idle_exit and now - last_row > args.idle_exit:
                print(f"No new rows for {args.idle_exit:.0f}s, stopping.")
                break
    except KeyboardInterrupt:
        print("\nStopping.")
    finally:
        if pending:
            flush(conn, counts, offsets, pending, last_event)
            total += pending
        print(f"Total rows stored from the stream: {total:,}")
        consumer.close()
        conn.close()


if __name__ == "__main__":
    main()
