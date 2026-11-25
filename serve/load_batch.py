"""Part 3 Serve: load the batch output into PostgreSQL.

1. Every ticket from the Parquet written by convert.py goes into the `tickets` table,
   bulk loaded with COPY, with street, hour_of_day and weekday worked out once here.
2. The Spark answers in results/*.csv (from q1.py, q2.py, q3.py) go into the batch_* tables.

Safe to run again: both are replaced, not appended. Stream tables are not touched.
Run from the repository root:  python serve/load_batch.py
"""
import csv
import io
import os
import sys
import time
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parent))
from db import connect, create_indexes, street_from_address  # noqa: E402

PARQUET = os.environ.get("PARQUET", "data/parquet_unpartitioned/tickets.parquet")
RESULTS = Path(os.environ.get("RESULTS_DIR", "results"))
BATCH_ROWS = 200_000

TEXT_COLUMNS = ["tag_number_masked", "date_of_infraction", "time_of_infraction", "infraction_code",
                "infraction_description", "location1", "location2", "location3", "location4", "province"]
COPY_COLUMNS = TEXT_COLUMNS + ["set_fine_amount", "event_time", "street", "hour_of_day", "weekday"]


def prepare(df):
    """Turn one chunk of Parquet rows into the columns of the tickets table."""
    out = pd.DataFrame(index=df.index)
    for c in TEXT_COLUMNS:
        out[c] = df[c].astype("string").str.strip() if c in df.columns else pd.NA
    out["set_fine_amount"] = pd.to_numeric(df.get("set_fine_amount"), errors="coerce").round(2)
    et = pd.to_datetime(df["event_time"], errors="coerce")
    out["event_time"] = et.dt.strftime("%Y-%m-%d %H:%M:%S")
    out["street"] = out["location2"].map(street_from_address, na_action="ignore")
    out["hour_of_day"] = et.dt.hour.astype("Int16")
    out["weekday"] = (et.dt.day_of_week + 1).astype("Int16")       # pandas Monday = 0 -> ISO Monday = 1
    return out[COPY_COLUMNS]


def load_tickets(conn):
    if not os.path.exists(PARQUET):
        raise SystemExit(f"{PARQUET} not found. Run convert.py first.")
    start = time.perf_counter()
    pf = pq.ParquetFile(PARQUET)
    rows_in = pf.metadata.num_rows
    print(f"Loading {rows_in:,} tickets from {PARQUET} ...")

    with conn.cursor() as cur:
        cur.execute("TRUNCATE tickets RESTART IDENTITY")
        # Drop the indexes during the bulk load (much faster), and build them once at the end.
        cur.execute("SELECT indexname FROM pg_indexes WHERE tablename = 'tickets' AND indexname LIKE 'idx_%'")
        for (name,) in cur.fetchall():
            cur.execute(f"DROP INDEX {name}")
        done = 0
        with cur.copy(f"COPY tickets ({', '.join(COPY_COLUMNS)}) FROM STDIN WITH (FORMAT csv, NULL '')") as copy:
            for batch in pf.iter_batches(batch_size=BATCH_ROWS):
                chunk = prepare(batch.to_pandas())
                buf = io.StringIO()
                chunk.to_csv(buf, header=False, index=False, na_rep="")
                copy.write(buf.getvalue())
                done += len(chunk)
                print(f"  {done:,} / {rows_in:,}", end="\r", flush=True)
        print()
    conn.commit()

    print("Building indexes and refreshing statistics ...")
    create_indexes(conn)
    conn.autocommit = True                      # VACUUM cannot run inside a transaction
    conn.execute("VACUUM ANALYZE tickets")      # also marks pages all-visible, so index-only scans work
    conn.autocommit = False

    rows_out = conn.execute("SELECT count(*) FROM tickets").fetchone()[0]
    secs = time.perf_counter() - start
    conn.execute("INSERT INTO batch_load_log (parquet_path, rows_in, rows_out, seconds) VALUES (%s, %s, %s, %s)",
                 (PARQUET, rows_in, rows_out, round(secs, 2)))
    conn.commit()
    print(f"Rows in {rows_in:,} / rows out {rows_out:,}: {'OK' if rows_in == rows_out else 'MISMATCH'}   ({secs:.1f}s)")


def read_csv(name):
    path = RESULTS / name
    if not path.exists():
        print(f"  skipped {path} (not found; run the Spark job that writes it)")
        return None
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def load_spark_results(conn):
    print(f"Loading Spark results from {RESULTS}/ ...")
    with conn.cursor() as cur:
        rows = read_csv("q1_top_streets.csv")
        if rows is not None:
            cur.execute("TRUNCATE batch_q1_top_streets")
            cur.executemany("INSERT INTO batch_q1_top_streets (rank, street, tickets) VALUES (%s, %s, %s)",
                            [(i, r["street"], int(r["count"])) for i, r in enumerate(rows, 1)])
            print(f"  batch_q1_top_streets: {len(rows)} rows")

        rows = read_csv("q2_tickets_by_hour.csv")
        if rows is not None:
            cur.execute("TRUNCATE batch_q2_by_hour")
            cur.executemany("INSERT INTO batch_q2_by_hour (hour_of_day, tickets) VALUES (%s, %s)",
                            [(int(r["hour_of_day"]), int(r["count"])) for r in rows])
            print(f"  batch_q2_by_hour: {len(rows)} rows")

        rows = read_csv("q2_tickets_by_weekday.csv")
        if rows is not None:
            cur.execute("TRUNCATE batch_q2_by_weekday")
            cur.executemany("INSERT INTO batch_q2_by_weekday (weekday, tickets) VALUES (%s, %s)",
                            [(r["weekday"], int(r["count"])) for r in rows])
            print(f"  batch_q2_by_weekday: {len(rows)} rows")

        rows = read_csv("q3_average_fine.csv")
        if rows is not None:
            r = rows[0]
            cur.execute("TRUNCATE batch_q3_average_fine")
            cur.execute("INSERT INTO batch_q3_average_fine (average_fine, tickets_with_fine, total_fines) VALUES (%s, %s, %s)",
                        (r["average_fine"], int(r["tickets_with_fine"]), r["total_fines"]))
            print("  batch_q3_average_fine: 1 row")

        for ranking, name in (("highest_average_fine", "q3_highest_average_fine.csv"),
                              ("most_total_money", "q3_most_total_money.csv")):
            rows = read_csv(name)
            if rows is not None:
                cur.execute("DELETE FROM batch_q3_infractions WHERE ranking = %s", (ranking,))
                cur.executemany(
                    "INSERT INTO batch_q3_infractions (ranking, rank, infraction_code, infraction_description, "
                    "average_fine, tickets, total_fines) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                    [(ranking, i, r["infraction_code"], r["infraction_description"], r["average_fine"],
                      int(r["tickets"]), r["total_fines"]) for i, r in enumerate(rows, 1)])
                print(f"  batch_q3_infractions ({ranking}): {len(rows)} rows")
    conn.commit()


if __name__ == "__main__":
    with connect() as conn:
        load_tickets(conn)
        load_spark_results(conn)
    print("Batch load done.")
