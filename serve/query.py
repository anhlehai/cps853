"""Part 3 Serve: answer the three questions from PostgreSQL, with response times.

For each question it times three ways of answering from the database:
  * live SQL over the `tickets` table (every ticket),
  * a lookup in the batch result tables loaded from Spark,
  * for Q1 also the stream's running result (stream_street_month).
Each time is the median of --runs runs after one warm-up run, measured in the client
(send query, execute, fetch every row). It also checks the SQL answers equal the Spark answers.

  python serve/query.py                     # answers + times with the indexes in place
  python serve/query.py --compare-indexes   # also times the live SQL without the indexes, then with them

Times are saved to results/serve_timings.csv.
"""
import argparse
import csv
import os
import statistics
import sys
import time
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from db import connect, create_indexes, drop_indexes  # noqa: E402

OUT = Path(os.environ.get("RESULTS_DIR", "results")) / "serve_timings.csv"

WEEKDAYS = "ARRAY['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday']"

# ---------- live SQL over every ticket ----------
LIVE = {
    "Q1 top 10 streets": """
        SELECT street, count(*) AS tickets
        FROM tickets WHERE street IS NOT NULL
        GROUP BY street ORDER BY tickets DESC, street LIMIT 10""",
    "Q2a tickets by hour": """
        SELECT hour_of_day, count(*) AS tickets
        FROM tickets WHERE hour_of_day IS NOT NULL
        GROUP BY hour_of_day ORDER BY tickets DESC, hour_of_day""",
    "Q2b tickets by weekday": f"""
        SELECT ({WEEKDAYS})[weekday] AS weekday, count(*) AS tickets
        FROM tickets WHERE weekday IS NOT NULL
        GROUP BY weekday ORDER BY tickets DESC, 1""",
    "Q3a average fine": """
        SELECT round(avg(set_fine_amount), 2) AS average_fine, count(set_fine_amount) AS tickets_with_fine,
               min(set_fine_amount) AS lowest, max(set_fine_amount) AS highest
        FROM tickets""",
    "Q3b highest average fine": """
        SELECT infraction_code, infraction_description, round(avg(set_fine_amount), 2) AS average_fine,
               count(*) AS tickets, sum(set_fine_amount) AS total_fines
        FROM tickets WHERE set_fine_amount IS NOT NULL
        GROUP BY infraction_code, infraction_description
        HAVING count(*) >= 100              -- same minimum as q3.py, so a one-ticket code cannot top the list
        ORDER BY avg(set_fine_amount) DESC, count(*) DESC, infraction_code LIMIT 10""",
    "Q3c most money in total": """
        SELECT infraction_code, infraction_description, round(avg(set_fine_amount), 2) AS average_fine,
               count(*) AS tickets, sum(set_fine_amount) AS total_fines
        FROM tickets WHERE set_fine_amount IS NOT NULL
        GROUP BY infraction_code, infraction_description
        ORDER BY sum(set_fine_amount) DESC, infraction_code LIMIT 10""",
    "Filtered: top street in March 2025": """
        SELECT street, count(*) AS tickets FROM tickets
        WHERE street = (SELECT street FROM batch_q1_top_streets ORDER BY rank LIMIT 1)
          AND event_time >= '2025-03-01' AND event_time < '2025-04-01'
        GROUP BY street""",
}

# ---------- lookups in the batch (Spark) result tables ----------
BATCH = {
    "Q1 top 10 streets": "SELECT street, tickets FROM batch_q1_top_streets ORDER BY rank",
    "Q2a tickets by hour": "SELECT hour_of_day, tickets FROM batch_q2_by_hour ORDER BY tickets DESC, hour_of_day",
    "Q2b tickets by weekday": "SELECT weekday, tickets FROM batch_q2_by_weekday ORDER BY tickets DESC, weekday",
    "Q3a average fine": "SELECT average_fine, tickets_with_fine, total_fines FROM batch_q3_average_fine",
    "Q3b highest average fine": """SELECT infraction_code, infraction_description, average_fine, tickets, total_fines
        FROM batch_q3_infractions WHERE ranking = 'highest_average_fine' ORDER BY rank""",
    "Q3c most money in total": """SELECT infraction_code, infraction_description, average_fine, tickets, total_fines
        FROM batch_q3_infractions WHERE ranking = 'most_total_money' ORDER BY rank""",
}

# ---------- the stream's running result ----------
STREAM = {
    "Q1 top 10 streets": """
        SELECT street, sum(tickets)::bigint AS tickets FROM stream_street_month
        WHERE street <> 'Unknown' GROUP BY street ORDER BY tickets DESC, street LIMIT 10""",
}


def timed(conn, sql, runs):
    """Median client-side response time in ms over `runs` runs, after one warm-up. Returns (ms, rows)."""
    with conn.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()
        times = []
        for _ in range(runs):
            t0 = time.perf_counter()
            cur.execute(sql)
            rows = cur.fetchall()
            times.append((time.perf_counter() - t0) * 1000)
    return statistics.median(times), rows


def fmt(v):
    if isinstance(v, Decimal):
        return f"{v:,.2f}"
    if isinstance(v, int):
        return f"{v:,}"
    return str(v)


def show(rows, limit=24):
    for r in rows[:limit]:
        print("      " + "  |  ".join(fmt(v) for v in r))


def same_answer(live, batch):
    """Do live SQL and Spark agree? Compares the key column and the ticket counts."""
    if not batch:
        return "no Spark result loaded"
    norm = lambda rows: [tuple(str(round(v, 2)) if isinstance(v, (Decimal, float)) else str(v) for v in r) for r in rows]
    return "YES" if norm(live) == norm(batch) else "NO  <-- check!"


def count_rows(conn):
    t = conn.execute("SELECT count(*) FROM tickets").fetchone()[0]
    s = conn.execute("SELECT coalesce(max(rows_seen), 0) FROM stream_progress").fetchone()[0]
    return t, s


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--runs", type=int, default=10)
    ap.add_argument("--compare-indexes", action="store_true")
    args = ap.parse_args()

    conn = connect()
    tickets, streamed = count_rows(conn)
    if not tickets:
        raise SystemExit("The tickets table is empty. Run serve/load_batch.py first.")
    print(f"Database: {tickets:,} tickets from the batch load, {streamed:,} rows received from the stream\n")

    out = []
    no_index = {}
    if args.compare_indexes:
        print(f"Timing live SQL WITHOUT indexes (median of {args.runs}) ...")
        drop_indexes(conn)
        conn.execute("ANALYZE tickets"); conn.commit()
        for q, sql in LIVE.items():
            no_index[q], _ = timed(conn, sql, args.runs)
            print(f"  {q:<36} {no_index[q]:>9.1f} ms")
        print("Building indexes ...\n")
        create_indexes(conn)
        conn.execute("ANALYZE tickets"); conn.commit()

    for q, sql in LIVE.items():
        live_ms, live_rows = timed(conn, sql, args.runs)
        print(f"{q}")
        show(live_rows)
        line = f"    live SQL over all tickets: {live_ms:8.1f} ms"
        if q in no_index:
            line += f"   (without indexes {no_index[q]:.1f} ms, {no_index[q] / live_ms:.1f}x faster with them)"
        print(line)
        row = {"question": q, "live_sql_ms": round(live_ms, 2),
               "live_sql_no_index_ms": round(no_index[q], 2) if q in no_index else ""}
        if q in BATCH:
            b_ms, b_rows = timed(conn, BATCH[q], args.runs)
            check = live_rows if q != "Q3a average fine" else [(r[0], r[1]) for r in live_rows]
            spark = b_rows if q != "Q3a average fine" else [(r[0], r[1]) for r in b_rows]
            match = same_answer(check, spark)
            print(f"    batch result table (Spark): {b_ms:7.2f} ms   same answer as Spark: {match}")
            row.update(batch_table_ms=round(b_ms, 3), same_as_spark=match)
        if q in STREAM:
            s_ms, s_rows = timed(conn, STREAM[q], args.runs)
            print(f"    stream result table: {s_ms:14.2f} ms   (running result after {streamed:,} streamed rows)")
            show(s_rows, 10)
            row["stream_table_ms"] = round(s_ms, 3)
        print()
        out.append(row)

    OUT.parent.mkdir(parents=True, exist_ok=True)
    cols = ["question", "live_sql_ms", "live_sql_no_index_ms", "batch_table_ms", "stream_table_ms", "same_as_spark"]
    with open(OUT, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        w.writerows(out)
    print(f"Saved to {OUT}")
    conn.close()


if __name__ == "__main__":
    main()
