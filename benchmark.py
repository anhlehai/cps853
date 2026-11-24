"""Benchmark: the same three questions on raw CSV vs Parquet.  (Spark, local)
Each timed run starts from the files on disk (read + compute + collect), averaged over RUNS runs.
The CSV side also parses the text and builds event_time on every run, which is the real cost of raw CSV.
It also checks that both layouts give IDENTICAL answers, so the timing comparison is fair."""
import os, csv, time
from pyspark.sql import SparkSession, functions as F

RAW_CSV = "data/raw/file.csv"                             # change if your path differs
PARQUET = "data/parquet_unpartitioned/tickets.parquet"    # change if your path differs
OUT_DIR = "results"
RUNS = int(os.environ.get("BENCH_RUNS", "3"))

spark = (SparkSession.builder.master("local[*]").appName("benchmark-csv-vs-parquet")
         .config("spark.driver.memory", "2g").config("spark.sql.shuffle.partitions", "8")
         .config("spark.sql.ansi.enabled", "false")       # bad dates become null instead of an error
         .config("spark.sql.session.timeZone", "UTC")     # keep the written clock times; no daylight-saving shifts (2:30 am on the spring-forward day does not exist locally)
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")


# ---------- the two layouts: both return a DataFrame with the same columns ----------
def load_csv():
    df = (spark.read.option("header", True).option("encoding", "ISO-8859-1").csv(RAW_CSV))   # all text
    df = df.toDF(*[c.strip().lstrip("﻿").lower() for c in df.columns])
    date = F.trim(F.coalesce(F.col("date_of_infraction"), F.lit("")))
    time_ = F.lpad(F.trim(F.coalesce(F.col("time_of_infraction"), F.lit(""))), 4, "0")
    return df.withColumn("event_time", F.to_timestamp(F.concat(date, time_), "yyyyMMddHHmm"))


def load_parquet():
    return spark.read.parquet(PARQUET)


LAYOUTS = {"csv": load_csv, "parquet": load_parquet}


# ---------- the three questions (identical code for both layouts) ----------
def with_street(df):
    addr = F.upper(F.trim(F.col("location2")))
    addr = F.regexp_replace(addr, r"^(NEAR|NR|OPP|OPPOSITE|AT|O/S|ADJ|BEHIND|REAR OF)\s+", "")
    return df.withColumn("street", F.trim(F.regexp_replace(addr, r"^[0-9]+[A-Z]?(-[0-9]+[A-Z]?)?\s+", "")))


def q1(df):   # ten streets with the most tickets
    rows = (with_street(df).where(F.col("street").isNotNull() & (F.col("street") != ""))
            .groupBy("street").count().orderBy(F.desc("count"), "street").limit(10).collect())
    return [(r["street"], r["count"]) for r in rows]


def q2(df):   # tickets per hour and per weekday
    t = df.where(F.col("event_time").isNotNull())
    hours = t.groupBy(F.hour("event_time").alias("h")).count().orderBy("h").collect()
    days = (t.groupBy(F.date_format("event_time", "EEEE").alias("d")).count().orderBy("d").collect())
    return [("hour", r["h"], r["count"]) for r in hours] + [("day", r["d"], r["count"]) for r in days]


def q3(df):   # average fine, and the ten infractions with the highest average fine
    d = df.withColumn("fine", F.col("set_fine_amount").cast("double")).where(F.col("fine").isNotNull())
    avg = d.agg(F.avg("fine")).collect()[0][0]
    top = (d.groupBy("infraction_code", "infraction_description")
           .agg(F.avg("fine").alias("a"), F.count("*").alias("n"))
           .where(F.col("n") >= 100)          # same minimum as q3.py: rank only infractions with 100+ tickets
           .orderBy(F.desc("a"), F.desc("n"), "infraction_code").limit(10).collect())
    return [("avg", round(avg, 4))] + [(r["infraction_code"], r["infraction_description"], round(r["a"], 4), r["n"]) for r in top]


QUESTIONS = {"Q1 top 10 streets": q1, "Q2 hour and weekday": q2, "Q3 average fine, top infractions": q3}


def run_once(layout, question):
    start = time.perf_counter()
    result = QUESTIONS[question](LAYOUTS[layout]())     # re-reads from disk every time
    return time.perf_counter() - start, result


# ---------- run ----------
print(f"CSV: {os.path.getsize(RAW_CSV) / 1048576:,.1f} MB   Parquet: {os.path.getsize(PARQUET) / 1048576:,.1f} MB   runs per measurement: {RUNS}")
print("Warm-up (not counted): starting Spark and loading classes...")
for layout in LAYOUTS:
    run_once(layout, "Q3 average fine, top infractions")

times, answers = {}, {}
for q in QUESTIONS:
    for layout in LAYOUTS:
        runs = []
        for _ in range(RUNS):
            secs, result = run_once(layout, q)
            runs.append(secs)
        times[(q, layout)], answers[(q, layout)] = runs, result
        print(f"  {q:<34} {layout:<8} " + "  ".join(f"{s:6.2f}s" for s in runs))

avg = lambda xs: sum(xs) / len(xs)
print("\n=========== RESULTS (average seconds) ===========")
print(f"{'Question':<34} {'CSV':>9} {'Parquet':>9} {'Speed-up':>9}   Same answer?")
rows_out, tot_csv, tot_pq = [], 0.0, 0.0
for q in QUESTIONS:
    c, p = avg(times[(q, "csv")]), avg(times[(q, "parquet")])
    same = answers[(q, "csv")] == answers[(q, "parquet")]
    tot_csv, tot_pq = tot_csv + c, tot_pq + p
    print(f"{q:<34} {c:>8.2f}s {p:>8.2f}s {c / p:>8.1f}x   {'YES' if same else 'NO  <-- check!'}")
    rows_out.append([q, round(c, 3), round(p, 3), round(c / p, 2), same] + [round(x, 3) for x in times[(q, 'csv')]] + [round(x, 3) for x in times[(q, 'parquet')]])
print(f"{'All three questions':<34} {tot_csv:>8.2f}s {tot_pq:>8.2f}s {tot_csv / tot_pq:>8.1f}x")
csv_mb, pq_mb = os.path.getsize(RAW_CSV) / 1048576, os.path.getsize(PARQUET) / 1048576
print(f"\nSize: CSV {csv_mb:,.1f} MB, Parquet {pq_mb:,.1f} MB -> {csv_mb / pq_mb:.1f}x smaller")
print(f"Proposal target 'Parquet at least 2x faster': {'MET' if tot_csv / tot_pq >= 2 else 'NOT MET'}   "
      f"'at least 3x smaller': {'MET' if csv_mb / pq_mb >= 3 else 'NOT MET'}")

os.makedirs(OUT_DIR, exist_ok=True)
with open(os.path.join(OUT_DIR, "benchmark_csv_vs_parquet.csv"), "w", newline="", encoding="utf-8") as f:
    w = csv.writer(f)
    w.writerow(["question", "csv_avg_s", "parquet_avg_s", "speedup", "same_answer"] + [f"csv_run{i + 1}" for i in range(RUNS)] + [f"parquet_run{i + 1}" for i in range(RUNS)])
    w.writerows(rows_out)
print(f"Saved to {OUT_DIR}/benchmark_csv_vs_parquet.csv")
spark.stop()
