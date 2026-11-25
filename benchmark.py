"""Deep part (Part 1): the same questions on three storage layouts.  (Spark, local)

Layouts, as in the proposal:
  csv          the raw CSV, parsed and cleaned on every run
  parquet      Parquet without partitioning   (data/parquet_unpartitioned)
  partitioned  Parquet partitioned by year and month   (data/parquet_partitioned_by_month)

Questions: Q1, Q2, Q3, plus a time-filtered query (tickets on Queen St W in March 2025),
where partitioning should matter most: Spark only has to read the month=3 folder.

Each timed run starts from the files on disk (read + compute + collect), averaged over RUNS runs.
It also checks that every layout gives the IDENTICAL answer, so the timing comparison is fair.
Writes results/benchmark_layouts.csv.     BENCH_RUNS=5 python benchmark.py  to change the runs."""
import os, csv, time
from pyspark.sql import SparkSession, functions as F

RAW_CSV = os.environ.get("RAW_CSV", "data/raw/file.csv")  # same file convert.py reads
PARQUET = "data/parquet_unpartitioned/tickets.parquet"
PART_DIR = "data/parquet_partitioned_by_month"
OUT_DIR = "results"
RUNS = int(os.environ.get("BENCH_RUNS", "3"))
FILTER_STREET, FILTER_YEAR, FILTER_MONTH = "QUEEN ST W", 2025, 3

spark = (SparkSession.builder.master("local[*]").appName("benchmark-layouts")
         .config("spark.driver.memory", "2g").config("spark.sql.shuffle.partitions", "8")
         .config("spark.sql.ansi.enabled", "false")       # bad dates become null instead of an error
         .config("spark.sql.session.timeZone", "UTC")     # keep the written clock times; no daylight-saving shifts (2:30 am on the spring-forward day does not exist locally)
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")


# ---------- the three layouts: each returns a DataFrame with the same rows and columns ----------
def load_csv():
    """Raw CSV. Applies the same rule as convert.py, so it keeps the same rows as the Parquet:
    the date must be YYYYMMDD and the time 1-4 digits; rows without a usable event_time are left out."""
    df = (spark.read.option("header", True).option("encoding", "ISO-8859-1").csv(RAW_CSV))   # all text
    df = df.toDF(*[c.strip().lstrip("﻿").lower() for c in df.columns])
    date = F.trim(F.col("date_of_infraction"))
    time_ = F.trim(F.col("time_of_infraction"))
    ok = date.rlike(r"^\d{8}$") & time_.rlike(r"^\d{1,4}$")
    event_time = F.when(ok, F.to_timestamp(F.concat(date, F.lpad(time_, 4, "0")), "yyyyMMddHHmm"))
    return (df.withColumn("event_time", event_time).where(F.col("event_time").isNotNull())
            .withColumn("year", F.year("event_time")).withColumn("month", F.month("event_time")))


def load_parquet():
    return spark.read.parquet(PARQUET)


def load_partitioned():
    # Spark on Windows can't list a folder without Hadoop's winutils.exe, so list the files here
    # and pass them in. basePath makes Spark still read year and month from the folder names
    # (year=2025/month=3/...), so a year/month filter only reads the matching folder.
    files = sorted(os.path.join(r, f) for r, _, fs in os.walk(PART_DIR) for f in fs if f.endswith(".parquet"))
    return spark.read.option("basePath", PART_DIR).parquet(*files)


LAYOUTS = {"csv": load_csv, "parquet": load_parquet, "partitioned": load_partitioned}


# ---------- the questions (identical code for every layout) ----------
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


def q_filtered(df):   # tickets on Queen St W in March 2025, and their total fines
    # Filtering on year and month lets Spark skip every other month's folder in the partitioned layout.
    t = df.where((F.col("year") == FILTER_YEAR) & (F.col("month") == FILTER_MONTH))
    r = (with_street(t).where(F.col("street") == FILTER_STREET)
         .agg(F.count("*").alias("n"), F.sum(F.col("set_fine_amount").cast("double")).alias("fines")).collect()[0])
    return [(r["n"], round(r["fines"] or 0, 2))]


FILTERED = f"Filtered: {FILTER_STREET.title()}, {FILTER_YEAR}-{FILTER_MONTH:02d}"
QUESTIONS = {"Q1 top 10 streets": q1, "Q2 hour and weekday": q2,
             "Q3 average fine, top infractions": q3, FILTERED: q_filtered}


def run_once(layout, question):
    start = time.perf_counter()
    result = QUESTIONS[question](LAYOUTS[layout]())     # re-reads from disk every time
    return time.perf_counter() - start, result


def size_mb(path):
    if os.path.isfile(path):
        return os.path.getsize(path) / 1048576
    return sum(os.path.getsize(os.path.join(r, f)) for r, _, fs in os.walk(path) for f in fs) / 1048576


# ---------- run ----------
for path in (RAW_CSV, PARQUET, PART_DIR):
    if not os.path.exists(path):
        raise SystemExit(f"{path} not found. Run convert.py first.")
sizes = {"csv": size_mb(RAW_CSV), "parquet": size_mb(PARQUET), "partitioned": size_mb(PART_DIR)}
print("Size on disk: " + "   ".join(f"{k} {v:,.1f} MB" for k, v in sizes.items()) + f"   runs per measurement: {RUNS}")
print("Warm-up (not counted): starting Spark and loading classes...")
for layout in LAYOUTS:
    run_once(layout, "Q3 average fine, top infractions")
    run_once(layout, FILTERED)

times, answers = {}, {}
for q in QUESTIONS:
    for layout in LAYOUTS:
        runs = []
        for _ in range(RUNS):
            secs, result = run_once(layout, q)
            runs.append(secs)
        times[(q, layout)], answers[(q, layout)] = runs, result
        print(f"  {q:<34} {layout:<12} " + "  ".join(f"{s:6.2f}s" for s in runs))

avg = lambda xs: sum(xs) / len(xs)
t = {(q, l): avg(times[(q, l)]) for q in QUESTIONS for l in LAYOUTS}
print(f"\n=========== RESULTS (average of {RUNS} runs, seconds) ===========")
print(f"{'Question':<34} {'CSV':>8} {'Parquet':>8} {'Partitnd':>8}  {'CSV/Pq':>7} {'Pq/Part':>7}   Same answer?")
rows_out = []
for q in QUESTIONS:
    c, p, pp = t[(q, "csv")], t[(q, "parquet")], t[(q, "partitioned")]
    same = answers[(q, "csv")] == answers[(q, "parquet")] == answers[(q, "partitioned")]
    print(f"{q:<34} {c:>7.2f}s {p:>7.2f}s {pp:>7.2f}s  {c / p:>6.1f}x {p / pp:>6.2f}x   {'YES' if same else 'NO  <-- check!'}")
    rows_out.append([q, round(c, 3), round(p, 3), round(pp, 3), round(c / p, 2), round(p / pp, 2), same]
                    + [round(x, 3) for l in LAYOUTS for x in times[(q, l)]])
three = list(QUESTIONS)[:3]
tot = {l: sum(t[(q, l)] for q in three) for l in LAYOUTS}
print(f"{'Q1-Q3 together':<34} {tot['csv']:>7.2f}s {tot['parquet']:>7.2f}s {tot['partitioned']:>7.2f}s  "
      f"{tot['csv'] / tot['parquet']:>6.1f}x {tot['parquet'] / tot['partitioned']:>6.2f}x")
print(f"\n{FILTERED} answer (tickets, total fines): {answers[(FILTERED, 'parquet')][0]}")

# ---------- the three success targets from the proposal ----------
smaller = sizes["csv"] / sizes["parquet"]
faster = tot["csv"] / tot["parquet"]
f_unpart, f_part = t[(FILTERED, "parquet")], t[(FILTERED, "partitioned")]
filt = f_unpart / f_part            # "at least 50% faster" read as a speed-up of at least 1.5x
mark = lambda ok: "MET" if ok else "NOT MET"
print("\n=========== PROPOSAL TARGETS ===========")
print(f"Parquet at least 3x smaller than CSV:                {smaller:5.1f}x   {mark(smaller >= 3)}")
print(f"Q1-Q3 at least 2x faster on Parquet than CSV:        {faster:5.1f}x   {mark(faster >= 2)}")
print(f"Filtered query at least 50% faster partitioned       {filt:5.2f}x   {mark(filt >= 1.5)}"
      f"\n  than unpartitioned (speed-up >= 1.5x):            ({f_unpart:.2f}s -> {f_part:.2f}s, {1 - f_part / f_unpart:.0%} less time)")

os.makedirs(OUT_DIR, exist_ok=True)
out = os.path.join(OUT_DIR, "benchmark_layouts.csv")
with open(out, "w", newline="", encoding="utf-8") as f:
    w = csv.writer(f)
    w.writerow(["question", "csv_avg_s", "parquet_avg_s", "partitioned_avg_s", "csv_vs_parquet_speedup",
                "parquet_vs_partitioned_speedup", "same_answer"]
               + [f"{l}_run{i + 1}" for l in LAYOUTS for i in range(RUNS)])
    w.writerows(rows_out)
    w.writerow([])
    w.writerow(["size_mb", round(sizes["csv"], 1), round(sizes["parquet"], 1), round(sizes["partitioned"], 1)])
print(f"\nSaved to {out}")
spark.stop()
