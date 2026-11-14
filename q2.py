"""Q2: What hour of the day are the most tickets issued? Which day of the week has the most?  (Spark batch job)"""
import os, csv
from pyspark.sql import SparkSession, functions as F

PARQUET = "data/parquet_unpartitioned/tickets.parquet"   # change if your path differs
OUT_DIR = "results"

spark = (SparkSession.builder.master("local[*]").appName("q2-hour-weekday")
         .config("spark.driver.memory", "2g").config("spark.sql.shuffle.partitions", "8").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.parquet(PARQUET)
total = df.count()
t = df.where(F.col("event_time").isNotNull())                 # rows whose date/time could not be parsed are left out
no_time = total - t.count()
t = t.withColumn("hour_of_day", F.hour("event_time")).withColumn("weekday", F.date_format("event_time", "EEEE"))

by_hour = t.groupBy("hour_of_day").count().orderBy(F.desc("count"), "hour_of_day").collect()
weekday_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
by_day = t.groupBy("weekday").count().orderBy(F.desc("count"), "weekday").collect()

print(f"\nTickets read: {total:,}   without a usable date/time: {no_time:,}")
print("\nTickets by hour of the day (busiest first)")
print("Rank  Hour      Tickets")
for i, r in enumerate(by_hour, 1):
    print(f"{i:>4}  {r['hour_of_day']:02d}:00  {r['count']:>9,}")
print("\nTickets by day of the week (busiest first)")
print("Rank  Day            Tickets")
for i, r in enumerate(by_day, 1):
    print(f"{i:>4}  {r['weekday']:<12} {r['count']:>9,}")
print(f"\nBusiest hour: {by_hour[0]['hour_of_day']:02d}:00    Busiest day: {by_day[0]['weekday']}")

os.makedirs(OUT_DIR, exist_ok=True)
with open(os.path.join(OUT_DIR, "q2_tickets_by_hour.csv"), "w", newline="", encoding="utf-8") as f:
    w = csv.writer(f); w.writerow(["hour_of_day", "count"])
    for r in sorted(by_hour, key=lambda r: r["hour_of_day"]): w.writerow([r["hour_of_day"], r["count"]])
with open(os.path.join(OUT_DIR, "q2_tickets_by_weekday.csv"), "w", newline="", encoding="utf-8") as f:
    w = csv.writer(f); w.writerow(["weekday", "count"])
    for r in sorted(by_day, key=lambda r: weekday_order.index(r["weekday"])): w.writerow([r["weekday"], r["count"]])
print(f"Saved to {OUT_DIR}/q2_tickets_by_hour.csv and {OUT_DIR}/q2_tickets_by_weekday.csv")
spark.stop()
