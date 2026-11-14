"""Q1: Which ten streets get the most parking tickets?  (Spark batch job)
Reads the Parquet written by convert.py, counts tickets per street, prints and saves the top ten."""
import os
import csv
from pyspark.sql import SparkSession, functions as F
 
PARQUET = "data/parquet_unpartitioned/tickets.parquet"   # change if your path differs
OUT_DIR = "results"
 
spark = (SparkSession.builder.master("local[*]").appName("q1-top-streets")
         .config("spark.driver.memory", "2g")
         .config("spark.sql.shuffle.partitions", "8")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")
 
df = spark.read.parquet(PARQUET)
total = df.count()
 
# location2 is the address, e.g. "123 QUEEN ST W" or "NEAR 45 KING ST W".
# Street = address without a NEAR/OPP/AT prefix and without the house number.
addr = F.upper(F.trim(F.col("location2")))
addr = F.regexp_replace(addr, r"^(NEAR|NR|OPP|OPPOSITE|AT|O/S|ADJ|BEHIND|REAR OF)\s+", "")
street = F.trim(F.regexp_replace(addr, r"^[0-9]+[A-Z]?(-[0-9]+[A-Z]?)?\s+", ""))
 
tickets = df.withColumn("street", street)
no_street = tickets.where(F.col("street").isNull() | (F.col("street") == "")).count()
 
top10 = (tickets.where(F.col("street").isNotNull() & (F.col("street") != ""))
         .groupBy("street").count()
         .orderBy(F.desc("count"), "street")      # street breaks ties so the result is repeatable
         .limit(10))
 
rows = top10.collect()
print(f"\nTickets read: {total:,}   without a usable street: {no_street:,}\n")
print("Rank  Street                          Tickets")
for i, r in enumerate(rows, 1):
    print(f"{i:>4}  {r['street']:<30}  {r['count']:>8,}")
 
os.makedirs(OUT_DIR, exist_ok=True)

with open(os.path.join(OUT_DIR, "q1_top_streets.csv"), "w", newline="", encoding="utf-8") as f:
    w = csv.writer(f)
    w.writerow(["street", "count"])
    for r in rows:                      # rows = the ten results already collected above
        w.writerow([r["street"], r["count"]])
print(f"\nSaved to {OUT_DIR}/q1_top_streets.csv")

spark.stop()
