"""Q3: What is the average fine, and which infractions cost the most?  (Spark batch job)"""
import os, csv
from pyspark.sql import SparkSession, functions as F

PARQUET = "data/parquet_unpartitioned/tickets.parquet"   # change if your path differs
OUT_DIR = "results"

spark = (SparkSession.builder.master("local[*]").appName("q3-fines")
         .config("spark.driver.memory", "2g").config("spark.sql.shuffle.partitions", "8").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.parquet(PARQUET)
total = df.count()
# set_fine_amount may be stored as text; cast it. Blank or non-numeric fines become null and are left out.
d = df.withColumn("fine", F.col("set_fine_amount").cast("double"))
priced = d.where(F.col("fine").isNotNull())
no_fine = total - priced.count()

overall = priced.agg(F.avg("fine").alias("avg"), F.sum("fine").alias("sum"), F.min("fine").alias("min"), F.max("fine").alias("max")).collect()[0]

per = (priced.groupBy("infraction_code", "infraction_description")
       .agg(F.round(F.avg("fine"), 2).alias("average_fine"), F.count("*").alias("tickets"),
            F.round(F.sum("fine"), 2).alias("total_fines")))
highest = per.orderBy(F.desc("average_fine"), F.desc("tickets"), "infraction_code").limit(10).collect()
most_money = per.orderBy(F.desc("total_fines"), "infraction_code").limit(10).collect()

print(f"\nTickets read: {total:,}   without a usable fine amount: {no_fine:,}")
print(f"\nAverage fine: ${overall['avg']:,.2f}   (lowest ${overall['min']:,.2f}, highest ${overall['max']:,.2f}, total ${overall['sum']:,.0f})")

def show(title, rows):
    print(f"\n{title}")
    print("Rank  Code  Avg fine   Tickets   Description")
    for i, r in enumerate(rows, 1):
        print(f"{i:>4}  {str(r['infraction_code']):>4}  ${r['average_fine']:>7,.2f}  {r['tickets']:>8,}   {r['infraction_description']}")
show("Infractions with the highest average fine", highest)
show("Infractions that bring in the most money in total", most_money)

os.makedirs(OUT_DIR, exist_ok=True)
with open(os.path.join(OUT_DIR, "q3_average_fine.csv"), "w", newline="", encoding="utf-8") as f:
    w = csv.writer(f); w.writerow(["average_fine", "tickets_with_fine", "total_fines"])
    w.writerow([round(overall["avg"], 2), total - no_fine, round(overall["sum"], 2)])
for name, rows in (("q3_highest_average_fine.csv", highest), ("q3_most_total_money.csv", most_money)):
    with open(os.path.join(OUT_DIR, name), "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f); w.writerow(["infraction_code", "infraction_description", "average_fine", "tickets", "total_fines"])
        for r in rows: w.writerow([r["infraction_code"], r["infraction_description"], r["average_fine"], r["tickets"], r["total_fines"]])
print(f"\nSaved to {OUT_DIR}/q3_average_fine.csv, q3_highest_average_fine.csv, q3_most_total_money.csv")
spark.stop()
