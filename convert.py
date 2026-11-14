import os
import shutil
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# ---------- Paths ----------
RAW_CSV = "data/raw/file.csv"          # change if needed
UNPART_DIR = "data/parquet_unpartitioned"
PART_DIR = "data/parquet_partitioned_by_month"

for d in (UNPART_DIR, PART_DIR):
    shutil.rmtree(d, ignore_errors=True)
    os.makedirs(d, exist_ok=True)

# ---------- 1. Read CSV ----------
print("Reading CSV...")
df = pd.read_csv(RAW_CSV, encoding="latin1", dtype=str)
df.columns = [c.strip().lower() for c in df.columns]
rows_in = len(df)
print(f"Loaded {rows_in:,} rows")

# ---------- 2. Create event_time, year, month, hour ----------
date = df["date_of_infraction"].fillna("").str.strip()
time = df["time_of_infraction"].fillna("").str.strip().str.zfill(4)   # 930 -> 0930, blank -> 0000
df["event_time"] = pd.to_datetime(date + time, format="%Y%m%d%H%M", errors="coerce").astype("datetime64[us]")
df["year"] = df["event_time"].dt.year
df["month"] = df["event_time"].dt.month
df["hour"] = df["event_time"].dt.hour

bad = df["event_time"].isna().sum()
print(f"Rows with unparseable date/time: {bad:,} ({bad / rows_in:.2%})")
if bad > 0.01 * rows_in:
    raise SystemExit("More than 1% of rows failed to parse. Check the sample below.\n"
                     + df[df["event_time"].isna()][["date_of_infraction", "time_of_infraction"]].head(10).to_string())
print(df[["date_of_infraction", "time_of_infraction", "event_time", "month"]].head())

# ---------- 3. Unpartitioned Parquet ----------
unpart_file = os.path.join(UNPART_DIR, "tickets.parquet")
df.to_parquet(unpart_file, compression="snappy", index=False,
              coerce_timestamps="us", allow_truncated_timestamps=True)
print("Unpartitioned Parquet written")

# ---------- 4. Partitioned Parquet (year and month, as in the proposal) ----------
table = pa.Table.from_pandas(df, preserve_index=False)
pq.write_to_dataset(table, root_path=PART_DIR, partition_cols=["year", "month"], compression="snappy")
print("Partitioned Parquet (by year/month) written")

# ---------- 5. Sizes and row counts ----------
def folder_size(path):
    return sum(os.path.getsize(os.path.join(r, f)) for r, _, fs in os.walk(path) for f in fs) / (1024 * 1024)

csv_size = os.path.getsize(RAW_CSV) / (1024 * 1024)
unpart_size = os.path.getsize(unpart_file) / (1024 * 1024)
part_size = folder_size(PART_DIR)
print("\n===== FILE SIZES =====")
print(f"CSV size:                 {csv_size:,.1f} MB")
print(f"Unpartitioned Parquet:    {unpart_size:,.1f} MB")
print(f"Partitioned by month:     {part_size:,.1f} MB")
print(f"Compression ratio:        {csv_size / unpart_size:.1f}x")
rows_out = pq.read_table(unpart_file).num_rows
print(f"Rows in {rows_in:,} / rows out {rows_out:,}: {'OK' if rows_in == rows_out else 'MISMATCH'}")