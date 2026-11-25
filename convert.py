import os
import shutil
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import subprocess
import sys

# ---------- Paths ----------
RAW_CSV = os.environ.get("RAW_CSV", "data/raw/file.csv")   # change if needed (quality.py reads the same file)
UNPART_DIR = "data/parquet_unpartitioned"
PART_DIR = "data/parquet_partitioned_by_month"
REJECT_DIR = "data/rejected"

for d in (UNPART_DIR, PART_DIR):
    shutil.rmtree(d, ignore_errors=True)
    os.makedirs(d, exist_ok=True)

# ---------- 1. Run quality checks ----------
print("Running Quality & Monitoring checks...")
subprocess.run(
    [sys.executable, "quality.py"],          # same Python as this script (works on Windows too)
    check=True
)



# ---------- 1. Read CSV ----------
print("Reading CSV...")
df = pd.read_csv(RAW_CSV, encoding="latin1", dtype=str)
df.columns = [c.strip().lower() for c in df.columns]
rows_in = len(df)
print(f"Loaded {rows_in:,} rows")

# ---------- 2. Create event_time ----------
date = df["date_of_infraction"].fillna("").str.strip()
time_raw = df["time_of_infraction"].fillna("").str.strip()
# 930 -> 0930. A blank time stays blank, so it fails to parse below instead of becoming midnight.
time = time_raw.where(time_raw == "", time_raw.str.zfill(4))
# Require exactly YYYYMMDD + HHMM: otherwise a blank time lets "20251104" parse as 2025-1-1 00:04.
well_formed = date.str.fullmatch(r"\d{8}") & time.str.fullmatch(r"\d{4}")
df["event_time"] = (pd.to_datetime(date + time, format="%Y%m%d%H%M", errors="coerce")
                    .where(well_formed).astype("datetime64[us]"))

# ---------- 3. Set aside rows without a usable event time ----------
# They cannot be placed in time (Q2, partitions, the stream replay), so they go to their own
# file with the reason, instead of being dropped silently or written into the Parquet.
bad_mask = df["event_time"].isna()
bad = int(bad_mask.sum())
print(f"Rows with unusable date/time: {bad:,} ({bad / rows_in:.2%})")
if bad > 0.01 * rows_in:
    raise SystemExit("More than 1% of rows failed to parse. Check the sample below.\n"
                     + df[bad_mask][["date_of_infraction", "time_of_infraction"]].head(10).to_string())

rejected = df[bad_mask].drop(columns=["event_time"]).copy()
rejected.insert(0, "reason", (date[bad_mask].eq("") | time_raw[bad_mask].eq(""))
                .map({True: "blank date or time", False: "invalid date or time"}))
shutil.rmtree(REJECT_DIR, ignore_errors=True)
os.makedirs(REJECT_DIR, exist_ok=True)
reject_file = os.path.join(REJECT_DIR, "bad_event_time.csv")
rejected.to_csv(reject_file, index=False, encoding="utf-8")
print(f"Set aside {bad:,} rows in {reject_file}")
if bad:
    print(rejected["reason"].value_counts().to_string())

df = df[~bad_mask].reset_index(drop=True)
df["year"] = df["event_time"].dt.year.astype("int32")
df["month"] = df["event_time"].dt.month.astype("int32")
df["hour"] = df["event_time"].dt.hour.astype("int32")
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
print(f"Rows in {rows_in:,} = rows out {rows_out:,} + set aside {bad:,}: "
      f"{'OK' if rows_in == rows_out + bad else 'MISMATCH'}")