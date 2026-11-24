from kafka import KafkaProducer
import pandas as pd
import json
import os
import time

TOPIC = "tickets"

# Optional settings for demos (defaults keep the original behaviour):
#   PRODUCER_LIMIT=50000     send only the first N rows in event time order (0 = all)
#   PRODUCER_DELAY=0         seconds to wait between rows (default 0.001)
#   PRODUCER_LOG_EVERY=1000  print one line per N rows instead of every row
LIMIT = int(os.environ.get("PRODUCER_LIMIT", "0"))
DELAY = float(os.environ.get("PRODUCER_DELAY", "0.001"))
LOG_EVERY = max(1, int(os.environ.get("PRODUCER_LOG_EVERY", "1")))

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

print("Loading parquet...")

df = pd.read_parquet(
    "data/parquet_unpartitioned/tickets.parquet"
)

df = df.sort_values("event_time")
if LIMIT:
    df = df.head(LIMIT)

print(f"Streaming {len(df)} rows")

for i, (_, row) in enumerate(df.iterrows(), 1):

    record = row.to_dict()

    if pd.notna(record["event_time"]):
        record["event_time"] = str(record["event_time"])

    producer.send(TOPIC, record)

    if i % LOG_EVERY == 0:
        print(
            f"Sent ticket at {record['event_time']}"
        )

    if DELAY:
        time.sleep(DELAY)

producer.flush()

print("Done")