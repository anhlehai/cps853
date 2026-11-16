from kafka import KafkaProducer
import pandas as pd
import json
import time

TOPIC = "tickets"

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

print("Loading parquet...")

df = pd.read_parquet(
    "data/parquet_unpartitioned/tickets.parquet"
)

df = df.sort_values("event_time")

print(f"Streaming {len(df)} rows")

for _, row in df.iterrows():

    record = row.to_dict()

    if pd.notna(record["event_time"]):
        record["event_time"] = str(record["event_time"])

    producer.send(TOPIC, record)

    print(
        f"Sent ticket at {record['event_time']}"
    )

    time.sleep(0.001)

producer.flush()

print("Done")