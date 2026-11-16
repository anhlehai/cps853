from collections import defaultdict
from datetime import datetime
import json
import re
import sys
import time

from kafka import KafkaConsumer


def street_from_address(location):
    if location is None:
        return "Unknown"

    address = str(location).strip().upper()
    if not address or address == "NAN":
        return "Unknown"

    address = re.sub(
        r"^(NEAR|NR|OPP|OPPOSITE|AT|O/S|ADJ|BEHIND|REAR OF)\s+",
        "",
        address,
    )
    street = re.sub(r"^[0-9]+[A-Z]?(-[0-9]+[A-Z]?)?\s+", "", address).strip()
    return street or "Unknown"


def hour_bucket(event_time):
    try:
        timestamp = datetime.fromisoformat(str(event_time).replace("Z", "+00:00"))
    except ValueError:
        return "Unknown"
    return timestamp.replace(minute=0, second=0, microsecond=0).strftime("%Y-%m-%d %H:%M")


consumer = KafkaConsumer(
    "tickets",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
)

group_counts = defaultdict(int)
total_tickets = 0
start_time = time.perf_counter()
last_render_time = start_time
dashboard_line_count = 0
has_unrendered_updates = False

while True:
    for messages in consumer.poll(timeout_ms=100).values():
        for msg in messages:
            ticket = msg.value
            street = street_from_address(ticket.get("location2"))
            hour = hour_bucket(ticket.get("event_time"))
            group_counts[(street, hour)] += 1
            total_tickets += 1
            has_unrendered_updates = True

    now = time.perf_counter()
    if not has_unrendered_updates:
        continue

    if dashboard_line_count and now - last_render_time < 0.5:
        continue

    elapsed_seconds = max(now - start_time, 1e-9)
    processing_rate = total_tickets / elapsed_seconds
    top_groups = sorted(
        group_counts.items(),
        key=lambda item: (-item[1], item[0][1], item[0][0]),
    )[:10]

    lines = [
        f"Total Seen      : {total_tickets:,}",
        "Tickets per Street and Hour (top 10):",
        f"{'Street':<28} {'Hour':>16} {'Count':>8}",
        "-" * 55,
    ]
    lines.extend(
        f"{street[:28]:<28} {hour:>16} {count:>8,}"
        for (street, hour), count in top_groups
    )
    lines.append(f"Processing Rate : {processing_rate:.2f} tickets/sec")
    dashboard = "\n".join(lines)

    if sys.stdout.isatty():
        if dashboard_line_count:
            print(f"\033[{dashboard_line_count}F\033[J", end="")
        print(dashboard, flush=True)
    else:
        print(dashboard, flush=True)

    dashboard_line_count = len(lines)
    last_render_time = now
    has_unrendered_updates = False
