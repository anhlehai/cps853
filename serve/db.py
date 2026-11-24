"""Shared helpers for Part 3 Serve: connecting to PostgreSQL and the street rule."""
import os
import re
import time
from pathlib import Path

import psycopg

HERE = Path(__file__).resolve().parent

# Matches the postgres service in docker-compose.yml. Override with DATABASE_URL if needed.
DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://tickets:tickets@localhost:5432/tickets")

# Same rule as q1.py, benchmark.py and stream/consumer.py, so all parts agree on what a street is.
_PREFIX = re.compile(r"^(NEAR|NR|OPP|OPPOSITE|AT|O/S|ADJ|BEHIND|REAR OF)\s+")
_NUMBER = re.compile(r"^[0-9]+[A-Z]?(-[0-9]+[A-Z]?)?\s+")


def street_from_address(location):
    """'NEAR 45 KING ST W' -> 'KING ST W'. Returns None when there is no usable street."""
    if location is None:
        return None
    address = str(location).strip().upper()
    if not address or address == "NAN":
        return None
    street = _NUMBER.sub("", _PREFIX.sub("", address)).strip()
    return street or None


def connect(wait_seconds=60, autocommit=False):
    """Connect to the database, waiting for the container to come up, and make sure the tables exist."""
    deadline = time.monotonic() + wait_seconds
    while True:
        try:
            conn = psycopg.connect(DATABASE_URL, autocommit=autocommit)
            break
        except psycopg.OperationalError:
            if time.monotonic() > deadline:
                raise SystemExit(f"Could not reach PostgreSQL at {DATABASE_URL}. Is `docker compose up -d` running?")
            time.sleep(1)
    with conn.cursor() as cur:
        cur.execute((HERE / "init.sql").read_text())
    if not autocommit:
        conn.commit()
    return conn


def create_indexes(conn):
    with conn.cursor() as cur:
        cur.execute((HERE / "indexes.sql").read_text())
    conn.commit()


def drop_indexes(conn):
    names = re.findall(r"CREATE INDEX IF NOT EXISTS (\w+)", (HERE / "indexes.sql").read_text())
    with conn.cursor() as cur:
        for name in names:
            cur.execute(f"DROP INDEX IF EXISTS {name}")
    conn.commit()
