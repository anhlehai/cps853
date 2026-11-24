-- Part 3 Serve: one PostgreSQL database holding the batch and the stream results.
-- Docker runs this file the first time the database volume is created, and
-- serve/db.py runs it again on every connect, so every statement is safe to repeat.

-- ---------- Every ticket, bulk loaded from the Parquet written by convert.py ----------
CREATE TABLE IF NOT EXISTS tickets (
    id                      BIGSERIAL PRIMARY KEY,
    tag_number_masked       TEXT,
    date_of_infraction      TEXT,
    time_of_infraction      TEXT,
    infraction_code         TEXT,
    infraction_description  TEXT,
    set_fine_amount         NUMERIC(10, 2),     -- NULL when blank or not a number
    location1               TEXT,
    location2               TEXT,               -- the address, e.g. "NEAR 45 KING ST W"
    location3               TEXT,
    location4               TEXT,
    province                TEXT,
    event_time              TIMESTAMP,          -- date + time of infraction, local clock time
    -- derived once at load time so the three questions read a plain column
    street                  TEXT,               -- location2 without NEAR/OPP prefix and house number (same rule as q1.py)
    hour_of_day             SMALLINT,           -- 0..23
    weekday                 SMALLINT            -- ISO: 1 = Monday .. 7 = Sunday
);

-- ---------- Batch results, copied from the results/*.csv files written by q1.py, q2.py, q3.py (Spark) ----------
CREATE TABLE IF NOT EXISTS batch_q1_top_streets (
    rank      SMALLINT PRIMARY KEY,
    street    TEXT    NOT NULL,
    tickets   BIGINT  NOT NULL
);

CREATE TABLE IF NOT EXISTS batch_q2_by_hour (
    hour_of_day  SMALLINT PRIMARY KEY,
    tickets      BIGINT   NOT NULL
);

CREATE TABLE IF NOT EXISTS batch_q2_by_weekday (
    weekday   TEXT     PRIMARY KEY,           -- "Monday" .. "Sunday"
    tickets   BIGINT   NOT NULL
);

CREATE TABLE IF NOT EXISTS batch_q3_average_fine (
    id                 SMALLINT PRIMARY KEY DEFAULT 1 CHECK (id = 1),   -- one row
    average_fine       NUMERIC(12, 2) NOT NULL,
    tickets_with_fine  BIGINT         NOT NULL,
    total_fines        NUMERIC(16, 2) NOT NULL
);

CREATE TABLE IF NOT EXISTS batch_q3_infractions (
    ranking                 TEXT     NOT NULL,   -- 'highest_average_fine' or 'most_total_money'
    rank                    SMALLINT NOT NULL,
    infraction_code         TEXT,
    infraction_description  TEXT,
    average_fine            NUMERIC(12, 2),
    tickets                 BIGINT,
    total_fines             NUMERIC(16, 2),
    PRIMARY KEY (ranking, rank)
);

CREATE TABLE IF NOT EXISTS batch_load_log (
    loaded_at        TIMESTAMPTZ PRIMARY KEY DEFAULT now(),
    parquet_path     TEXT,
    rows_in          BIGINT,      -- rows read from the Parquet
    rows_out         BIGINT,      -- rows in the tickets table after the load
    seconds          NUMERIC(10, 2)
);

-- ---------- Stream results, written live by serve/consumer_db.py ----------
-- The running result: tickets per street per month (the same result stream/consumer.py prints).
CREATE TABLE IF NOT EXISTS stream_street_month (
    street      TEXT    NOT NULL,
    month       TEXT    NOT NULL,                 -- 'YYYY-MM' or 'Unknown'
    tickets     BIGINT  NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (street, month)
);

-- The Kafka offset each partition has been applied up to. It is written in the same
-- transaction as the counts above, so a restarted consumer neither loses nor double counts rows.
CREATE TABLE IF NOT EXISTS stream_offsets (
    topic       TEXT    NOT NULL,
    partition   INT     NOT NULL,
    next_offset BIGINT  NOT NULL,
    PRIMARY KEY (topic, partition)
);

-- One row: how far the stream has got, for the video and for run_all.py.
CREATE TABLE IF NOT EXISTS stream_progress (
    id               SMALLINT PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    rows_seen        BIGINT      NOT NULL DEFAULT 0,
    last_event_time  TEXT,
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);
