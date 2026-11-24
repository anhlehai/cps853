-- Indexes designed for the three questions. serve/query.py --compare-indexes
-- times every question without these, then with them.
-- Each one covers every column its question reads, so PostgreSQL can answer
-- from the (much smaller) index alone, with an index-only scan, instead of reading the table.

-- Q1 top 10 streets: GROUP BY street
CREATE INDEX IF NOT EXISTS idx_tickets_street ON tickets (street);

-- Q2 busiest hour / busiest weekday: GROUP BY hour_of_day, GROUP BY weekday
CREATE INDEX IF NOT EXISTS idx_tickets_hour ON tickets (hour_of_day);
CREATE INDEX IF NOT EXISTS idx_tickets_weekday ON tickets (weekday);

-- Q3 average fine and costliest infractions: no index. These read every ticket's fine,
-- so an index cannot skip any rows. A covering index on (infraction_code,
-- infraction_description, set_fine_amount) was measured on the 2M-row 2025 data and made
-- Q3 slower (0.6x to 0.9x), so Q3 is served from the Spark result tables instead (< 1 ms).

-- Filtered lookup ("tickets on QUEEN ST W in March 2025"), where an index matters most
CREATE INDEX IF NOT EXISTS idx_tickets_street_time ON tickets (street, event_time);
