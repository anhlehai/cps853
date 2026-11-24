-- Indexes designed for the three questions. serve/query.py --compare-indexes
-- times every question without these, then with them.
-- Each one covers every column its question reads, so PostgreSQL can answer
-- from the (much smaller) index alone, with an index-only scan, instead of reading the table.

-- Q1 top 10 streets: GROUP BY street
CREATE INDEX IF NOT EXISTS idx_tickets_street ON tickets (street);

-- Q2 busiest hour / busiest weekday: GROUP BY hour_of_day, GROUP BY weekday
CREATE INDEX IF NOT EXISTS idx_tickets_hour ON tickets (hour_of_day);
CREATE INDEX IF NOT EXISTS idx_tickets_weekday ON tickets (weekday);

-- Q3 average fine and costliest infractions: AVG(set_fine_amount) GROUP BY infraction
CREATE INDEX IF NOT EXISTS idx_tickets_infraction_fine
    ON tickets (infraction_code, infraction_description, set_fine_amount);

-- Filtered lookup ("tickets on QUEEN ST W in March 2025"), where an index matters most
CREATE INDEX IF NOT EXISTS idx_tickets_street_time ON tickets (street, event_time);
