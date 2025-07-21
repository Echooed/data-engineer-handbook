-- Drop and recreate the cumulative user activity table
DROP TABLE IF EXISTS users_cumulated;


-- Create a table to store cumulative user activity
CREATE TABLE users_cumulated (
    user_id TEXT,
    dates_active DATE[], -- the list of date that the user was active
    date DATE, -- the currrent date for the user
    PRIMARY KEY (user_id, dates_active)
);


-- Insert cumulative user activity for a new day
-- This is an manual approach that simulates an ETL step that runs each day
INSERT INTO users_cumulated (user_id, dates_active, date) 
WITH yesterday AS (
        SELECT *
        FROM users_cumulated
        WHERE date = DATE('2023-01-30')
    ), -- get yesterday’s snapshot

-- Get today’s activity from raw events
    today AS (
        SELECT user_id::TEXT,
            DATE(event_time) AS date_active
        FROM events
        WHERE DATE(event_time) = DATE '2023-01-31'
            AND user_id IS NOT NULL
        GROUP BY user_id,
            DATE(event_time)
    )

-- Merge today’s activity with yesterday’s snapshot
SELECT COALESCE(t.user_id, y.user_id) AS user_id,
    CASE
        -- if yesterday's activity is null, use today's activity
        WHEN y.dates_active IS NULL THEN ARRAY [t.date_active] 
        -- if today's activity is null, use yesterday's activity
        WHEN t.date_active IS NULL THEN y.dates_active
        -- Otherwise, append today’s date to existing dates
        ELSE ARRAY [t.date_active] || y.dates_active 
    END AS dates_active,
    --advance the snapshot date
    COALESCE(t.date_active, y.date + INTERVAL '1 day') AS date 
FROM today t
FULL OUTER JOIN yesterday y ON t.user_id = y.user_id;



-- Flatten cumulative array to test date membership
-- This helps check if a user was active on specific dates
WITH users AS (
    SELECT user_id,
        dates_active,                                                   -- array of dates this user was active
        date                                                            -- current snapshot date
    FROM users_cumulated
    --WHERE date = DATE '2023-01-31'
),

-- Generate the full date range for date range we care about
series AS (
    SELECT generate_series(
        '2023-01-01'::DATE, 
        '2023-01-31'::DATE, 
        '1 day'::INTERVAL
    ) AS series_date
),


-- For each user-date combo, generate a bit value (placeholder_int_value)
-- If user was active on that date, compute a bitmask representing that date
placeholder_int_value AS (
    SELECT
        CASE   
            WHEN dates_active @> ARRAY[series_date::DATE] 
                THEN POW(2, 32 - (date - DATE(series_date)))::BIGINT
            ELSE 0                                      
        END AS placeholder_int_value,*                                  -- if user was active on the given series_date, compute 2^(32 - day_diff)
    FROM users u
    CROSS JOIN series s 
    -- WHERE user_id = '17358702759623100000'
)

-- Aggregate the bit values for each user
SELECT user_id,
    SUM(placeholder_int_value)::BIGINT::bit(32) AS active_bits,          -- Combine all daily bit values into a single 32-bit value
    BIT_LENGTH(
        SUM(placeholder_int_value)::BIGINT::bit(32))
          AS bit_length,                                                 -- Get total number of bits (should be 32)
    BIT_COUNT(
        SUM(placeholder_int_value)::BIGINT::bit(32)
    ) > 0 AS is_monthly_active,                                          -- Check if user was active on *any* of the 32 days (active in the month)
    BIT_COUNT(
        ('11111110000000000000000000000000')::bit(32) 
        & SUM(placeholder_int_value)::BIGINT::bit(32)
    ) > 0 AS is_weekly_active,                                           -- is user active in the last 7 days
    BIT_COUNT(
        ('10000000000000000000000000000000')::bit(32) 
        & SUM(placeholder_int_value)::BIGINT::bit(32)
    ) > 0 AS is_daily_active                                             -- Check if user was active on the *first day only*
FROM placeholder_int_value
GROUP BY user_id;

------------------------------------------------------------------------
