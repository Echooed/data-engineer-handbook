-- Drop and recreate the user_devices_cumulated table
-- This table stores a per-user, per-browser record of active dates as an array and the snapshot date
DROP TABLE IF EXISTS user_devices_cumulated;
CREATE TABLE user_devices_cumulated (
    user_id TEXT,
    browser_type TEXT,
    dates_active DATE[],
    snapshot_date DATE,
    PRIMARY KEY (user_id, browser_type, snapshot_date)
);


-- Deduplicate raw events using ROW_NUMBER()
-- Ensure we only keep one event per user-device per timestamp to avoid duplication in daily activity
WITH deduped AS (
    SELECT 
        e.user_id,
        e.device_id,
        d.device_type,
        d.browser_type,
        e.event_time,
        ROW_NUMBER() OVER (PARTITION BY e.user_id, e.device_id, e.event_time ORDER BY e.event_time) AS rn
    FROM events e 
    LEFT JOIN devices d ON e.device_id = d.device_id
    WHERE e.user_id IS NOT NULL AND e.device_id IS NOT NULL
),

-- Load yesterday’s snapshot to accumulate prior activity
yesterday AS (
    SELECT * FROM user_devices_cumulated
    WHERE snapshot_date = DATE '2023-01-30'
),

-- Extract today's deduplicated activity (Jan 31), grouped by user and browser
today AS (
    SELECT
        user_id::TEXT,
        browser_type,
        DATE(event_time) AS date_active
    FROM deduped
    WHERE rn = 1  -- Keep only first occurrence per user-device-time
        AND DATE(event_time) = DATE '2023-01-31'
        AND browser_type IS NOT NULL
    GROUP BY user_id, browser_type, DATE(event_time)
)

--Merge today's activity with yesterday's snapshot into new daily snapshot
INSERT INTO user_devices_cumulated (user_id, browser_type, dates_active, snapshot_date)
SELECT
    COALESCE(t.user_id, y.user_id)                                          AS user_id,
    COALESCE(t.browser_type, y.browser_type)                                AS browser_type,
    -- accumulate new date into existing dates_active array
    CASE
        WHEN y.dates_active IS NULL THEN ARRAY[t.date_active]
        WHEN t.date_active IS NULL THEN y.dates_active
        ELSE ARRAY[t.date_active] || y.dates_active
    END AS dates_active,
    -- Use today’s date or infer it from yesterday's snapshot
    COALESCE(t.date_active, y.snapshot_date + INTERVAL '1 day')::DATE      AS snapshot_date
FROM today t
FULL OUTER JOIN yesterday y ON t.user_id = y.user_id AND t.browser_type = y.browser_type;

-- Convert dates_active into bitmask (datelist_int-style encoding) for efficient activity checks
WITH user_data AS (
    SELECT user_id, browser_type, dates_active, snapshot_date
    FROM user_devices_cumulated
    WHERE snapshot_date = DATE '2023-01-31'
),
-- Generate date range for all days in January
series AS (
    SELECT generate_series(DATE '2023-01-01', DATE '2023-01-31', '1 day'::INTERVAL)::DATE AS series_date
),
-- For each date in the range, compute its bit position and whether user was active
bit_values AS (
    SELECT
        u.user_id,
        u.browser_type,
        -- if user was active on that date, encode it as a 1 at the correct bit position
        CASE 
            WHEN u.dates_active @> ARRAY[s.series_date] 
                THEN POW(2, s.series_date - DATE '2023-01-01')::BIGINT  -- Corrected bit position
            ELSE 0
        END AS bit_value
    FROM user_data u
    CROSS JOIN series s
)

--Aggregate all bit values to get a full month’s activity pattern in a 32-bit integer
SELECT
    user_id,
    browser_type,
    -- Actual count of active days (more useful than bit_length)
    BIT_COUNT(SUM(bit_value)::BIGINT::bit(32)) AS total_active_days,
    
    -- Monthly active: active on any day
    BIT_COUNT(SUM(bit_value)::BIGINT::bit(32)) > 0 AS is_monthly_active,
    
    -- Weekly active: active in last 7 days (Jan 25-31)
    BIT_COUNT(
        ('11111110000000000000000000000000')::bit(32) 
        & SUM(bit_value)::BIGINT::bit(32)
    ) > 0 AS is_weekly_active,
    
    -- Active on snapshot date (Jan 31)
    BIT_COUNT(
        ('10000000000000000000000000000000')::bit(32) 
        & SUM(bit_value)::BIGINT::bit(32)
    ) > 0 AS is_active_on_snapshot_date,
    
    -- Active in last 3 days (Jan 29-31)
    BIT_COUNT(
        ('11100000000000000000000000000000')::bit(32) 
        & SUM(bit_value)::BIGINT::bit(32)
    ) > 0 AS is_active_last_3_days,
    
    -- Active in first week of month (Jan 1-7)
    BIT_COUNT(
        ('00000000000000000000000001111111')::bit(32) 
        & SUM(bit_value)::BIGINT::bit(32)
    ) > 0 AS is_active_first_week,
    
    -- Show the actual bit pattern for debugging
    SUM(bit_value)::BIGINT::bit(32) AS activity_bits
FROM bit_values
GROUP BY user_id, browser_type;       