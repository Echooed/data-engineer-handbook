--
-- DROP TABLE IF EXISTS users_growth_accounting;
-- CREATE TABLE users_growth_accounting (
--     user_id TEXT,
--     first_active_date DATE,
--     last_active_date DATE,
--     daily_active_state TEXT,
--     weekly_active_state TEXT,
--     dates_active_list DATE[],
--     date DATE,
--     PRIMARY KEY (user_id, date)
-- );

-- DELETE FROM users_growth_accounting;

-- This SQL script updates the users_growth_accounting table with the latest user activity dat

WITH deduped AS (
    SELECT
        user_id,
        event_time,
        ROW_NUMBER() OVER (PARTITION BY user_id, event_time ORDER BY event_time) AS rn
    FROM events
    WHERE user_id IS NOT NULL 
),
yesterday AS (
    SELECT * 
    FROM users_growth_accounting
    WHERE date = DATE '2023-01-16'
),
today AS (
    SELECT
            CAST(user_id AS TEXT) as user_id,
            DATE_TRUNC('day', event_time::timestamp) as today_date
         FROM deduped
         WHERE DATE_TRUNC('day', event_time::timestamp) = DATE('2023-01-17')
         AND rn = 1
         GROUP BY user_id, DATE_TRUNC('day', event_time::timestamp)
)
INSERT INTO users_growth_accounting (
    user_id, 
    first_active_date, 
    last_active_date, 
    daily_active_state, 
    weekly_active_state, 
    dates_active_list, 
    date
)
SELECT
    COALESCE(y.user_id, t.user_id) AS user_id,

    -- First active date remains the earliest known
    COALESCE(y.first_active_date, t.today_date) AS first_active_date,

    -- Last active is updated only if active today
    CASE 
        WHEN t.today_date IS NOT NULL THEN t.today_date
        ELSE y.last_active_date
    END AS last_active_date,

    -- Daily active classification
    CASE 
        WHEN y.user_id IS NULL AND t.user_id IS NOT NULL THEN 'new'
        WHEN y.last_active_date = t.today_date - INTERVAL '1 day' THEN 'returning'
        WHEN y.last_active_date < t.today_date - INTERVAL '1 day' THEN 'reactivated'
        WHEN y.user_id IS NOT NULL AND t.user_id IS NULL THEN 'Churned'
        ELSE 'stale'
    END AS daily_active_state,

    -- Weekly active classification (fixed)
   CASE
    WHEN y.user_id IS NULL AND t.user_id IS NOT NULL THEN 'new'
    WHEN t.user_id IS NOT NULL AND y.last_active_date >= DATE y.date-iTERVAL '7 days' THEN 'retained'
    WHEN t.user_id IS NOT NULL AND y.last_active_date < DATE y.date INTERVAL '7 days' THEN 'reactivated'
    WHEN t.user_id IS NULL AND y.last_active_date >= DATE y.date INTERVAL '7 days' THEN 'churned'
    WHEN t.user_id IS NULL AND y.last_active_date < DATE y.date  INTERVAL '7 days' THEN 'stale'
    
    -- Explicit data quality checks
    WHEN y.last_active_date IS NULL THEN 'data_error_null_date'
    WHEN y.user_id IS NULL AND t.user_id IS NULL THEN 'data_error_both_null'
    
    ELSE 'truly_unknown'
END AS weekly_active_state
END AS weekly_active_state
    -- Append today's date if active, else keep old array
  COALESCE(y.dates_active_list, ARRAY[]::DATE[]) ||
CASE
    WHEN t.today_date IS NOT NULL THEN ARRAY[t.today_date::DATE]
    ELSE ARRAY[]::DATE[]  
END AS dates_active_list,
    -- Store the date snapshot for this record
    COALESCE(t.today_date, y.date + INTERVAL '1 day')::DATE AS date
FROM today t
FULL OUTER JOIN yesterday y ON y.user_id = t.user_id;



- TERVAL '7 days' THEN