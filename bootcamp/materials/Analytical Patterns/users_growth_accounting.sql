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
INSERT INTO users_growth_accounting (
    user_id, 
    first_active_date, 
    last_active_date, 
    daily_active_state, 
    weekly_active_state, 
    dates_active_list, 
    date
)
WITH yesterday AS (
    SELECT * 
    FROM users_growth_accounting
    WHERE date = DATE '2022-12-31'
),
today AS (
    SELECT 
        CAST(user_id AS TEXT) AS user_id,
        ARRAY_AGG (DISTINCT(DATE_TRUNC('day', event_time::TIMESTAMP)::DATE)) AS today_date
    FROM events
    WHERE DATE_TRUNC('day', event_time::TIMESTAMP) = DATE '2023-01-01'
      AND user_id IS NOT NULL
    GROUP BY user_id, DATE_TRUNC('day', event_time::TIMESTAMP)
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
        WHEN t.user_id IS NULL THEN 'churned'
        ELSE 'stale'
    END AS daily_active_state,

    -- Weekly active classification (fixed)
    CASE
        WHEN y.user_id IS NULL AND t.user_id IS NOT NULL THEN 'new'
        WHEN y.last_active_date >= y.date - INTERVAL '7 days' THEN 'retained'
        WHEN y.last_active_date < t.today_date - INTERVAL '7 days' AND t.user_id IS NOT NULL THEN 'reactivated'
        WHEN t.today_date IS NULL AND y.last_active_date < t.today_date - INTERVAL '6 days' 
             AND y.last_active_date > t.today_date - INTERVAL '14 days' THEN 'churned'
        WHEN t.user_id IS NULL AND y.last_active_date < t.today_date - INTERVAL '13 days'  THEN 'stale'
        --ELSE 'stale'
    END AS weekly_active_state,

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


--SELECT * FROM users_growth_accounting;



COALESCE(y.dates_active_list, ARRAY[]::DATE[]) || 
COALESCE(t.today_dates, ARRAY[]::DATE[]) AS dates_active_list
