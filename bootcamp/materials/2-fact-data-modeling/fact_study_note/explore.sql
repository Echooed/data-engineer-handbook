-- DROP TABLE IF EXISTS users_cumulated;
-- CREATE TABLE users_cumulated (
--     user_id TEXT,
--     -- The list of date that the user was active
--     dates_active DATE[],
--     -- the currrent date for the user
--     date DATE,
--     PRIMARY KEY (user_id, dates_active)
-- );




INSERT INTO users_cumulated (user_id, dates_active, date) WITH yesterday AS (
        SELECT *
        FROM users_cumulated
        WHERE date = DATE('2023-01-30')
    ),
    today AS (
        SELECT user_id::TEXT,
            DATE(event_time) AS date_active
        FROM events
        WHERE DATE(event_time) = DATE '2023-01-31'
            AND user_id IS NOT NULL
        GROUP BY user_id,
            DATE(event_time)
    )
SELECT COALESCE(t.user_id, y.user_id) AS user_id,
    CASE
        WHEN y.dates_active IS NULL THEN ARRAY [t.date_active]
        WHEN t.date_active IS NULL THEN y.dates_active
        ELSE ARRAY [t.date_active] || y.dates_active
    END AS dates_active,
    COALESCE(t.date_active, y.date + INTERVAL '1 day') AS date
FROM today t
    LEFT JOIN yesterday y ON t.user_id = y.user_id;





WITH users AS (
    SELECT *
    FROM users_cumulated
    WHERE date = DATE '2023-01-31'
),
series AS (
    SELECT * FROM generate_series(
    '2023-01-01'::DATE, 
    '2023-01-31'::DATE, 
    '1 day'::INTERVAL
) AS series_date
)
SELECT dates_active @> ARRAY[series_date::DATE], *
FROM users
CROSS JOIN series


WITH users AS (
    SELECT user_id,
        dates_active,
        date
    FROM users_cumulated
    WHERE date = DATE '2023-01-31'
),
series AS (
    SELECT generate_series(
        '2023-01-01'::DATE, 
        '2023-01-31'::DATE, 
        '1 day'::INTERVAL
    ) AS series_date
)
SELECT 
    series.series_date,
    users.*,
    users.dates_active @> ARRAY[series_date::DATE] AS is_active
FROM users
CROSS JOIN series
WHERE user_id = '17358702759623100000';
