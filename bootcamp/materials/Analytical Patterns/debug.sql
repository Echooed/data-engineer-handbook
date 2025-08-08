-- Run just the SELECT part without INSERT to see what you get
WITH yesterday AS (
    SELECT * 
    FROM users_growth_accounting
    WHERE date = DATE '2022-12-31'
),
today AS (
    SELECT 
        CAST(user_id AS TEXT) AS user_id,
        DATE_TRUNC('day', event_time::TIMESTAMP)::DATE AS today_date
    FROM events
    WHERE DATE_TRUNC('day', event_time::TIMESTAMP)::DATE = DATE '2023-01-01'
      AND user_id IS NOT NULL
    GROUP BY user_id, DATE_TRUNC('day', event_time::TIMESTAMP)::DATE
)
SELECT
    COALESCE(y.user_id, t.user_id) AS user_id,
    COALESCE(y.dates_active_list, ARRAY[]::DATE[]) || 
    CASE 
        WHEN t.user_id IS NOT NULL 
            THEN ARRAY[t.today_date::DATE]
        ELSE ARRAY[]::DATE[]
    END AS dates_active_list
FROM today t
FULL OUTER JOIN yesterday y ON y.user_id = t.user_id
WHERE COALESCE(y.user_id, t.user_id) = '14434469444350800000';




WITH yesterday AS (
    SELECT * FROM users_growth_accounting
    WHERE date = DATE('2022-12-31')
),
     today AS (
         SELECT
            CAST(user_id AS TEXT) as user_id,
            DATE_TRUNC('day', event_time::timestamp)::DATE as today_date,
            COUNT(1)
         FROM events
         WHERE DATE_TRUNC('day', event_time::timestamp)::DATE = DATE('2023-01-01')
         AND user_id IS NOT NULL
         GROUP BY user_id, DATE_TRUNC('day', event_time::timestamp)
     )

         SELECT COALESCE(t.user_id, y.user_id)                    as user_id,
                COALESCE(y.first_active_date, t.today_date)       AS first_active_date,
                COALESCE(t.today_date, y.last_active_date)        AS last_active_date,
                CASE
                    WHEN y.user_id IS NULL THEN 'New'
                    WHEN y.last_active_date = t.today_date - Interval '1 day' THEN 'Retained'
                    WHEN y.last_active_date < t.today_date - Interval '1 day' THEN 'Resurrected'
                    WHEN t.today_date IS NULL AND y.last_active_date = y.date THEN 'Churned'
                    ELSE 'Stale'
                    END                                           as daily_active_state,
                CASE
                    WHEN y.user_id IS NULL THEN 'New'
                    WHEN y.last_active_date < t.today_date - Interval '7 day' THEN 'Resurrected'
                    WHEN
                            t.today_date IS NULL
                            AND y.last_active_date = y.date - interval '7 day' THEN 'Churned'
                    WHEN COALESCE(t.today_date, y.last_active_date) + INTERVAL '7 day' >= y.date THEN 'Retained'
                    ELSE 'Stale'
                    END                                           as weekly_active_state,
                    ARRAY_CAT(
    ARRAY(SELECT d::DATE FROM UNNEST(COALESCE(y.dates_active_list, ARRAY[]::DATE[])) AS d),
    CASE 
        WHEN t.user_id IS NOT NULL THEN ARRAY[t.today_date::DATE]
        ELSE ARRAY[]::DATE[]
    END
) AS dates_active_list,
                COALESCE(t.today_date, y.date + Interval '1 day') as date
         FROM today t
                  FULL OUTER JOIN yesterday y
                                  ON t.user_id = y.user_id



(COALESCE(y.dates_active_list, ARRAY[]::DATE[]) || 
    CASE 
        WHEN t.user_id IS NOT NULL 
            THEN ARRAY[t.today_date]
        ELSE ARRAY[]::DATE[]
    END)::DATE[] AS dates_active_list,;



COALESCE(y.dates_active_list, ARRAY[]::DATE[])
||
CASE
    WHEN t.user_id IS NOT NULL THEN ARRAY[t.today_date::DATE]
    ELSE ARRAY[]::DATE[]
END AS dates_active_list



DATE_TRUNC('day', event_time::TIMESTAMP)::DATE



SELECT column_name, data_type, udt_name 
FROM information_schema.columns 
WHERE table_name = 'users_growth_accounting' 
  AND column_name = 'dates_active_list';



  SELECT dates_active_list[1]::TEXT FROM users_growth_accounting WHERE user_id = '14434469444350800000';



  SELECT 
    event_time,
    event_time::TIMESTAMP,
    DATE_TRUNC('day', event_time::TIMESTAMP)::DATE
FROM events 
WHERE CAST(user_id AS TEXT) = '14434469444350800000'
ORDER BY 1;


WITH yesterday AS (
    SELECT * 
    FROM users_growth_accounting
    WHERE date = DATE '2022-12-31'
),
today AS (
    SELECT 
        CAST(user_id AS TEXT) AS user_id,
        ARRAY_AGG(DISTINCT DATE_TRUNC('day', event_time)::DATE) AS today_date
    FROM events
    WHERE DATE_TRUNC('day', event_time) = DATE '2023-01-01'
      AND user_id IS NOT NULL
    GROUP BY user_id
)

SELECT
    COALESCE(y.user_id, t.user_id) AS user_id,

    -- First active date remains the earliest known
    COALESCE(y.first_active_date, t.today_date[1]) AS first_active_date,

    -- Last active is updated only if active today
    CASE 
        WHEN t.today_date IS NOT NULL THEN t.today_date[1]
        ELSE y.last_active_date
    END AS last_active_date,

    -- Daily active classification
    CASE 
        WHEN y.user_id IS NULL AND t.user_id IS NOT NULL THEN 'new'
        WHEN y.last_active_date = t.today_date[1] - INTERVAL '1 day' THEN 'returning'
        WHEN y.last_active_date < t.today_date[1] - INTERVAL '1 day' THEN 'reactivated'
        WHEN t.user_id IS NULL THEN 'churned'
        ELSE 'stale'
    END AS daily_active_state,

    -- Weekly active classification (fixed)
    CASE
        WHEN y.user_id IS NULL AND t.user_id IS NOT NULL THEN 'new'
        WHEN y.last_active_date >= y.date - INTERVAL '7 days' THEN 'retained'
        WHEN y.last_active_date < t.today_date[1] - INTERVAL '7 days' AND t.user_id IS NOT NULL THEN 'reactivated'
        WHEN t.today_date IS NULL AND y.last_active_date < t.today_date[1] - INTERVAL '6 days' 
             AND y.last_active_date > t.today_date[1] - INTERVAL '14 days' THEN 'churned'
        WHEN t.user_id IS NULL AND y.last_active_date < t.today_date[1] - INTERVAL '13 days' THEN 'stale'
    END AS weekly_active_state,

    -- Append today's date(s) if active, else keep old array
    COALESCE(y.dates_active_list, ARRAY[]::DATE[]) ||
    COALESCE(t.today_date, ARRAY[]::DATE[]) AS dates_active_list,

    -- Store the date snapshot for this record
    COALESCE(t.today_date[1], y.date + INTERVAL '1 day')::DATE AS date

FROM today t
FULL OUTER JOIN yesterday y ON y.user_id = t.user_id;
