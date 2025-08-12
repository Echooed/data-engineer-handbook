WITH event_device_deduped AS (
    SELECT 
        COALESCE(d.browser_type, 'N/A') AS browser_type,
        COALESCE(d.os_type, 'N/A') AS os_type,
        e.*,
        CASE 
            WHEN referrer LIKE '%zachwilson%' THEN 'on-site'
            WHEN referrer LIKE '%eczachly%' THEN 'on-site'
            WHEN referrer LIKE '%t.co%' THEN 'x.com'
            WHEN referrer LIKE '%google%' THEN 'google'
            WHEN referrer LIKE '%linkedin%' THEN 'linkedin'
            WHEN referrer LIKE '%youtube%' THEN 'youtube'
            WHEN referrer LIKE '%bing%' THEN 'bing'
            WHEN referrer LIKE '%facebook%' THEN 'facebook'
            WHEN referrer IS NULL THEN 'direct'
            ELSE 'others'
        END AS referrer_mapped,
        ROW_NUMBER() OVER (PARTITION BY e.user_id, e.device_id, e.event_time ORDER BY e.event_time) AS rn
    FROM events e
    JOIN devices d ON e.device_id = d.device_id
    WHERE e.user_id IS NOT NULL 
),
deduped_events AS (
    SELECT * 
    FROM event_device_deduped 
    WHERE rn = 1
),
ranked_events AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY user_id, DATE(event_time) ORDER BY event_time) as event_rank
    FROM deduped_events
)
SELECT 
    d1.user_id,
    d1.url AS from_url,
    d2.url AS to_url,
    d1.event_time AS start_time,
    d2.event_time AS end_time,
    CAST(
        EXTRACT(EPOCH FROM (CAST(d2.event_time AS TIMESTAMP) - CAST(d1.event_time AS TIMESTAMP))) AS REAL
    ) AS duration_seconds,
    d2.referrer_mapped,
    d2.browser_type,
    d2.os_type
FROM ranked_events d1
JOIN ranked_events d2 
    ON d1.user_id = d2.user_id
    AND DATE(d1.event_time) = DATE(d2.event_time)
    AND d2.event_rank = d1.event_rank + 1  -- Ensures sequential events only
WHERE d1.user_id = '688189120956475300'
  AND EXTRACT(EPOCH FROM (CAST(d2.event_time AS TIMESTAMP) - CAST(d1.event_time AS TIMESTAMP))) BETWEEN 1 AND 3600
ORDER BY d1.event_time;





-- using rolling windows funtion for the same task


WITH event_device_deduped AS (
    SELECT 
        COALESCE(d.browser_type, 'N/A') AS browser_type,
        COALESCE(d.os_type, 'N/A') AS os_type,
        e.*,
        CASE 
            WHEN referrer LIKE '%zachwilson%' THEN 'on-site'
            WHEN referrer LIKE '%eczachly%' THEN 'on-site'
            WHEN referrer LIKE '%t.co%' THEN 'x.com'
            WHEN referrer LIKE '%google%' THEN 'google'
            WHEN referrer LIKE '%linkedin%' THEN 'linkedin'
            WHEN referrer LIKE '%youtube%' THEN 'youtube'
            WHEN referrer LIKE '%bing%' THEN 'bing'
            WHEN referrer LIKE '%facebook%' THEN 'facebook'
            WHEN referrer IS NULL THEN 'direct'
            ELSE 'others'
        END AS referrer_mapped,
        ROW_NUMBER() OVER (PARTITION BY e.user_id, e.device_id, e.event_time ORDER BY e.event_time) AS rn
    FROM events e
    JOIN devices d ON e.device_id = d.device_id
    WHERE e.user_id IS NOT NULL 
),
deduped_events AS (
    SELECT * 
    FROM event_device_deduped 
    WHERE rn = 1
),
sequential_events AS (
    SELECT 
        user_id,
        url,
        event_time,
        referrer_mapped,
        browser_type,
        os_type,
        LAG(url) OVER (PARTITION BY user_id, DATE(event_time) ORDER BY event_time) AS prev_url,
        LAG(event_time) OVER (PARTITION BY user_id, DATE(event_time) ORDER BY event_time) AS prev_event_time,
        LEAD(url) OVER (PARTITION BY user_id, DATE(event_time) ORDER BY event_time) AS next_url,
        LEAD(event_time) OVER (PARTITION BY user_id, DATE(event_time) ORDER BY event_time) AS next_event_time
    FROM deduped_events
)
SELECT 
    user_id,
    prev_url AS from_url,
    url AS to_url,
    prev_event_time AS start_time,
    event_time AS end_time,
    CAST(
        EXTRACT(EPOCH FROM (CAST(event_time AS TIMESTAMP) - CAST(prev_event_time AS TIMESTAMP))) AS REAL
    ) AS duration_seconds,
    referrer_mapped,
    browser_type,
    os_type
FROM sequential_events
WHERE prev_url IS NOT NULL
  AND user_id = '688189120956475300'
  -- Optional: Filter out unrealistic durations
  AND EXTRACT(EPOCH FROM (CAST(event_time AS TIMESTAMP) - CAST(prev_event_time AS TIMESTAMP))) BETWEEN 1 AND 3600  -- 1 second to 1 hour
ORDER BY event_time;