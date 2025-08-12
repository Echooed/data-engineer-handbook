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
        ROW_NUMBER() OVER (PARTITION BY e.user_id, event_time ORDER BY event_time) AS rn
    FROM events e
    JOIN devices d ON e.device_id = d.device_id
    WHERE e.user_id IS NOT NULL 
),
deduped_ AS (SELECT * FROM event_device_deduped WHERE rn = 1)

SELECT d2.url, d1.url, d2.event_time, d1.event_time,
        CAST(
        EXTRACT(EPOCH FROM (CAST(d1.event_time AS TIMESTAMP) - CAST(d2.event_time AS TIMESTAMP))) AS REAL
    ) AS duration_seconds
FROM deduped_ d1
    JOIN deduped_ d2 
    ON d1.user_id = d2.user_id
    AND DATE(d1.event_time) = DATE(d2.event_time)
    AND d1.event_time > d2.event_time
 WHERE d1.user_id = '688189120956475300'

