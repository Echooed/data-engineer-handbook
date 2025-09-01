WITH events_deduped AS (
    SELECT 
        COALESCE(d.os_type, 'unknown')      AS os_type,
        COALESCE(d.device_type, 'unknown')  AS device_type,
        COALESCE(d.browser_type, 'unknown') AS browser_type,
        url,
        user_id
    FROM events e 
    JOIN devices d 
      ON e.device_id = d.device_id
    WHERE user_id IS NOT NULL
)
SELECT
    CASE
        WHEN GROUPING(os_type) = 0 
         AND GROUPING(device_type) = 0 
         AND GROUPING(browser_type) = 0
            THEN 'os_type__device_type__browser'
        WHEN GROUPING(os_type) = 1 
         AND GROUPING(device_type) = 1 
         AND GROUPING(browser_type) = 0
            THEN 'browser_type'
        WHEN GROUPING(os_type) = 0 
         AND GROUPING(device_type) = 1 
         AND GROUPING(browser_type) = 1
            THEN 'os_type'
        WHEN GROUPING(os_type) = 1 
         AND GROUPING(device_type) = 0 
         AND GROUPING(browser_type) = 1
            THEN 'device_type'
    END AS aggregation_level,
    COALESCE(os_type, '(overall)')       AS os_type,
    COALESCE(device_type, '(overall)')   AS device_type,
    COALESCE(browser_type, '(overall)')  AS browser_type,
    COUNT(*) AS number_of_hits
FROM events_deduped
GROUP BY GROUPING SETS (
    (os_type, device_type, browser_type),
    (browser_type),
    (os_type),
    (device_type)
)
ORDER BY number_of_hits DESC;
