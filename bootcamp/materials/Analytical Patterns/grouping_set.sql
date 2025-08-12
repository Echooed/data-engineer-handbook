
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
        ROW_NUMBER() OVER (PARTITION BY e.user_id ORDER BY event_time) AS rn
    FROM events e
    JOIN devices d ON e.device_id = d.device_id
    WHERE e.user_id IS NOT NULL 
)
SELECT 
        COALESCE(referrer_mapped, '(overall)') AS referrer,
        COALESCE(browser_type, '(overall)') AS browser_type,
        COALESCE(os_type, '(overall)') AS os_type,
        COUNT(1) AS no_of_site_hits,
        COUNT(CASE WHEN url LIKE '/signup'THEN 1 END) AS no_of_signups,
        COUNT(CASE WHEN url LIKE '/contact' THEN 1 END) AS no_of_contact_visits,
        COUNT(CASE WHEN url LIKE '/login' THEN 1 END) AS no_of_login_visits
    FROM event_device_deduped 
    WHERE rn = 1
    GROUP BY GROUPING SETS (
        (referrer_mapped, browser_type, os_type),
        (os_type),
        (browser_type),
        (referrer_mapped),
        ()
    )
ORDER BY COUNT(1) DESC;








