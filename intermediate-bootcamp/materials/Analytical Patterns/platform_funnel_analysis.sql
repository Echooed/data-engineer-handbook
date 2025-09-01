WITH cleaned_events AS (
    -- Data quality and performance improvements
    SELECT 
    e.user_id,
    e.device_id,
    e.event_time,
    -- Clean and normalize URLs
    CASE 
        WHEN lower(trim(e.url)) LIKE '/signup%'  THEN '/signup'
        WHEN lower(trim(e.url)) LIKE '/contact%' THEN '/contact'
        WHEN lower(trim(e.url)) LIKE '/login%'   THEN '/login'
        ELSE lower(trim(e.url))
    END AS normalized_url,
    -- Extract domain from referrer for better categorization (Postgres)
    CASE
        WHEN coalesce(trim(e.referrer), '') = '' THEN NULL
        WHEN lower(trim(e.referrer)) LIKE 'http%' THEN
            substring(e.referrer FROM '^https?://(?:www\. )?([^/]+)')  -- capture domain
        ELSE e.referrer
    END AS referrer_domain
FROM events e
WHERE e.event_time::timestamp >= TIMESTAMP '2023-01-01 00:00:00'
  AND e.event_time::timestamp <  TIMESTAMP '2023-02-01 00:00:00'
  AND e.user_id IS NOT NULL
  AND e.url IS NOT NULL
),

referrer_mapped AS (
    SELECT *,
        CASE 
            WHEN referrer_domain IN ('zachwilson.com', 'eczachly.com') THEN 'on-site'
            WHEN referrer_domain IN ('t.co', 'twitter.com', 'x.com') THEN 'x.com'
            WHEN referrer_domain LIKE '%google.%' OR referrer_domain = 'google.com' THEN 'google'
            WHEN referrer_domain LIKE '%linkedin.%' OR referrer_domain = 'linkedin.com' THEN 'linkedin'
            WHEN referrer_domain LIKE '%youtube.%' OR referrer_domain = 'youtube.com' THEN 'youtube'
            WHEN referrer_domain LIKE '%bing.%' OR referrer_domain = 'bing.com' THEN 'bing'
            WHEN referrer_domain LIKE '%facebook.%' OR referrer_domain = 'facebook.com' THEN 'facebook'
            WHEN referrer_domain LIKE '%instagram.%' OR referrer_domain = 'instagram.com' THEN 'instagram'
            WHEN referrer_domain LIKE '%reddit.%' OR referrer_domain = 'reddit.com' THEN 'reddit'
            WHEN referrer_domain LIKE '%tiktok.%' OR referrer_domain = 'tiktok.com' THEN 'tiktok'
            WHEN referrer_domain IS NULL THEN 'direct'
            ELSE 'others'
        END AS referrer_category
    FROM cleaned_events
),
user_sessions AS (
    -- Better approach: Analyze complete user journeys instead of just first events
    SELECT 
        rm.user_id,
        d.browser_type,
        d.os_type,
        -- First touch attribution
        FIRST_VALUE(rm.referrer_category) OVER (
            PARTITION BY rm.user_id 
            ORDER BY rm.event_time 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS first_touch_referrer,
        -- Last touch attribution  
        LAST_VALUE(rm.referrer_category) OVER (
            PARTITION BY rm.user_id 
            ORDER BY rm.event_time 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS last_touch_referrer,
        rm.normalized_url,
        rm.event_time,
        -- Session indicators
        ROW_NUMBER() OVER (PARTITION BY rm.user_id ORDER BY rm.event_time) as event_sequence
    FROM referrer_mapped rm
    JOIN devices d ON rm.device_id = d.device_id
),

user_summary AS (
    -- Aggregate user-level metrics
    SELECT 
        user_id,
        browser_type,
        os_type,
        first_touch_referrer,
        last_touch_referrer,
        COUNT(*) as total_page_views,
        COUNT(DISTINCT normalized_url) as unique_pages_visited,
        MAX(CASE WHEN normalized_url = '/signup' THEN 1 ELSE 0 END) as converted_signup,
        MAX(CASE WHEN normalized_url = '/contact' THEN 1 ELSE 0 END) as visited_contact,
        MAX(CASE WHEN normalized_url = '/login' THEN 1 ELSE 0 END) as visited_login,
        MIN(event_time) as first_visit,
        MAX(event_time) as last_visit
    FROM user_sessions
    GROUP BY user_id, browser_type, os_type, first_touch_referrer, last_touch_referrer
)

-- Final aggregation with multiple attribution models
SELECT 
    'first_touch' as attribution_model,
    COALESCE(first_touch_referrer, '(overall)') AS referrer,
    COALESCE(browser_type, '(overall)') AS browser_type,
    COALESCE(os_type, '(overall)') AS os_type,
    
    -- User-based metrics (more accurate)
    COUNT(DISTINCT user_id) AS unique_users,
    SUM(total_page_views) AS total_page_views,
    ROUND(AVG(total_page_views), 2) AS avg_pages_per_user,
    
    -- Conversion metrics  
    SUM(converted_signup) AS users_who_signed_up,
    ROUND(100.0 * SUM(converted_signup) / COUNT(DISTINCT user_id), 2) AS signup_conversion_rate,
    SUM(visited_contact) AS users_who_visited_contact,
    SUM(visited_login) AS users_who_visited_login,
    
    -- Engagement metrics
    ROUND(AVG(unique_pages_visited), 2) AS avg_unique_pages_per_user
FROM user_summary
GROUP BY GROUPING SETS (
    (first_touch_referrer, browser_type, os_type),
    (first_touch_referrer, browser_type),
    (first_touch_referrer, os_type), 
    (browser_type, os_type),
    (first_touch_referrer),
    (browser_type),
    (os_type),
    ()
)

UNION ALL

-- Last touch attribution analysis
SELECT 
    'last_touch' as attribution_model,
    COALESCE(last_touch_referrer, '(overall)') AS referrer,
    COALESCE(browser_type, '(overall)') AS browser_type,
    COALESCE(os_type, '(overall)') AS os_type,
    
    COUNT(DISTINCT user_id) AS unique_users,
    SUM(total_page_views) AS total_page_views,
    ROUND(AVG(total_page_views), 2) AS avg_pages_per_user,
    
    SUM(converted_signup) AS users_who_signed_up,
    ROUND(100.0 * SUM(converted_signup) / COUNT(DISTINCT user_id), 2) AS signup_conversion_rate,
    SUM(visited_contact) AS users_who_visited_contact,
    SUM(visited_login) AS users_who_visited_login,
    
    ROUND(AVG(unique_pages_visited), 2) AS avg_unique_pages_per_user
FROM user_summary  
GROUP BY GROUPING SETS (
    (last_touch_referrer, browser_type, os_type),
    (last_touch_referrer, browser_type),
    (last_touch_referrer, os_type),
    (browser_type, os_type), 
    (last_touch_referrer),
    (browser_type),
    (os_type),
    ()
)

ORDER BY attribution_model, unique_users DESC;


















-- Additional query for conversion funnel analysis
-- This can be run separately to understand user journey patterns
WITH cleaned_events AS (
    SELECT 
        e.user_id,
        e.device_id,
        e.event_time,
        -- Clean and normalize URLs
        CASE 
            WHEN lower(trim(e.url)) LIKE '/signup%'  THEN '/signup'
            WHEN lower(trim(e.url)) LIKE '/contact%' THEN '/contact'
            WHEN lower(trim(e.url)) LIKE '/login%'   THEN '/login'
            ELSE lower(trim(e.url))
        END AS normalized_url,
        -- Extract domain from referrer for better categorization (Postgres)
        CASE
            WHEN coalesce(trim(e.referrer), '') = '' THEN NULL
            WHEN lower(trim(e.referrer)) LIKE 'http%' THEN
                substring(e.referrer FROM '^https?://(?:www\. )?([^/]+)')  
            ELSE e.referrer
        END AS referrer_domain
FROM events e
WHERE e.event_time::timestamp >= TIMESTAMP '2023-01-01 00:00:00'
  AND e.event_time::timestamp <  TIMESTAMP '2023-02-01 00:00:00'
  AND e.user_id IS NOT NULL
  AND e.url IS NOT NULL
),

referrer_mapped AS (
    SELECT *,
        CASE 
            WHEN referrer_domain IN ('zachwilson.com', 'eczachly.com') THEN 'on-site'
            WHEN referrer_domain IN ('t.co', 'twitter.com', 'x.com') THEN 'x.com'
            WHEN referrer_domain LIKE '%google.%' OR referrer_domain = 'google.com' THEN 'google'
            WHEN referrer_domain LIKE '%linkedin.%' OR referrer_domain = 'linkedin.com' THEN 'linkedin'
            WHEN referrer_domain LIKE '%youtube.%' OR referrer_domain = 'youtube.com' THEN 'youtube'
            WHEN referrer_domain LIKE '%bing.%' OR referrer_domain = 'bing.com' THEN 'bing'
            WHEN referrer_domain LIKE '%facebook.%' OR referrer_domain = 'facebook.com' THEN 'facebook'
            WHEN referrer_domain LIKE '%instagram.%' OR referrer_domain = 'instagram.com' THEN 'instagram'
            WHEN referrer_domain LIKE '%reddit.%' OR referrer_domain = 'reddit.com' THEN 'reddit'
            WHEN referrer_domain LIKE '%tiktok.%' OR referrer_domain = 'tiktok.com' THEN 'tiktok'
            WHEN referrer_domain IS NULL THEN 'direct'
            ELSE 'others'
        END AS referrer_category
    FROM cleaned_events
),
user_sessions AS (
    -- Better approach: Analyze complete user journeys instead of just first events
    SELECT 
        rm.user_id,
        d.browser_type,
        d.os_type,
        -- First touch attribution
        FIRST_VALUE(rm.referrer_category) OVER (
            PARTITION BY rm.user_id 
            ORDER BY rm.event_time 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS first_touch_referrer,
        -- Last touch attribution  
        LAST_VALUE(rm.referrer_category) OVER (
            PARTITION BY rm.user_id 
            ORDER BY rm.event_time 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS last_touch_referrer,
        rm.normalized_url,
        rm.event_time,
        -- Session indicators
        ROW_NUMBER() OVER (PARTITION BY rm.user_id ORDER BY rm.event_time) as event_sequence
    FROM referrer_mapped rm
    JOIN devices d ON rm.device_id = d.device_id
),

user_summary AS (
    -- Aggregate user-level metrics
    SELECT 
        user_id,
        browser_type,
        os_type,
        first_touch_referrer,
        last_touch_referrer,
        COUNT(*) as total_page_views,
        COUNT(DISTINCT normalized_url) as unique_pages_visited,
        MAX(CASE WHEN normalized_url = '/signup' THEN 1 ELSE 0 END) as converted_signup,
        MAX(CASE WHEN normalized_url = '/contact' THEN 1 ELSE 0 END) as visited_contact,
        MAX(CASE WHEN normalized_url = '/login' THEN 1 ELSE 0 END) as visited_login,
        MIN(event_time) as first_visit,
        MAX(event_time) as last_visit
    FROM user_sessions
    GROUP BY user_id, browser_type, os_type, first_touch_referrer, last_touch_referrer
),
conversion_funnel AS (
    SELECT 
        first_touch_referrer,
        COUNT(DISTINCT user_id) as total_users,
        COUNT(DISTINCT CASE WHEN visited_contact = 1 THEN user_id END) as contacted,  
        COUNT(DISTINCT CASE WHEN visited_login = 1 THEN user_id END) as attempted_login,
        COUNT(DISTINCT CASE WHEN converted_signup = 1 THEN user_id END) as signed_up
    FROM user_summary
    GROUP BY first_touch_referrer
)
SELECT  
    first_touch_referrer,
    total_users,
    contacted,
    ROUND(100.0 * contacted / total_users, 2) as contact_rate,
    attempted_login, 
    ROUND(100.0 * attempted_login / total_users, 2) as login_attempt_rate,
    signed_up,
    ROUND(100.0 * signed_up / total_users, 2) as signup_rate
FROM conversion_funnel
ORDER BY total_users DESC;
