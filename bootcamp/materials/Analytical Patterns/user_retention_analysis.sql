SELECT date,
    daily_active_state,
    COUNT(date)
FROM users_growth_accounting

GROUP BY 1,2;



SELECT 
    date - first_active_date AS days_since_first_active,
    COUNT(CASE WHEN daily_active_state IN ('returning', 'reactivated', 'new') THEN 1 END) AS number_active,
    COUNT(CASE WHEN daily_active_state IN ('returning', 'reactivated', 'new') THEN 1 END)::REAL
        / COUNT(date)::REAL AS pct_number_active,
    COUNT(1)
FROM users_growth_accounting
--WHERE first_active_date = '2023-01-01'
GROUP BY date - first_active_date
ORDER BY 1 ASC;




