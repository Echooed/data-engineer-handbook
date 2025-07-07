DROP TABLE IF EXISTS actors_scd_history;

CREATE TABLE actors_scd_history (
    actorid TEXT,
    actor TEXT,
    quality_class quality_class,
    is_active BOOLEAN,
    current_year INTEGER,
    start_year INTEGER,
    end_year INTEGER,
    PRIMARY KEY (actorid, start_year)
);



-- Get current and previous slowly changing value for each actor.
WITH with_previous AS (
    SELECT 
        actorid,
        actor,
        quality_class,
        is_active,
        current_year,
        LAG(quality_class) OVER (PARTITION BY actorid ORDER BY current_year) AS previous_quality_class,
        LAG(is_active) OVER (PARTITION BY actorid ORDER BY current_year) AS previous_is_active
    FROM actors
    WHERE current_year <= 1974
),

with_indicator AS (
    SELECT *,
        CASE 
            WHEN previous_quality_class IS DISTINCT FROM quality_class THEN 1
            WHEN previous_is_active IS DISTINCT FROM is_active THEN 1
            ELSE 0
        END AS change_indicator
    FROM with_previous
),

with_streaks AS (
    SELECT *,
        SUM(change_indicator) 
        OVER (PARTITION BY actorid ORDER BY current_year) AS streak_identifier
    FROM with_indicator
)

SELECT 
    actorid,
    actor,
    quality_class,
    is_active,
    MAX(current_year) AS current_year,
    MIN(current_year) AS start_year,

    CASE 
        WHEN MAX(current_year) = (SELECT MAX(current_year) FROM actors)
        THEN NULL
        ELSE MAX(current_year)
    END AS end_year
FROM with_streaks
GROUP BY actorid, actor, quality_class, is_active, streak_identifier
ORDER BY actor, start_year;
