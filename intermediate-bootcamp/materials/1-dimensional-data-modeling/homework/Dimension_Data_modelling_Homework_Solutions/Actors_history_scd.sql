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


-- Insert into SCD Type 2 history table for actors
INSERT INTO actors_scd_history (
    actorid,
    actor,
    quality_class,
    is_active,
    current_year,
    start_year,
    end_year
)

-- Filter only records up to the selected snapshot year (e.g. 1974)
WITH filtered_actors AS (
    SELECT 
        actorid,
        actor,
        quality_class,
        is_active,
        current_year
    FROM actors
    WHERE current_year <= 2020 
),

-- Add previous row values using LAG to detect changes in dimension attributes
with_previous AS (
    SELECT 
        actorid,
        actor,
        quality_class,
        is_active,
        current_year,
        LAG(quality_class) OVER (PARTITION BY actorid ORDER BY current_year) AS previous_quality_class,
        LAG(is_active) OVER (PARTITION BY actorid ORDER BY current_year) AS previous_is_active
    FROM filtered_actors
),

-- Mark rows where a change in dimension attributes occurs
with_indicator AS (
    SELECT *,
        CASE 
            WHEN previous_quality_class IS DISTINCT FROM quality_class 
              OR previous_is_active IS DISTINCT FROM is_active 
            THEN 1 ELSE 0
        END AS change_indicator
    FROM with_previous
),

-- Use running sum to assign streak IDs for unchanged periods
with_streaks AS (
    SELECT *,
        SUM(change_indicator) OVER (
            PARTITION BY actorid 
            ORDER BY current_year
        ) AS streak_identifier
    FROM with_indicator
),

-- Get the maximum year in the snapshot to identify open-ended records
max_year AS (
    SELECT MAX(current_year) AS max_current_year 
    FROM filtered_actors
)

-- Aggregate streaks into start/end periods per actor and dimension values
SELECT 
    actorid,
    actor,
    quality_class,
    is_active,
    MAX(current_year) AS current_year,       -- latest year for this streak (for tracking)
    MIN(current_year) AS start_year,         -- when this dimensional state started
    CASE 
        WHEN MAX(current_year) = max_current_year THEN NULL  -- open-ended streak
        ELSE MAX(current_year)                          -- closed streak
    END AS end_year
FROM with_streaks
CROSS JOIN max_year
GROUP BY actorid, actor, quality_class, is_active, streak_identifier, max_current_year
ORDER BY actor, start_year;


