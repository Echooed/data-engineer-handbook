-- Drop and recreate the custom row type
DROP TYPE IF EXISTS actors_scd_type CASCADE;
CREATE TYPE actors_scd_type AS (
    quality_class quality_class,
    is_active BOOLEAN,
    start_year INTEGER,
    end_year INTEGER
);

-- Load new data and detect changes
WITH actors_last_year_scd AS (
    SELECT * 
    FROM actors_scd_history
    WHERE current_year = 2019
    AND (end_year IS NULL OR end_year = 2019)
),

actors_historical_scd AS (
    SELECT 
        actorid,
        actor,
        quality_class,
        is_active,
        start_year,
        end_year,
        current_year
    FROM actors_scd_history
    WHERE current_year = 2019
    AND end_year < 2019
),

this_year_data AS (
    SELECT *
    FROM actors
    WHERE current_year = 2020
),

actors_unchanged_records AS (
    SELECT 
        ts.actorid,
        ts.actor,
        ts.quality_class,
        ts.is_active,
        ls.start_year,
        ts.current_year AS end_year,
        ts.current_year AS current_year
    FROM this_year_data ts
    JOIN actors_last_year_scd ls
        ON ts.actorid = ls.actorid
    WHERE ts.quality_class = ls.quality_class
      AND ts.is_active = ls.is_active
),

actors_changed_records AS (
    SELECT 
        ts.actorid,
        ts.actor,
        UNNEST(ARRAY[
            ROW(ls.quality_class, ls.is_active, ls.start_year, 2019)::actors_scd_type,
            ROW(ts.quality_class, ts.is_active, ts.current_year, ts.current_year)::actors_scd_type
        ]) AS records
    FROM this_year_data ts
    JOIN actors_last_year_scd ls
        ON ts.actorid = ls.actorid
    WHERE ts.quality_class IS DISTINCT FROM ls.quality_class
       OR ts.is_active IS DISTINCT FROM ls.is_active
),

unnested_actors_changed_records AS (
    SELECT 
        actorid,
        actor,
        (records).quality_class AS quality_class,
        (records).is_active AS is_active,
        (records).start_year AS start_year,
        (records).end_year AS end_year,
        (records).end_year AS current_year
    FROM actors_changed_records
),

actors_new_records AS (
    SELECT 
        ts.actorid,
        ts.actor,
        ts.quality_class,
        ts.is_active,
        ts.current_year AS start_year,
        ts.current_year AS end_year,
        ts.current_year AS current_year
    FROM this_year_data ts
    LEFT JOIN actors_last_year_scd ls 
        ON ts.actorid = ls.actorid
    WHERE ls.actorid IS NULL
)

-- Final union of all record types
SELECT *
FROM  (
    SELECT * FROM actors_historical_scd
    UNION ALL
    SELECT * FROM actors_unchanged_records
    UNION ALL
    SELECT * FROM unnested_actors_changed_records
    UNION ALL
    SELECT * FROM actors_new_records
) union_records
ORDER BY actorid, start_year;
