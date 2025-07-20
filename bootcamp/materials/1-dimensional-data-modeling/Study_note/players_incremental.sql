-- Drop the SCD row type if it exists
DROP TYPE IF EXISTS scd_type CASCADE;

-- Define a composite type to represent a Slowly Changing Dimension (SCD) row
CREATE TYPE scd_type AS (
    scoring_class scoring_class,
    is_active BOOLEAN,
    start_season INTEGER,
    end_season INTEGER
);



WITH last_season_scd AS (
    -- Select latest known SCD rows from 2021
    SELECT *
    FROM players_scd
    WHERE current_season = 2021
      AND (end_season IS NULL OR end_season = 2021)  -- open or closed in 2021
),

historical_scd AS (
    -- Keep historical records before 2022 unchanged
    SELECT 
        player_name,
        scoring_class,
        is_active,
        start_season,
        end_season,
        current_season
    FROM players_scd
    WHERE current_season = 2021
      AND end_season < 2021
),

this_season_data AS (
    -- New snapshot data from 2022
    SELECT *
    FROM players
    WHERE current_season = 2022
),

unchanged_records AS (
    -- Players with no change: update their end_season to 2022
    SELECT 
        ts.player_name,
        ts.scoring_class,
        ts.is_active,
        ls.start_season,
        ts.current_season AS end_season,
        ts.current_season AS current_season
    FROM this_season_data ts
    JOIN last_season_scd ls ON ts.player_name = ls.player_name
    WHERE ts.scoring_class = ls.scoring_class
      AND ts.is_active = ls.is_active
),

changed_records AS (
    -- Players with changes: split into old and new rows
    SELECT 
        ts.player_name,
        UNNEST(ARRAY[
            -- Previous streak: ends in 2021
            ROW(
                ls.scoring_class,
                ls.is_active,
                ls.start_season,
                2021
            )::scd_type,
            -- New streak: starts in 2022
            ROW(
                ts.scoring_class,
                ts.is_active,
                ts.current_season,
                ts.current_season
            )::scd_type
        ]) AS records
    FROM this_season_data ts
    JOIN last_season_scd ls ON ts.player_name = ls.player_name
    WHERE ts.scoring_class IS DISTINCT FROM ls.scoring_class
       OR ts.is_active IS DISTINCT FROM ls.is_active
),

unnested_changed_records AS (
    -- Flatten SCD composite array into proper columns
    SELECT 
        player_name,
        (records).scoring_class AS scoring_class,
        (records).is_active AS is_active,
        (records).start_season AS start_season,
        (records).end_season AS end_season,
        (records).end_season AS current_season
    FROM changed_records
),

new_records AS (
    -- Players newly appearing in 2022
    SELECT 
        ts.player_name,
        ts.scoring_class,
        ts.is_active,
        ts.current_season AS start_season,
        ts.current_season AS end_season,
        ts.current_season AS current_season
    FROM this_season_data ts
    LEFT JOIN last_season_scd ls ON ts.player_name = ls.player_name
    WHERE ls.player_name IS NULL
)

SELECT *, 2022 AS current_season FROM (
    SELECT * FROM historical_scd
    UNION ALL
    SELECT * FROM unchanged_records
    UNION ALL  
    SELECT * FROM unnested_changed_records
    UNION ALL       
    SELECT * FROM new_records
) union_records
ORDER BY player_name, start_season;

