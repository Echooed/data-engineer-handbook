-- Drop the SCD row type if it exists
DROP TYPE IF EXISTS scd_type CASCADE;

-- Define a composite type to represent a Slowly Changing Dimension (SCD) row
CREATE TYPE scd_type AS (
    scoring_class scoring_class,
    is_active BOOLEAN,
    start_season INTEGER,
    end_season INTEGER
);

-- Get SCD records from the previous season (2021) that were still active that year
WITH last_season_scd AS (
    SELECT * FROM players_scd
    WHERE current_season = 2021
    AND end_season = 2021
),

-- Get historical SCD records that ended before 2021
historical_scd AS (
    SELECT 
        player_name,
        scoring_class, 
        is_active,
        start_season,
        end_season
    FROM players_scd
    WHERE current_season = 2021
    AND end_season < 2021
),      

-- Get player records for the current season (2022)
this_season_data AS (
    SELECT * FROM players
    WHERE current_season = 2022
),
        
-- Select players whose scoring_class and is_active have not changed since 2021
-- Extend their end_season to 2022
unchanged_records AS (
    SELECT 
        ts.player_name,
        ts.scoring_class,
        ts.is_active,
        ls.start_season,
        ts.current_season AS end_season
    FROM this_season_data ts 
    JOIN last_season_scd ls 
        ON ts.player_name = ls.player_name
    WHERE ts.scoring_class = ls.scoring_class
      AND ts.is_active = ls.is_active
),

-- Identify players whose scoring_class or is_active has changed
-- Split them into two rows: one for the previous streak, one for the new
changed_records AS (
    SELECT 
        ts.player_name,
        UNNEST(ARRAY[
            -- Old streak (ending in 2021)
            ROW(
                ls.scoring_class, 
                ls.is_active, 
                ls.start_season, 
                ls.end_season
            )::scd_type,
            -- New streak (starting in 2022)
            ROW(
                ts.scoring_class, 
                ts.is_active, 
                ts.current_season, 
                ts.current_season
            )::scd_type
        ]) AS records
    FROM this_season_data ts 
    JOIN last_season_scd ls 
        ON ts.player_name = ls.player_name
    WHERE ts.scoring_class IS DISTINCT FROM ls.scoring_class
       OR ts.is_active IS DISTINCT FROM ls.is_active
),

-- Flatten the changed_records array back into individual columns
unnested_changed_records AS (
    SELECT 
        player_name,
        (records).scoring_class AS scoring_class,
        (records).is_active AS is_active,
        (records).start_season AS start_season,
        (records).end_season AS end_season
    FROM changed_records
),

-- Identify new players who have no record in last season's SCD table
new_records AS (
    SELECT  
        ts.player_name,
        ts.scoring_class,
        ts.is_active,
        ts.current_season AS start_season,
        ts.current_season AS end_season
    FROM this_season_data ts
    LEFT JOIN last_season_scd ls
        ON ts.player_name = ls.player_name
    WHERE ls.player_name IS NULL
)

-- Combine all record types: history, unchanged, changed, and new
SELECT *, 2022 AS current_season FROM (
    SELECT * FROM historical_scd
    UNION ALL
    SELECT * FROM unchanged_records
    UNION ALL  
    SELECT * FROM unnested_changed_records
    UNION ALL       
    SELECT * FROM new_records
) union_records;
