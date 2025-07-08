DROP TABLE IF EXISTS players_scd;


-- Create a table to track Slowly Changing Dimensions (SCD) of players
CREATE TABLE players_scd (
    player_name TEXT,
    scoring_class scoring_class, 
    is_active BOOLEAN,
    start_season INTEGER,       -- Season when the current classification began
    end_season INTEGER,         -- Season when it ended (if ever)
    current_season INTEGER,     -- The current reference season (used for tracking/inserts)
    PRIMARY KEY(player_name, current_season)   
);


-- Insert SCD history based on changes to scoring_class and is_active
-- Tracks changes in scoring_class and is_active over time per player
INSERT INTO players_scd 

-- Get current and previous season values per player using window functions
WITH with_previous AS (
    SELECT
        player_name,
        scoring_class,
        is_active,
        current_season,
        -- Capture previous values for comparison
        LAG(scoring_class) OVER (PARTITION BY player_name ORDER BY current_season) AS previous_scoring_class,
        LAG(is_active) OVER (PARTITION BY player_name ORDER BY current_season) AS previous_is_active
    FROM players
    WHERE current_season <= 2021
),

-- Identify change points (i.e., where scoring_class or is_active changed)
with_indicators AS (
    SELECT *,
        CASE 
            -- use IS DISTINCT FROM to safely compare even if NULLs are involved
            WHEN previous_scoring_class IS DISTINCT FROM scoring_class THEN 1
            WHEN previous_is_active IS DISTINCT FROM is_active THEN 1
            ELSE 0
        END AS change_indicator
    FROM with_previous
),

-- Create streak groups by summing change indicators (streak = unchanged values)
with_streaks AS (
    SELECT *,
        -- each streak_identifier marks a continuous period of the same scoring_class and is_active
        SUM(change_indicator) OVER (
            PARTITION BY player_name 
            ORDER BY current_season
        ) AS streak_identifier
    FROM with_indicators
)

-- Group by streak_identifier to extract SCD periods and compute the start and end season for each period of stability
SELECT
    player_name,
    scoring_class, 
    is_active,
    MIN(current_season) AS start_season,   -- the first season of the streak
    
    CASE 
        WHEN MAX(current_season) = (SELECT MAX(current_season) FROM players)
        THEN NULL
        ELSE MAX(current_season)
    END AS end_season,  -- if the streak is still ongoing (i.e., current season is the latest), set end_season to NULL
    MAX(current_season) AS current_season  -- reference point for PK
FROM with_streaks
GROUP BY 1,2,3, streak_identifier
ORDER BY player_name, start_season;
