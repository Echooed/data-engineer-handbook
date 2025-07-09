DROP TABLE IF EXISTS players_scd;

-- Create a table to track Slowly Changing Dimensions (SCD) of players
CREATE TABLE players_scd (
    player_name TEXT,
    scoring_class scoring_class, 
    is_active BOOLEAN,
    start_season INTEGER,       -- Season when the current classification began
    end_season INTEGER,         -- Season when it ended (if ever)
    current_season INTEGER,     -- The current reference season (used for tracking/inserts)
    PRIMARY KEY(player_name, start_season)   -- Changed to start_season for better SCD Type 2 semantics
);

-- Insert SCD Type 2 history based on changes to scoring_class and is_active
-- Tracks changes in scoring_class and is_active over time per player
INSERT INTO players_scd (
    player_name,
    scoring_class,
    is_active,
    start_season,
    end_season,
    current_season
)

-- Filter only records up to the selected snapshot season (e.g. 2021)
WITH filtered_players AS (
    SELECT
        player_name,
        scoring_class,
        is_active,
        current_season
    FROM players
    WHERE current_season <= 2021
),

-- Add previous row values using LAG to detect changes in dimension attributes
with_previous AS (
    SELECT
        player_name,
        scoring_class,
        is_active,
        current_season,
        -- Capture previous values for comparison
        LAG(scoring_class) OVER (PARTITION BY player_name ORDER BY current_season) AS previous_scoring_class,
        LAG(is_active) OVER (PARTITION BY player_name ORDER BY current_season) AS previous_is_active
    FROM filtered_players
),

-- Mark rows where a change in dimension attributes occurs
with_indicators AS (
    SELECT *,
        CASE 
            -- Use IS DISTINCT FROM to safely compare even if NULLs are involved
            WHEN previous_scoring_class IS DISTINCT FROM scoring_class 
              OR previous_is_active IS DISTINCT FROM is_active 
            THEN 1 
            ELSE 0
        END AS change_indicator
    FROM with_previous
),

-- Use running sum to assign streak IDs for unchanged periods
with_streaks AS (
    SELECT *,
        -- Each streak_identifier marks a continuous period of the same scoring_class and is_active
        SUM(change_indicator) OVER (
            PARTITION BY player_name 
            ORDER BY current_season
        ) AS streak_identifier
    FROM with_indicators
),

-- Get the maximum season in the snapshot to identify open-ended records
max_season AS (
    SELECT MAX(current_season) AS max_current_season 
    FROM filtered_players
)

-- Aggregate streaks into start/end periods per player and dimension values
SELECT
    player_name,
    scoring_class, 
    is_active,
    MIN(current_season) AS start_season,     -- When this dimensional state started
    CASE 
        WHEN MAX(current_season) = max_current_season THEN NULL  -- Open-ended streak
        ELSE MAX(current_season)                                 -- Closed streak
    END AS end_season,                       -- When this dimensional state ended (NULL if ongoing)
    MAX(current_season) AS current_season    -- Latest season for this streak (for tracking)
FROM with_streaks
CROSS JOIN max_season
GROUP BY player_name, scoring_class, is_active, streak_identifier, max_current_season
ORDER BY player_name, start_season;




SELECT * FROM players_scd;