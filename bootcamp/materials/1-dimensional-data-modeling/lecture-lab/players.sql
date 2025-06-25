-- This script creates a new table `players` that consolidates player data.
-- Drop the existing type and table if they exist
DROP TYPE IF EXISTS season_stats CASCADE;
DROP TABLE IF EXISTS players;

-- Create the composite type(struct)

CREATE TYPE season_stats AS (
    season INTEGER,
    gp INTEGER,
    pts REAL,
    reb REAL,
    ast REAL
);

-- Create a scoring class enum type to grade players based on their scoring performance
CREATE TYPE scoring_class AS ENUM (
    'star',  -- Excellent
    'good',  -- Good
    'average',  -- Average
    'bad',  -- Below Average
);

-- Create the main players table extracted from player_season table
-- To group related column into a single value.
CREATE TABLE players (
    player_name TEXT,
    height TEXT,
    college TEXT,
    country TEXT,
    draft_year TEXT,
    draft_round TEXT,
    draft_number TEXT,
    season_stats season_stats[],  -- array of composite type created earlier
    scoring_class scoring_class,  -- enum type for player scoring classification
    current_season INTEGER,
    PRIMARY KEY(player_name, current_season) -- ensure each player and season pair are unique
);

-- Join yesterday and today for comparing player history
INSERT INTO players 
WITH yesterday AS (
    SELECT * FROM players
    WHERE current_season = 2005
),
today AS (
    SELECT * FROM player_seasons
    WHERE season = 2006
)
SELECT 
    COALESCE(t.player_name, y.player_name) AS player_name,
    COALESCE(t.height, y.height) AS height,
    COALESCE(t.college, y.college) AS college,
    COALESCE(t.country, y.country) AS country,
    COALESCE(t.draft_year, y.draft_year) AS draft_year,
    COALESCE(t.draft_round, y.draft_round) AS draft_round,
    COALESCE(t.draft_number, y.draft_number) AS draft_number,
    CASE 
        WHEN y.season_stats IS NULL THEN 
            ARRAY[ROW(t.season, t.gp, t.pts, t.reb, t.ast)::season_stats]
        WHEN t.season IS NOT NULL THEN 
            y.season_stats || ARRAY[ROW(t.season, t.gp, t.pts, t.reb, t.ast)::season_stats]
        ELSE 
            y.season_stats
    END AS season_stats,
    COALESCE(t.season, y.current_season + 1) AS current_season
FROM today t
FULL OUTER JOIN yesterday y 
    ON t.player_name = y.player_name;


--------------------------------------------------------------------
-- This query retrieves the player name and their season stats for a specific player in a specific season.
-- It uses the UNNEST function to expand the array of season_stats into individual rows.
WITH unnested AS (
    SELECT player_name,
            UNNEST(season_stats)::season_stats AS season_stats
    FROM players
    WHERE current_season = 2001 
      AND player_name = 'Michael Jordan'
)
SELECT player_name,
       (season_stats::season_stats).*
FROM unnested;  



SELECT max(current_season) FROM players;