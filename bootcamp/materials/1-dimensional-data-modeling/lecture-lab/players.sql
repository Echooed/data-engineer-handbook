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
    current_season INTEGER,
    PRIMARY KEY(player_name, current_season) -- ensure each player and season pair are unique
);

-- Join yesterday and today for comparing player history
INSERT INTO players 
WITH yesterday AS (
    SELECT * FROM players
    WHERE current_season = 2001
),
today AS (
    SELECT * FROM player_seasons
    WHERE season = 2002
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




