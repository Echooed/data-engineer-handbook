
-- This script creates a new table `players` that consolidates player data.
-- Drop the existing type and table if they exist
DROP TYPE IF EXISTS season_statss CASCADE;
DROP TABLE IF EXISTS playerss;
DROP TYPE IF EXISTS scoring_classs CASCADE;

-- Create the composite type(struct)

CREATE TYPE season_statss AS (
    season INTEGER,
    gp INTEGER,
    pts REAL,
    reb REAL,
    ast REAL
);

-- Create a scoring class enum type to grade players based on their scoring performance
CREATE TYPE scoring_classs AS ENUM (
    'star',  -- Excellent
    'good',  -- Good
    'average',  -- Average
    'bad' -- Below Average
);

-- Create the main players table extracted from player_season table
-- To group related column into a single value.
CREATE TABLE playerss (
    player_name TEXT,
    height TEXT,
    college TEXT,
    country TEXT,
    draft_year TEXT,
    draft_round TEXT,
    draft_number TEXT,
    season_statss season_statss[],  -- array of composite type created earlier
    scoring_classs scoring_classs,  -- enum type for player scoring classification
    years_since_last_season INTEGER,  -- number of years since the last season played
    current_season INTEGER,
    PRIMARY KEY(player_name, current_season) -- ensure each player and season pair are unique
);

--1) Ensure our target table is empty
TRUNCATE playerss;

-- 2) Use a recursive CTE to walk seasons 2000→2020
WITH RECURSIVE player_history AS (

  -- Anchor member: start with season = 2000 data from player_seasons
  SELECT
    p.player_name,
    p.height,
    p.college,
    p.country,
    p.draft_year,
    p.draft_round,
    p.draft_number,
    ARRAY[ROW(p.season, p.gp, p.pts, p.reb, p.ast)::season_statss]            AS season_statss,
    CASE 
      WHEN p.pts >= 25 THEN 'star'::scoring_classs
      WHEN p.pts >= 20 THEN 'good'::scoring_classs
      WHEN p.pts >= 15 THEN 'average'::scoring_classs
      ELSE 'bad'::scoring_classs
    END                                                                        AS scoring_classs,
    0                                                                          AS years_since_last_season,
    p.season                                                                   AS current_season
  FROM player_seasons p
  WHERE p.season = 1996

  UNION ALL

  -- Recursive member: join each prior row to the next season’s stats
  SELECT
    COALESCE(next.player_name, prev.player_name)                             AS player_name,
    COALESCE(next.height,       prev.height)                                 AS height,
    COALESCE(next.college,      prev.college)                                AS college,
    COALESCE(next.country,      prev.country)                                AS country,
    COALESCE(next.draft_year,   prev.draft_year)                             AS draft_year,
    COALESCE(next.draft_round,  prev.draft_round)                            AS draft_round,
    COALESCE(next.draft_number, prev.draft_number)                           AS draft_number,

    -- Append the new season row if it exists
    CASE
      WHEN next.season IS NULL THEN prev.season_statss
      ELSE prev.season_statss || ROW(next.season, next.gp, next.pts, next.reb, next.ast)::season_statss
    END                                                                        AS season_statss,

    -- Recompute scoring_class for the new season if present, else carry forward
    CASE
      WHEN next.season IS NOT NULL THEN
        CASE 
          WHEN next.pts >= 25 THEN 'star'::scoring_classs
          WHEN next.pts >= 20 THEN 'good'::scoring_classs
          WHEN next.pts >= 15 THEN 'average'::scoring_classs
          ELSE 'bad'::scoring_classs
        END
      ELSE prev.scoring_classs
    END                                                                        AS scoring_classs,

    -- If they played this season, reset years_since_last_season; else +1
    CASE
      WHEN next.season IS NOT NULL THEN 0
      ELSE prev.years_since_last_season + 1
    END                                                                        AS years_since_last_season,

    COALESCE(next.season, prev.current_season + 1)                             AS current_season

  FROM player_history AS prev

  -- left-join to the next year’s raw data
  LEFT JOIN player_seasons AS next
    ON next.player_name = prev.player_name
   AND next.season      = prev.current_season + 1

  -- stop once we’ve reached 2022
  WHERE prev.current_season < 2022
)

-- 3) Insert all years 2001–2022 into players
INSERT INTO playerss (
  player_name, height, college, country,
  draft_year, draft_round, draft_number,
  season_statss, scoring_classs, years_since_last_season, current_season
)
SELECT
  player_name, height, college, country,
  draft_year, draft_round, draft_number,
  season_statss, scoring_classs, years_since_last_season, current_season
FROM player_history
WHERE current_season BETWEEN 2001 AND 2022;
--ORDER BY player_name, current_season;


SELECT * FROM playerss WHERE years_since_last_season > 1 AND player_name = 'A.C. Green';