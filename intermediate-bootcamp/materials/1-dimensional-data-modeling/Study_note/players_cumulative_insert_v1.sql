-- consolidated player records into the 'players' table
INSERT INTO players
-- Generate a list of seasons from 1996 to 2022
WITH years AS (
    SELECT *
    FROM GENERATE_SERIES(1996, 2022) AS season
),

-- Find the first season each player appeared in
p AS (
    SELECT
        player_name,
        MIN(season) AS first_season
    FROM player_seasons
    GROUP BY player_name
),

-- Build a cross join of each player with every season from their first season onward
players_and_seasons AS (
    SELECT *
    FROM p
    JOIN years y
        ON p.first_season <= y.season
),

-- For each player and season, construct a cumulative array of season_stats up to that year
windowed AS (
    SELECT
        pas.player_name,
        pas.season,
        -- Build an array of season_stat structs across seasons using a window function
        ARRAY_REMOVE(
            ARRAY_AGG(
                CASE
                    WHEN ps.season IS NOT NULL THEN
                        ROW(ps.season, ps.gp, ps.pts, ps.reb, ps.ast)::season_stats
                END
            ) OVER (
                PARTITION BY pas.player_name
                ORDER BY COALESCE(pas.season, ps.season)
            ),
            NULL -- remove NULLs introduced by seasons without activity
        ) AS seasons
    FROM players_and_seasons pas
    LEFT JOIN player_seasons ps
        ON pas.player_name = ps.player_name
        AND pas.season = ps.season
    ORDER BY pas.player_name, pas.season
),

-- Get static (unchanging) player data like height, college, draft info
static AS (
    SELECT
        player_name,
        MAX(height) AS height,
        MAX(college) AS college,
        MAX(country) AS country,
        MAX(draft_year) AS draft_year,
        MAX(draft_round) AS draft_round,
        MAX(draft_number) AS draft_number
    FROM player_seasons
    GROUP BY player_name
)

-- Final SELECT: combine season-level and static info to produce a full row per player-season
SELECT
    w.player_name,
    s.height,
    s.college,
    s.country,
    s.draft_year,
    s.draft_round,
    s.draft_number,
    seasons AS season_stats,

    -- Classify player based on most recent (latest) season's points
    CASE
        WHEN (seasons[CARDINALITY(seasons)]::season_stats).pts > 20 THEN 'star'
        WHEN (seasons[CARDINALITY(seasons)]::season_stats).pts > 15 THEN 'good'
        WHEN (seasons[CARDINALITY(seasons)]::season_stats).pts > 10 THEN 'average'
        ELSE 'bad'
    END::scoring_class AS scoring_class,

    -- Calculate how many years have passed since last active season
    w.season - (seasons[CARDINALITY(seasons)]::season_stats).season AS years_since_last_active,

    -- Current season for this record
    w.season,

    -- Flag if this season is the player’s most recent active one
    (seasons[CARDINALITY(seasons)]::season_stats).season = season AS is_active

FROM windowed w
JOIN static s
    ON w.player_name = s.player_name;

 
