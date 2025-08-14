DROP TABLE IF EXISTS players_state_change;

CREATE TABLE players_state_change (
    player_name TEXT,
    first_active_season INTEGER,
    last_active_season INTEGER,
    season_active_state TEXT,
    season_active_list INTEGER[],
    season INTEGER,
    PRIMARY KEY (player_name, season)
);



WITH last_season AS (
    SELECT 
        *
    FROM players_state_change
    WHERE season = 2021
),
present_season AS ( 
    SELECT 
        player_name,
        season
    FROM player_seasons
    WHERE season = 2022
)

INSERT INTO players_state_change

SELECT 
    COALESCE(ls.player_name, ps.player_name) AS player_name,
    COALESCE(ls.first_active_season, ps.season) AS first_active_season,
    CASE
        WHEN ps.season IS NOT NULL THEN ps.season
        ELSE ls.last_active_season
    END AS last_active_season,

    CASE
        WHEN ls.player_name IS NULL AND ps.player_name IS NOT NULL THEN 'new'
        WHEN ps.player_name IS NOT NULL AND ls.last_active_season = ps.season - 1 THEN 'continued Playing'
        WHEN ps.player_name IS NOT NULL AND ls.last_active_season < ps.season - 1 THEN 'Returned from Retirement'
        WHEN ps.player_name IS NULL AND ls.season = ls.last_active_season THEN 'retired'  -- Just retired
        WHEN ps.player_name IS NULL AND ls.season > ls.last_active_season THEN 'stayed_retired'  -- Already retire
        ELSE 'logic_error_caught'
    END as season_active_state,

    COALESCE(ls.season_active_list, ARRAY[]::INTEGER[])||
        CASE
            WHEN ps.season IS NOT NULL THEN ARRAY[ps.season::INTEGER]
            ELSE ARRAY[]::INTEGER[]
        END AS season_active_list,
    COALESCE(ps.season, ls.season + 1) AS season
FROM present_season ps
FULL OUTER JOIN last_season ls 
ON ps .player_name = ls.player_name




