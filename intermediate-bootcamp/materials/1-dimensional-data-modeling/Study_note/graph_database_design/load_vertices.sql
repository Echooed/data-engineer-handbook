
--  VERTEX LOADERS
-- a. Load Game Vertices
INSERT INTO vertices
SELECT 
    game_id AS identifier,
    'game'::vertex_type AS type,
    json_build_object(
        'pts_home', pts_home,
        'pts_away', pts_away,
        'winning_team', CASE WHEN home_team_wins = 1 THEN home_team_id ELSE visitor_team_id END
    ) AS properties
FROM games;


-- b. Load Player Vertices
WITH players_agg AS (
    SELECT 
        player_id AS identifier,
        MAX(player_name) AS player_name,
        COUNT(*) AS number_of_games,
        SUM(pts) AS total_points,
        ARRAY_AGG(DISTINCT team_id) AS teams
    FROM game_details
    GROUP BY player_id
)
INSERT INTO vertices
SELECT 
    identifier,
    'player'::vertex_type AS type,
    json_build_object(
        'player_name', player_name,
        'number_of_games', number_of_games,
        'total_points', total_points,
        'team', teams
    ) AS properties
FROM players_agg;


-- c. Load Team Vertices (Deduplicated)
WITH teams_deduped AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY team_id ORDER BY team_id) AS row_num
    FROM teams
)
INSERT INTO vertices
SELECT 
    team_id AS identifier,
    'team'::vertex_type AS type,
    json_build_object(
        'abbreviation', abbreviation,
        'nickname', nickname,
        'city', city,
        'arena', arena,
        'year_founded', yearfounded
    ) AS properties
FROM teams_deduped
WHERE row_num = 1;