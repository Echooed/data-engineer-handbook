-- EDGE LOADERS
-- a. Load Player -> Game Edges (plays_in)
WITH game_details_deduped AS (
    SELECT *, ROW_NUMBER() OVER(PARTITION BY player_id, game_id) AS row_num
    FROM game_details
),
filtered_deduped_game_details AS (
    SELECT * FROM game_details_deduped WHERE row_num = 1
)
INSERT INTO edges
SELECT 
    player_id AS subject_identifier,
    'player'::vertex_type AS subject_type,
    game_id AS object_identifier,
    'game'::vertex_type AS object_type,
    'plays_in'::edge_type AS edge_type,
    json_build_object(
        'start_position', start_position,
        'pts', pts,
        'team_id', team_id,
        'team_abbreviation', team_abbreviation
    ) AS properties
FROM filtered_deduped_game_details;


-- b. Load Player <-> Player Edges (plays_with / plays_against)
WITH game_details_deduped AS (
    SELECT *, ROW_NUMBER() OVER(PARTITION BY player_id, game_id) AS row_num
    FROM game_details
),
filtered_deduped_game_details AS (
    SELECT * FROM game_details_deduped WHERE row_num = 1
),
aggregated AS (
    SELECT 
        f1.player_id AS subject_player_id,
        f2.player_id AS object_player_id,
        CASE 
            WHEN f1.team_abbreviation = f2.team_abbreviation THEN 'plays_with'::edge_type
            ELSE 'plays_against'::edge_type
        END AS relationship,
        MAX(f2.player_name) AS object_player_name,
        COUNT(*) AS num_games,
        SUM(f1.pts) AS subject_points,
        SUM(f2.pts) AS object_points
    FROM filtered_deduped_game_details f1
    JOIN filtered_deduped_game_details f2
        ON f1.game_id = f2.game_id
        AND f1.player_id <> f2.player_id
        AND f1.player_id > f2.player_id
    GROUP BY f1.player_id, f2.player_id, relationship
)
INSERT INTO edges
SELECT
    subject_player_id AS subject_identifier,
    'player'::vertex_type AS subject_type,
    object_player_id AS object_identifier,
    'player'::vertex_type AS object_type,
    relationship AS edge_type,
    json_build_object(
        'object_player_name', object_player_name,
        'num_games', num_games,
        'subject_points', subject_points,
        'object_points', object_points
    ) AS properties
FROM aggregated;