-- ANALYTICAL QUERIES
-- a. Max points per player in a single game
WITH player_max_points AS (
    SELECT 
        v.properties->>'player_name' AS player_name,
        MAX((e.properties->>'pts')::INT) AS max_points
    FROM vertices v
    JOIN edges e
        ON v.identifier = e.subject_identifier
        AND v.type = e.subject_type
    GROUP BY v.properties->>'player_name'
)
SELECT * FROM player_max_points WHERE max_points IS NOT NULL;


-- b. Average player points based on edges
SELECT 
    v.properties->>'player_name' AS player_name,
    e.object_identifier AS object_player_id,
    COALESCE(
        (v.properties->>'total_points')::REAL / NULLIF((v.properties->>'number_of_games')::REAL, 0),
        0
    ) AS ave_points,
    e.properties->>'subject_points' AS subject_points,
    e.properties->>'num_games' AS num_games
FROM vertices v
JOIN edges e
    ON v.identifier = e.subject_identifier
    AND v.type = e.subject_type
WHERE e.object_type = 'player'::vertex_type;
