DROP TABLE IF EXISTS edges CASCADE;
DROP TABLE IF EXISTS vertices CASCADE;


CREATE TYPE vertex_type AS ENUM(
    'player', 
    'team', 
    'game'
);

CREATE TABLE vertices (
    identifier TEXT,
    type vertex_type,
    properties JSON,
    PRIMARY KEY (identifier, type)
);


CREATE TYPE edge_type AS ENUM(
    'plays_against',
    'plays_with',
    'plays_on',
    'plays_in'
);

CREATE TABLE edges (
    subject_identifier TEXT,
    subject_type vertex_type,
    object_identifier TEXT,
    object_type vertex_type,
    edge_type edge_type,
    properties JSON,
    PRIMARY KEY (subject_identifier, 
                subject_type,
                object_identifier,
                object_type, 
                edge_type)
);


---

INSERT INTO vertices
SELECT 
    game_id AS identifier,
    'game'::vertex_type AS type,
    json_build_object(
        'pts_home', pts_home,
        'pts_away', pts_away,
        'winning_team', CASE WHEN home_team_wins = 1 
                        THEN home_team_id ELSE visitor_team_id END
    ) AS properties
FROM games;

---

INSERT INTO vertices
WITH  players_agg AS (
    SELECT 
        player_id                   AS identifier,
        MAX(Player_name)            AS player_name,
        count(player_id)            AS number_of_games,
        SUM(pts)                    AS total_points,
        ARRAY_AGG(DISTINCT team_id) AS teams
    FROM game_details
    GROUP BY player_id   
)
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


---

INSERT INTO vertices
WITH teams_deduped AS
            (SELECT *,
                    ROW_NUMBER() OVER (PARTITION BY team_id) AS row_num
            FROM teams)
 
SELECT 
    team_id AS identifier,
    'team':: vertex_type AS type,
    json_build_object(
        'abbreviation', abbreviation,
        'nickname', nickname,
        'city', city,
        'arena', arena,
        'year_founded', yearfounded
    ) AS properties
FROM teams_deduped
WHERE row_num = 1;


--
INSERT INTO edges
WITH game_details_deduped AS(
        SELECT *,
                ROW_NUMBER() 
                OVER(PARTITION BY player_id, game_id) AS row_num
        FROM game_details
),
    filtered_deduped_game_details AS (
        SELECT * FROM game_details_deduped 
        WHERE row_num = 1
    )
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



-- play against players

WITH game_details_deduped AS(
        SELECT *,
                ROW_NUMBER() 
                OVER(PARTITION BY player_id, game_id) AS row_num
        FROM game_details
),
    filtered_deduped_game_details AS (
        SELECT * FROM game_details_deduped 
        WHERE row_num = 1
    )
SELECT 
    f1.player_name,
    f1.team_abbreviation,
    f2.player_name,
    f2.team_abbreviation
FROM filtered_deduped_game_details f1 
JOIN filtered_deduped_game_details f2
ON f1.game_id = f2.game_id
AND f1.player_name <> f2.player_name;




---


WITH player_max_points AS (
    SELECT  
        v.properties->>'player_name' AS player_name,
        MAX((e.properties->>'pts')::INT) AS max_points
FROM vertices v 
JOIN edges e 
    ON v.identifier = e.subject_identifier 
    AND e.subject_type = v.type
GROUP BY v.properties->>'player_name'
ORDER BY max_points DESC
)

SELECT * 
FROM player_max_points 
WHERE max_points IS NOT NULL;






-- Insert player-to-player relationships into edges table
INSERT INTO edges
WITH game_details_deduped AS (
    -- Deduplicate player-game entries
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY player_id, game_id) AS row_num
    FROM game_details
),

filtered_deduped_game_details AS (
    -- Keep only one entry per player-game pair
    SELECT * 
    FROM game_details_deduped 
    WHERE row_num = 1
),

aggregated AS (
    -- For each pair of players in the same game, classify relationship
    SELECT 
        f1.player_id AS subject_player_id,
        MAX(f1.player_name) AS subject_player_name,

        -- Determine relationship type
        CASE 
            WHEN f1.team_abbreviation = f2.team_abbreviation THEN 'plays_with'::edge_type
            ELSE 'plays_against'::edge_type
        END AS relationship,

        f2.player_id AS object_player_id,
        MAX(f2.player_name) AS object_player_name,

        -- Aggregated stats for the relationship
        COUNT(*) AS num_games,
        SUM(f1.pts) AS subject_points,
        SUM(f2.pts) AS object_points

    FROM filtered_deduped_game_details f1
    JOIN filtered_deduped_game_details f2
        ON f1.game_id = f2.game_id
       AND f1.player_id <> f2.player_id
       AND f1.player_id > f2.player_id  -- prevent duplicates and self-joins

    GROUP BY f1.player_id, f2.player_id, relationship
)

-- Final insert into the edges table
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




SELECT 
  v.properties->> 'player_name' AS player_name,
  e.object_identifier AS object_player_id,
  -- Corrected: total_points / number_of_games
  COALESCE(
    CAST((v.properties->>'total_points') AS REAL) / NULLIF(CAST((v.properties->>'number_of_games') AS REAL), 0),
    0
  ) AS ave_points,
  e.properties->>'subject_points' AS subject_points,
  e.properties->>'num_games' AS num_games
FROM vertices v
JOIN edges e 
  ON v.identifier = e.subject_identifier 
  AND e.subject_type = v.type
WHERE e.object_type = 'player'::vertex_type;




























































-- SELECT 
-- v.properties->> 'player_name',
-- e.object_identifier,
-- CAST((v.properties->>'number_of_games')::REAL) / CASE WHEN CAST((V.properties->>'total_points')::REAL) = 0
-- THEN 1
-- ELSE CAST((v.properties->>'total_points')::REAL) END AS ave_points,
-- e.properties->>'subject_points',
-- e.properties->>'num_games'
-- FROM vertices v 
-- JOIN edges e 
--     ON v.identifier = e.subject_identifier 
--     AND e.subject_type = v.type
-- WHERE e.object_type = 'player'::vertex_type
-- GROUP BY v.properties->>'player_name';


SELECT player_name, SUM(pts) FROM game_details GROUP BY Player_name LIMIT 50;

SELECT_type: EDGE_TYPE                 # Relationship type (e.g., "purchased", "follows")
--   properties: MAP<STRING, STRING>      # Edge metadata (timestamp, context, etc.) DISTINCT(nickname), yearfounded FROM teams

SELECT * FROM games;

SELECT player_id, game_id, count(1) FROM game_details GROUP BY 1, 2;


SELECT * FROM game_details WHERE player_id = '201142' LIMIT 50;


SELECT type, count(1) FROM vertices group by 1;


-- Vertex:
--   identifier: STRING              # Unique ID for the node
--   type: STRING                    # What kind of entity this is (e.g., "user", "product")
--   properties: MAP<STRING, STRING> # Key-value attributes (flexible)


-- Edge:
--   subject_identifier: STRING           # Source vertex ID
--   subject_type: VERTEX_TYPE            # Source vertex type (e.g., "user")
--   object_identifier: STRING            # Target vertex ID
--   object_type: VERTEX_TYPE             # Target vertex type (e.g., "product")
--   edge