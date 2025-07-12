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

DROP TABLE IF EXISTS edges CASCADE;
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
FROM players_agg


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
)
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



---
SELECT v.properties->>'player_name',
    MAX(e.properties->>'pts')
FROM vertices v
    JOIN edges e ON v.identifier = e.subject_identifier
    AND e.subject_type = v.type
WHERE player_name = 'Michael Jor'
GROUP BY 1
ORDER BY 2 DESC;


---
ggggg

WITH player_max_points AS (
    SELECT  
        v.properties->>'player_name' AS player_name,
        MAX((e.properties->>'pts')::INT) AS max_points
FROM vertices v 
JOIN edges e 
    ON v.identifier = e.subject_identifier 
    AND e.subject_type = v.type
GROUP BY v.properties->>'player_name'
ORDER BY max_points DESC)

SELECT * 
FROM player_max_points 
WHERE max_points IS NOT NULL;








































































SELECT player_name, SUM(pts) FROM game_details GROUP BY Player_name LIMIT 50;

SELECT DISTINCT(nickname), yearfounded FROM teams

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
--   edge_type: EDGE_TYPE                 # Relationship type (e.g., "purchased", "follows")
--   properties: MAP<STRING, STRING>      # Edge metadata (timestamp, context, etc.)