SELECT * FROM fct_game_details;


SELECT * FROM game_details LIMIT 100


SELECT * FROM devices LIMIT 100;




WITH deduped AS(
SELECT *,
ROW_NUMBER() OVER (PARTITION BY game_id, player_id, team_id) AS row_num
FROM game_details
)
SELECT * FROM deduped
WHERE row_num = 1;


