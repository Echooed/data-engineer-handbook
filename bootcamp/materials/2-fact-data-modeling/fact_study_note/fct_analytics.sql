SELECT t.*, gd.*
FROM fct_game_details gd JOIN teams t 
ON gd.dim_team_id = t.team_id;

SELECT 
    dim_player_name,
    COUNT(1)::INT AS total_games,
    COUNT(CASE WHEN dim_not_with_team THEN 1 END) AS not_with_team_count
FROM fct_game_details
GROUP BY dim_player_name;

