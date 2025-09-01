
WITH points_diff AS (
        SELECT  player_name, 
        (season_stats[1]::season_stats).pts AS first_season,
        (season_stats[cardinality(season_stats)]::season_stats).pts AS last_season
FROM players WHERE current_season = (SELECT MAX(current_season) FROM players)
)
SELECT player_name, 
       first_season, 
       last_season, 
       last_season  / CASE WHEN first_season = 0 THEN 1 ELSE first_season END AS pts_growth
FROM points_diff;
