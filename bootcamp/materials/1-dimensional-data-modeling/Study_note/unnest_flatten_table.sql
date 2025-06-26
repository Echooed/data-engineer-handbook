-- It uses the UNNEST function to expand the array of season_stats into individual rows.
WITH unnested AS (
    SELECT player_name,
            UNNEST(season_stats)::season_stats AS season_stats
    FROM players
    WHERE current_season = 2001 
      AND player_name = 'Michael Jordan'
)
SELECT player_name,
       (season_stats::season_stats).*
FROM unnested; 