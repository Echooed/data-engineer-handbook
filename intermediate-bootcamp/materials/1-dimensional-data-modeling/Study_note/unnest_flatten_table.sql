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


-- This query uses the UNNEST function 
--to expand the array of films into individual rows.

WITH unnested AS (
    SELECT actor,current_year,
        UNNEST(films)::films  AS films
    FROM actors WHERE actor = 'Brigitte Bardot'
),
inner_unnested AS  (
    SELECT actor,
        current_year,
              (films::films).*
    FROM unnested
)
SELECT * FROM inner_unnested;

