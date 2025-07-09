-- DROP TABLE IF EXISTS actors;
-- CREATE TABLE actors (
--     actorid TEXT,
--     actor TEXT,
--     films films[],
--     quality_class quality_class,
--     years_since_last_active INTEGER,
--     is_active BOOLEAN,
--     current_year  INTEGER,
--     PRIMARY KEY (actorid, current_year)
-- );

-- DROP TYPE IF EXISTS films CASCADE;
-- CREATE TYPE films AS (
--     film TEXT,
--     votes INTEGER,
--     rating REAL,
--     filmid TEXT,
--     year INTEGER
-- );


-- -- Create the quality_class enum type first
-- DROP TYPE IF EXISTS quality_class CASCADE;
-- CREATE TYPE quality_class AS ENUM ('star', 'good', 'average', 'bad');

-- -- Create the films composite type
-- DROP TYPE IF EXISTS films CASCADE;
-- CREATE TYPE films AS (
--     film TEXT,
--     votes INTEGER,
--     rating REAL,
--     filmid TEXT,
--     year INTEGER
-- );

-- -- Create the actors table
-- DROP TABLE IF EXISTS actors;
-- CREATE TABLE actors (
--     actorid TEXT,
--     actor TEXT,
--     films films[],
--     quality_class quality_class,
--     years_since_last_active INTEGER,
--     is_active BOOLEAN,
--     current_year INTEGER,
--     PRIMARY KEY (actorid, current_year)
-- );

-- INSERT INTO actors
-- -- Build cumulative film record per actor per year
-- WITH years AS (
--     SELECT * FROM GENERATE_SERIES(1970, 2021) AS year
-- ),

-- -- Find the first year each actor appeared in a film
-- first_year AS (
--     SELECT
--         actorid,
--         actor,
--         MIN(year) AS first_year
--     FROM actor_films
--     GROUP BY actorid, actor
-- ),

-- -- Aggregate films by actor and year to handle multiple films per year
-- yearly_aggregated AS (
--     SELECT
--         actorid,
--         actor,
--         year,
--         -- Calculate average rating for this actor in this year
--         AVG(rating) AS avg_rating,
--         -- Sum votes for all films this year
--         SUM(votes) AS total_votes,
--         -- Create array of all films for this year
--         ARRAY_AGG(
--             ROW(film, votes, rating, filmid, year)::films 
--             ORDER BY rating DESC  -- Order by rating within the year
--         ) AS films_this_year
--     FROM actor_films
--     GROUP BY actorid, actor, year
-- ),

-- -- Build a cross join of each actor with every year from their first year onward
-- actors_and_years AS (
--     SELECT *
--     FROM first_year f
--     CROSS JOIN years y
--     WHERE f.first_year <= y.year
-- ),  

-- -- For each actor and year, construct a cumulative array of films up to that year
-- windowed AS (
--     SELECT
--         ay.actor,
--         ay.actorid,
--         ay.year,
--         -- Build a cumulative array of films structs across years using window function
--         ARRAY_REMOVE(
--             ARRAY_AGG(
--                 CASE WHEN ya.year IS NOT NULL THEN
--                     -- Create one film entry per year with average rating
--                     ROW(
--                         CASE 
--                             WHEN ARRAY_LENGTH(ya.films_this_year, 1) = 1 THEN 
--                                 (ya.films_this_year[1]::films).film
--                             ELSE 
--                                 'Multiple Films (' || ARRAY_LENGTH(ya.films_this_year, 1) || ')'
--                         END,
--                         ya.total_votes,
--                         ya.avg_rating,
--                         CASE 
--                             WHEN ARRAY_LENGTH(ya.films_this_year, 1) = 1 THEN 
--                                 (ya.films_this_year[1]::films).filmid
--                             ELSE 
--                                 'MULTIPLE'
--                         END,
--                         ya.year
--                     )::films
--                 END
--             ) OVER (
--                 PARTITION BY ay.actorid
--                 ORDER BY COALESCE(ya.year, ay.year)
--                 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
--             ),
--             NULL -- remove NULLs introduced by years without films
--         ) AS films
--     FROM actors_and_years ay
--     LEFT JOIN yearly_aggregated ya
--         ON ay.actorid = ya.actorid 
--         AND ay.year = ya.year
--     ORDER BY ay.actorid, ay.year
-- )

-- -- Final SELECT with proper handling of edge cases
-- SELECT 
--     w.actorid,
--     w.actor,
--     w.films,

--     -- Classify actors based on their latest film average rating (handle empty arrays)
--     CASE
--         WHEN CARDINALITY(w.films) = 0 THEN 'bad'::quality_class
--         WHEN (w.films[CARDINALITY(w.films)]::films).rating > 8 THEN 'star'::quality_class
--         WHEN (w.films[CARDINALITY(w.films)]::films).rating > 7 THEN 'good'::quality_class
--         WHEN (w.films[CARDINALITY(w.films)]::films).rating > 6 THEN 'average'::quality_class
--         ELSE 'bad'::quality_class
--     END AS quality_class,

--     -- Calculate years since last active (handle empty arrays)
--     CASE 
--         WHEN CARDINALITY(w.films) = 0 THEN NULL
--         ELSE w.year - (w.films[CARDINALITY(w.films)]::films).year
--     END AS years_since_last_active,
    
--     -- Flag actor as active if their latest film year matches the current year
--     CASE 
--         WHEN CARDINALITY(w.films) = 0 THEN FALSE
--         ELSE (w.films[CARDINALITY(w.films)]::films).year = w.year
--     END AS is_active,

--     -- Current year for this record
--     w.year AS current_year
-- FROM windowed w
-- ORDER BY w.actorid, w.year;




-- --------------------------------------------------------

-- -- Create the quality_class enum type first
-- DROP TYPE IF EXISTS quality_class CASCADE;
-- CREATE TYPE quality_class AS ENUM ('star', 'good', 'average', 'bad');

-- -- Create the films composite type
-- DROP TYPE IF EXISTS films CASCADE;
-- CREATE TYPE films AS (
--     film TEXT,
--     votes INTEGER,
--     rating REAL,
--     filmid TEXT,
--     year INTEGER
-- );

-- -- Create the actors table
-- DROP TABLE actors;
-- CREATE TABLE actors (
--     actorid TEXT,
--     actor TEXT,
--     films films[],
--     quality_class quality_class,
--     years_since_last_active INTEGER,
--     is_active BOOLEAN,
--     current_year INTEGER,
--     PRIMARY KEY (actorid, current_year)
-- );

-- INSERT INTO actors
-- WITH 
-- -- Aggregate films by actor and year (handles multiple films per year)
-- yearly_films AS (
--     SELECT
--         actorid,
--         actor,
--         year,
--         AVG(rating) AS avg_rating,
--         SUM(votes) AS total_votes,
--         CASE 
--             WHEN COUNT(*) = 1 THEN MIN(film)
--             ELSE 'Multiple Films (' || COUNT(*) || ')'
--         END AS film_title,
--         CASE 
--             WHEN COUNT(*) = 1 THEN MIN(filmid)
--             ELSE 'MULTIPLE'
--         END AS film_id
--     FROM actor_films
--     GROUP BY actorid, actor, year
-- ),

-- -- Get actor career spans and build complete film history
-- actor_spans AS (
--     SELECT
--         actorid,
--         actor,
--         MIN(year) AS first_year,
--         MAX(year) AS last_year,
--         -- Build complete films array ordered by year
--         ARRAY_AGG(
--             ROW(film_title, total_votes, avg_rating, film_id, year)::films
--             ORDER BY year
--         ) AS all_films
--     FROM yearly_films
--     GROUP BY actorid, actor
-- ),

-- -- Generate all years from 1970 to 2021
-- all_years AS (
--     SELECT generate_series(1970, 2021) AS year
-- ),

-- -- Create final result with cumulative film arrays
-- final_data AS (
--     SELECT
--         a.actorid,
--         a.actor,
--         y.year AS current_year,
--         a.all_films,
--         a.last_year,
--         -- Filter films up to current year and create cumulative array
--         ARRAY(
--             SELECT f 
--             FROM UNNEST(a.all_films) AS f
--             WHERE (f::films).year <= y.year
--         ) AS films,
--         -- Get latest film info for classification
--         (
--             SELECT f 
--             FROM UNNEST(a.all_films) AS f
--             WHERE (f::films).year <= y.year
--             ORDER BY (f::films).year DESC
--             LIMIT 1
--         ) AS latest_film
--     FROM actor_spans a
--     CROSS JOIN all_years y
--     WHERE y.year >= a.first_year -- Only years from first film onward
-- )

-- SELECT 
--     actorid,
--     actor,
--     films,
    
--     -- Quality classification based on latest film's average rating
--     CASE
--         WHEN latest_film IS NULL THEN 'bad'::quality_class
--         WHEN (latest_film::films).rating > 8 THEN 'star'::quality_class
--         WHEN (latest_film::films).rating > 7 THEN 'good'::quality_class
--         WHEN (latest_film::films).rating > 6 THEN 'average'::quality_class
--         ELSE 'bad'::quality_class
--     END AS quality_class,
    
--     -- Years since last active
--     CASE 
--         WHEN latest_film IS NULL THEN NULL
--         ELSE current_year - (latest_film::films).year
--     END AS years_since_last_active,
    
--     -- Is active this year
--     CASE 
--         WHEN latest_film IS NULL THEN FALSE
--         ELSE (latest_film::films).year = current_year
--     END AS is_active,
    
--     current_year
-- FROM final_data
-- ORDER BY actorid, current_year;


























-- -- INSERT INTO actors
-- -- -- Build cumulative film record per actor per year
-- -- WITH years AS (
-- --     SELECT *
-- --     FROM GENERATE_SERIES(1970, 2021) AS year
-- -- ),
-- -- -- Find the first year each actor appeared in a film
-- -- first_year AS (
-- --     SELECT actorid,
-- --         actor,
-- --         MIN(year) AS first_year
-- --     FROM actor_films
-- --     GROUP BY actorid,
-- --         actor
-- -- ),
-- -- -- Build a cross join of each actor with every year from their first year onward
-- -- actors_and_years AS (
-- --     SELECT *
-- --     FROM first_year f
-- --         CROSS JOIN years y
-- --     WHERE f.first_year <= y.year
-- -- ),
-- -- -- For each actor and year, construct a cumulative array of films up to that year
-- -- windowed AS (
-- --     SELECT ay.actorid,
-- --         ay.actor,
-- --         ay.year,
-- --         -- build an array of films structs across years using a window function
-- --         ARRAY_REMOVE(
-- --             ARRAY_AGG(
-- --                 CASE
-- --                     WHEN af.year IS NOT NULL THEN (af.film, af.votes, af.rating, af.filmid, af.year)::films
-- --                 END
-- --             ) OVER (
-- --                 PARTITION BY ay.actorid
-- --                 ORDER BY COALESCE(af.year, ay.year) ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
-- --             ),
-- --             NULL -- remove NULLs introduced by years without films
-- --         ) AS films
-- --     FROM actors_and_years ay
-- --         LEFT JOIN actor_films af ON ay.actorid = af.actorid
-- --         AND ay.year = af.year
-- --     ORDER BY ay.actorid,
-- --         ay.year
-- -- ),
-- -- -- Get static (unchanging) actor data like name and ID
-- -- static AS (
-- --     SELECT actor,
-- --         actorid
-- --     FROM actor_films
-- --     GROUP BY actorid,
-- --         actor
-- -- )
-- -- SELECT w.actorid,
-- --     w.actor,
-- --     w.films,
-- --     -- force every actor-year to 'average'
-- --     'average'::quality_class AS quality_class,
-- --     -- calculate years since last active release
-- --     w.year - (w.films [CARDINALITY(w.films)]::films).year AS years_since_last_active,
-- --     -- flag actor as active if their latest film year matches the current year
-- --     CASE
-- --         WHEN CARDINALITY(w.films) = 0 THEN FALSE
-- --         ELSE (w.films [CARDINALITY(w.films)]::films).year = w.year
-- --     END AS is_active,
-- --     -- current year for this record
-- --     w.year AS current_year
-- -- FROM windowed w
-- --     JOIN static s ON w.actorid = s.actorid
-- -- GROUP BY w.actorid,
-- --     w.actor,
-- --     w.year,
-- --     w.films
-- -- ORDER BY w.actorid,
-- --     w.actor,
-- --     w.year;

-----------------------------------------------------------------

-- -- Original actor_scd query

-- INSERT INTO actors_scd_history
-- -- Get current and previous slowly changing value for each actor.
-- WITH with_previous AS (
--     SELECT 
--         actorid,
--         actor,
--         quality_class,
--         is_active,
--         current_year,
--         LAG(quality_class) OVER (PARTITION BY actorid ORDER BY current_year) AS previous_quality_class,
--         LAG(is_active) OVER (PARTITION BY actorid ORDER BY current_year) AS previous_is_active
--     FROM actors
--     WHERE current_year <= 2021
-- ),

-- with_indicator AS (
--     SELECT *,
--         CASE 
--             WHEN previous_quality_class IS DISTINCT FROM quality_class THEN 1
--             WHEN previous_is_active IS DISTINCT FROM is_active THEN 1
--             ELSE 0
--         END AS change_indicator
--     FROM with_previous
-- ),

-- with_streaks AS (
--     SELECT *,
--         SUM(change_indicator) 
--         OVER (PARTITION BY actorid ORDER BY current_year) AS streak_identifier
--     FROM with_indicator
-- )

-- SELECT 
--     actorid,
--     actor,
--     quality_class,
--     is_active,
--     MAX(current_year) AS current_year,
--     MIN(current_year) AS start_year,
--     CASE 
--         WHEN MAX(current_year) = (SELECT MAX(current_year) FROM actors)
--         THEN NULL
--         ELSE MAX(current_year)
--     END AS end_year
-- FROM with_streaks
-- GROUP BY actorid, actor, quality_class, is_active, streak_identifier
-- ORDER BY actor, start_year;


-- Refactored SCD Type 2 incremental logic for 2022 season
-- Handles: unchanged, changed, new, and reactivated players

-- Identify 2021 SCD baseline (open streaks or ended in 2021)
WITH last_season_scd AS (
    SELECT * FROM players_scd
    WHERE current_season = 2021
      AND (end_season IS NULL OR end_season = 2021)
),

   historical_scd AS (
        SELECT
            player_name,
               scoring_class,
               is_active,
               start_season,
               end_season
        FROM players_scd
        WHERE current_season = 2021
        AND end_season < 2021
     ),
-- Identify players from 2022 season
this_season_data AS (
    SELECT * FROM players
    WHERE current_season = 2022
),


-- Players who were active in 2021 and unchanged in 2022
unchanged_records AS (
    SELECT 
        ts.player_name,
        ts.scoring_class,
        ts.is_active,
        ls.start_season,
        ts.current_season AS end_season
    FROM this_season_data ts
    JOIN last_season_scd ls 
        ON ts.player_name = ls.player_name
    WHERE ts.scoring_class = ls.scoring_class
      AND ts.is_active = ls.is_active
      AND ls.is_active = TRUE
),

-- Players who were active in 2021 but changed in 2022
changed_records AS (
    SELECT 
        ts.player_name,
        UNNEST(ARRAY[
            -- Old streak (ending in 2021)
            ROW(ls.scoring_class, ls.is_active, ls.start_season, 2021)::scd_type,
            -- New streak (starting in 2022)
            ROW(ts.scoring_class, ts.is_active, 2022, 2022)::scd_type
        ]) AS record
    FROM this_season_data ts
    JOIN last_season_scd ls 
        ON ts.player_name = ls.player_name
    WHERE (ts.scoring_class IS DISTINCT FROM ls.scoring_class 
        OR ts.is_active IS DISTINCT FROM ls.is_active)
      AND ls.is_active = TRUE
),

unnested_changed_records AS (
    SELECT 
        player_name,
        (record).scoring_class AS scoring_class,
        (record).is_active AS is_active,
        (record).start_season AS start_season,
        (record).end_season AS end_season
    FROM changed_records
),

-- New players not in last season's SCD
new_players AS (
    SELECT 
        ts.player_name,
        ts.scoring_class,
        ts.is_active,
        2022 AS start_season,
        2022 AS end_season
    FROM this_season_data ts
    LEFT JOIN last_season_scd ls 
        ON ts.player_name = ls.player_name
    WHERE ls.player_name IS NULL
),

-- Reactivated players (inactive in 2021, active in 2022)
reactivated_players AS (
    SELECT 
        ts.player_name,
        ts.scoring_class,
        ts.is_active,
        2022 AS start_season,
        2022 AS end_season
    FROM this_season_data ts
    JOIN last_season_scd ls 
        ON ts.player_name = ls.player_name
    WHERE ls.is_active = FALSE AND ts.is_active = TRUE
)

-- Final union of all record types
SELECT *, 2022 AS current_season FROM (
    SELECT * FROM historical_scd
    UNION ALL
    SELECT * FROM unchanged_records
    UNION ALL
    SELECT * FROM unnested_changed_records
    UNION ALL
    SELECT * FROM new_players
    UNION ALL
    SELECT * FROM reactivated_players
) all_records;



SELECT * FROM players_scd WHERE player_name = 'LeBron James';

SELECT * FROM players_scd;









-- Insert new Type 2 records into players_scd
INSERT INTO players_scd (
    player_name,
    scoring_class,
    is_active,
    start_season,
    end_season,
    current_season
)

WITH last_season_scd AS (
    -- Grab records from 2021 where the dimension was still active
    SELECT * 
    FROM players_scd
    WHERE current_season = 2021
      AND (end_season IS NULL OR end_season = 2021)
),

historical_scd AS (
    -- Keep all rows that ended before 2021 as-is
    SELECT 
        player_name,
        scoring_class,
        is_active,
        start_season,
        end_season,
        current_season
    FROM players_scd
    WHERE current_season = 2021
      AND end_season < 2021
),

this_season_data AS (
    -- Get the current (2022) player data
    SELECT * 
    FROM players
    WHERE current_season = 2022
),

unchanged_records AS (
    -- Extend the previous record if nothing changed
    SELECT 
        ts.player_name,
        ts.scoring_class,
        ts.is_active,
        ls.start_season,
        2022 AS end_season,
        2022 AS current_season
    FROM this_season_data ts
    JOIN last_season_scd ls 
      ON ts.player_name = ls.player_name
    WHERE ts.scoring_class = ls.scoring_class
      AND ts.is_active = ls.is_active
),

changed_records AS (
    -- If there's a change, create 2 rows: one to close old, one to open new
    SELECT 
        ts.player_name,
        UNNEST(ARRAY[
            ROW(
                ls.scoring_class, 
                ls.is_active, 
                ls.start_season, 
                2021 -- close old record
            )::scd_type,
            ROW(
                ts.scoring_class, 
                ts.is_active, 
                2022, 
                NULL -- open new record
            )::scd_type
        ]) AS records
    FROM this_season_data ts
    JOIN last_season_scd ls 
      ON ts.player_name = ls.player_name
    WHERE ts.scoring_class IS DISTINCT FROM ls.scoring_class
       OR ts.is_active IS DISTINCT FROM ls.is_active
),

unnested_changed_records AS (
    -- Flatten those two-row arrays with correct current_season tagging
    SELECT 
        player_name,
        (records).scoring_class,
        (records).is_active,
        (records).start_season,
        (records).end_season,
        CASE 
            WHEN (records).start_season = 2022 THEN 2022
            ELSE 2021
        END AS current_season
    FROM changed_records
),

new_records AS (
    -- Players newly appearing in 2022 (no 2021 record)
    SELECT  
        ts.player_name,
        ts.scoring_class,
        ts.is_active,
        ts.current_season AS start_season,
        NULL AS end_season,
        ts.current_season AS current_season
    FROM this_season_data ts
    LEFT JOIN last_season_scd ls
        ON ts.player_name = ls.player_name
    WHERE ls.player_name IS NULL
)

-- Final UNION of all 2022-relevant SCD output
SELECT * FROM historical_scd
UNION ALL
SELECT * FROM unchanged_records
UNION ALL
SELECT * FROM unnested_changed_records
UNION ALL
SELECT * FROM new_records;






INSERT INTO players_scd (
    player_name,
    scoring_class,
    is_active,
    start_season,
    end_season,
    current_season
)

WITH last_season_scd AS (
    -- Select latest known SCD rows from 2021
    SELECT *
    FROM players_scd
    WHERE current_season = 2021
      AND (end_season IS NULL OR end_season = 2021)  -- open or closed in 2021
),

historical_scd AS (
    -- Keep historical records before 2022 unchanged
    SELECT 
        player_name,
        scoring_class,
        is_active,
        start_season,
        end_season,
        current_season
    FROM players_scd
    WHERE current_season = 2021
      AND end_season < 2021
),

this_season_data AS (
    -- New snapshot data from 2022
    SELECT *
    FROM players
    WHERE current_season = 2022
),

unchanged_records AS (
    -- Players with no change: update their end_season to 2022
    SELECT 
        ts.player_name,
        ts.scoring_class,
        ts.is_active,
        ls.start_season,
        ts.current_season AS end_season,
        ts.current_season AS current_season
    FROM this_season_data ts
    JOIN last_season_scd ls ON ts.player_name = ls.player_name
    WHERE ts.scoring_class = ls.scoring_class
      AND ts.is_active = ls.is_active
),

changed_records AS (
    -- Players with changes: split into old and new rows
    SELECT 
        ts.player_name,
        UNNEST(ARRAY[
            -- Previous streak: ends in 2021
            ROW(
                ls.scoring_class,
                ls.is_active,
                ls.start_season,
                2021
            )::scd_type,
            -- New streak: starts in 2022
            ROW(
                ts.scoring_class,
                ts.is_active,
                ts.current_season,
                ts.current_season
            )::scd_type
        ]) AS records
    FROM this_season_data ts
    JOIN last_season_scd ls ON ts.player_name = ls.player_name
    WHERE ts.scoring_class IS DISTINCT FROM ls.scoring_class
       OR ts.is_active IS DISTINCT FROM ls.is_active
),

unnested_changed_records AS (
    -- Flatten SCD composite array into proper columns
    SELECT 
        player_name,
        (records).scoring_class AS scoring_class,
        (records).is_active AS is_active,
        (records).start_season AS start_season,
        (records).end_season AS end_season,
        (records).end_season AS current_season
    FROM changed_records
),

new_records AS (
    -- Players newly appearing in 2022
    SELECT 
        ts.player_name,
        ts.scoring_class,
        ts.is_active,
        ts.current_season AS start_season,
        ts.current_season AS end_season,
        ts.current_season AS current_season
    FROM this_season_data ts
    LEFT JOIN last_season_scd ls ON ts.player_name = ls.player_name
    WHERE ls.player_name IS NULL
)

SELECT *, 2022 AS current_season FROM (
    SELECT * FROM historical_scd
    UNION ALL
    SELECT * FROM unchanged_records
    UNION ALL  
    SELECT * FROM unnested_changed_records
    UNION ALL       
    SELECT * FROM new_records
) union_records
ORDER BY player_name, start_season;






























SELECT *, 2022 AS current_season FROM (
                  SELECT *
                  FROM historical_scd

                  UNION ALL

                  SELECT *
                  FROM unchanged_records

                  UNION ALL

                  SELECT *
                  FROM unnested_changed_records

                  UNION ALL

                  SELECT *
                  FROM new_records
              ) a



















-- Final UNION with proper type alignment
SELECT * FROM historical_scd
UNION ALL
SELECT * FROM unchanged_records
UNION ALL
SELECT * FROM unnested_changed_records
UNION ALL
SELECT * FROM new_records;

