-- --test
-- SELECT * FROM players WHERE years_since_last_season > 1 AND player_name = 'A.C. Green';

-- SELECT max(current_season) FROM players;

-- SELECT MAX(season) FROM player_seasons;

-- SELECT * FROM player_seasons WHERE player_name = 'A.C. Green';



DROP TYPE IF EXISTS films CASCADE;
DROP TYPE IF EXISTS quality_class CASCADE;
DROP TABLE IF EXISTS actors;

CREATE TYPE films AS (
    film TEXT,
    votes INTEGER,
    rating REAL,
    filmid TEXT
);

CREATE TYPE quality_class AS ENUM (
    'star',
    'good',
    'average',
    'bad' 
);

CREATE TABLE actors (
    actorid TEXT,
    actor TEXT,
    films films[],
    quality_class quality_class,
    is_active BOOLEAN,
    current_year  INTEGER,
    PRIMARY KEY (actorid, current_year)
);

INSERT INTO actors
WITH yesterday AS (
    SELECT * FROM actors
    WHERE current_year = 1969
),
today_raw AS (
    SELECT * FROM actor_films
    WHERE year = 1970
),
today AS (
    SELECT 
        actorid,
        actor,
        year,
        ARRAY_AGG(ROW(film, votes, rating, filmid)::films) AS films,
        ROUND(AVG(rating)::numeric, 2) AS avg_rating
    FROM today_raw
    GROUP BY actorid, actor, year
)
SELECT 
    COALESCE(t.actorid, y.actorid) AS actorid,
    COALESCE(t.actor, y.actor) AS actor,
    CASE 
        WHEN y.films IS NULL THEN t.films
        WHEN t.films IS NOT NULL THEN y.films || t.films
        ELSE y.films
    END AS films,
    CASE WHEN t.avg_rating IS NOT NULL THEN 
        CASE 
            WHEN t.avg_rating > 8 THEN 'star'::quality_class
            WHEN t.avg_rating > 7 THEN 'good'::quality_class
            WHEN t.avg_rating > 6 THEN 'average'::quality_class
            ELSE 'bad'::quality_class
        END
    ELSE 
        y.quality_class
    END AS quality_class,
    (t.year = EXTRACT(YEAR FROM CURRENT_DATE)::INT) AS is_active,
    COALESCE(t.year, y.current_year + 1) AS current_year
FROM today t
FULL OUTER JOIN yesterday y ON t.actorid = y.actorid;



SELECT * FROM actors WHERE actor = 'Brigitte Bardot';

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

