-- Task 1:
DROP TYPE IF EXISTS films CASCADE;
DROP TYPE IF EXISTS quality_class CASCADE;
DROP TABLE IF EXISTS actors;

-- Create composite type for film info
CREATE TYPE films AS (
    film TEXT,
    votes INTEGER,
    rating REAL,
    filmid TEXT
);

-- Create ENUM type for quality classification
CREATE TYPE quality_class AS ENUM (
    'star',
    'good',
    'average',
    'bad' 
);

-- Create main actors table
CREATE TABLE actors (
    actorid TEXT,
    actor TEXT,
    films films[],
    quality_class quality_class,
    is_active BOOLEAN,
    current_year  INTEGER,
    PRIMARY KEY (current_year, actorid)
);

-- Insert new data using SCD logic
WITH 
yesterday AS (
    SELECT * FROM actors
    WHERE current_year = 1974
),
today_raw AS (
    SELECT * FROM actor_films
    WHERE year = 1975
),
today AS (
    SELECT 
        actorid,
        actor,
        year,
        ARRAY_AGG(ROW(film, votes, rating, filmid)::films) AS films,
        AVG(rating) AS avg_rating
    FROM today_raw
    GROUP BY actorid, actor, year
)
INSERT INTO actors
SELECT 
    COALESCE(t.actorid, y.actorid) AS actorid,
    COALESCE(t.actor, y.actor) AS actor,
    CASE 
        WHEN y.films IS NULL THEN t.films
        WHEN t.films IS NOT NULL THEN y.films || t.films
        ELSE y.films
    END AS films,
    CASE 
        WHEN t.avg_rating IS NOT NULL THEN 
            CASE 
                WHEN t.avg_rating > 8 THEN 'star'::quality_class
                WHEN t.avg_rating > 7 THEN 'good'::quality_class
                WHEN t.avg_rating > 6 THEN 'average'::quality_class
                ELSE 'bad'::quality_class
            END
        ELSE y.quality_class
    END AS quality_class,
    (t.year IS NOT NULL) AS is_active,
    COALESCE(t.year, y.current_year + 1) AS current_year
FROM today t
FULL JOIN yesterday y ON t.actorid = y.actorid;



--TEST
SELECT * FROM actors WHERE actor = 'David Warner';
(t.year = EXTRACT(YEAR FROM CURRENT_DATE)::INT) AS is_active
(seasons[CARDINALITY(seasons)]::season_stats).season = season AS is_active
