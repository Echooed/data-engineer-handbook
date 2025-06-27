

-- Task 1:
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
    PRIMARY KEY (current_year)
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
        ARRAY_AGG(ROW(film, votes, rating, filmid)::films) AS films
    GROUP BY actorid, actor, year

)
SELECT 
    COALESCE(t.actorid, y.actorid) AS actorid,
    COALESCE(t.actor, y.actor) AS actor_name,
    CASE 
        WHEN y.films IS NULL THEN 
            ARRAY[ROW(t.film, t.votes, t.rating, t.filmid)::films]
        WHEN t.film IS NOT NULL THEN 
            y.films || ARRAY[ROW(t.film, t.votes, t.rating, t.filmid)::films]
        ELSE 
            y.films
    END AS films,
    CASE WHEN t.rating IS NOT NULL THEN 
        CASE 
            WHEN t.rating > 8 THEN 'star'::quality_class
            WHEN t.rating > 7 THEN 'good'::quality_class
            WHEN t.rating > 6 THEN 'average'::quality_class
            ELSE 'bad'::quality_class
        END
    ELSE 
        y.quality_class
    END AS quality_class,
    (t.year = EXTRACT(YEAR FROM CURRENT_DATE)::INT) AS is_active,
    COALESCE(t.year, y.current_year + 1) AS current_year
FROM today t
FULL JOIN yesterday y ON t.actor = y.actor;


--TEST
SELECT * FROM actors where actor_name = 'Brigitte Bardot';



select * from actor_films limit 10;