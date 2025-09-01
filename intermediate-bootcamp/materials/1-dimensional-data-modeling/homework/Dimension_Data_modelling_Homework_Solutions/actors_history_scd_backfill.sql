-- Create the quality_class enum type first
DROP TYPE IF EXISTS quality_class CASCADE;
CREATE TYPE quality_class AS ENUM ('star', 'good', 'average', 'bad');

-- Create the films composite type
DROP TYPE IF EXISTS films CASCADE;
CREATE TYPE films AS (
    film TEXT,
    votes INTEGER,
    rating REAL,
    filmid TEXT,
    year INTEGER
);

-- Create the actors table
DROP TABLE actors;
CREATE TABLE actors (
    actorid TEXT,
    actor TEXT,
    films films[],
    quality_class quality_class,
    years_since_last_active INTEGER,
    is_active BOOLEAN,
    current_year INTEGER,
    PRIMARY KEY (actorid, current_year)
);

INSERT INTO actors
WITH 
-- Aggregate films by actor and year (handles multiple films per year)
yearly_films AS (
    SELECT
        actorid,
        actor,
        year,
        AVG(rating) AS avg_rating,
        SUM(votes) AS total_votes,
        CASE 
            WHEN COUNT(*) = 1 THEN MIN(film)
            ELSE 'Multiple Films (' || COUNT(*) || ')'
        END AS film_title,
        CASE 
            WHEN COUNT(*) = 1 THEN MIN(filmid)
            ELSE 'MULTIPLE'
        END AS film_id
    FROM actor_films
    GROUP BY actorid, actor, year
),

-- Get actor career spans and build complete film history
actor_spans AS (
    SELECT
        actorid,
        actor,
        MIN(year) AS first_year,
        MAX(year) AS last_year,
        -- Build complete films array ordered by year
        ARRAY_AGG(
            ROW(film_title, total_votes, avg_rating, film_id, year)::films
            ORDER BY year
        ) AS all_films
    FROM yearly_films
    GROUP BY actorid, actor
),

-- Generate all years from 1970 to 2021
all_years AS (
    SELECT generate_series(1970, 2021) AS year
),

-- Create final result with cumulative film arrays
final_data AS (
    SELECT
        a.actorid,
        a.actor,
        y.year AS current_year,
        a.all_films,
        a.last_year,
        -- Filter films up to current year and create cumulative array
        ARRAY(
            SELECT f 
            FROM UNNEST(a.all_films) AS f
            WHERE (f::films).year <= y.year
        ) AS films,
        -- Get latest film info for classification
        (
            SELECT f 
            FROM UNNEST(a.all_films) AS f
            WHERE (f::films).year <= y.year
            ORDER BY (f::films).year DESC
            LIMIT 1
        ) AS latest_film
    FROM actor_spans a
    CROSS JOIN all_years y
    WHERE y.year >= a.first_year -- Only years from first film onward
)

SELECT 
    actorid,
    actor,
    films,
    
    -- Quality classification based on latest film's average rating
    CASE
        WHEN latest_film IS NULL THEN 'bad'::quality_class
        WHEN (latest_film::films).rating > 8 THEN 'star'::quality_class
        WHEN (latest_film::films).rating > 7 THEN 'good'::quality_class
        WHEN (latest_film::films).rating > 6 THEN 'average'::quality_class
        ELSE 'bad'::quality_class
    END AS quality_class,
    
    -- Years since last active
    CASE 
        WHEN latest_film IS NULL THEN NULL
        ELSE current_year - (latest_film::films).year
    END AS years_since_last_active,
    
    -- Is active this year
    CASE 
        WHEN latest_film IS NULL THEN FALSE
        ELSE (latest_film::films).year = current_year
    END AS is_active,
    
    current_year
FROM final_data
ORDER BY actorid, current_year;