DROP TYPE IF EXISTS films CASCADE;

CREATE TYPE films AS (
    film TEXT,
    votes INTEGER,
    rating REAL,
    filmid TEXT,
    year INTEGER 
);

-- Build cumulative film record per actor per year
WITH years AS (
    SELECT * FROM GENERATE_SERIES(1970, 2021) AS year
),

-- Find the first year each actor appeared in a film
first_year AS (
    SELECT
        actorid,
        actor,
        MIN(year) AS first_year
    FROM actor_films
    GROUP BY actorid, actor
),

-- Build a cross join of each actor with every year from their first year onward
actors_and_years AS (
    SELECT *
    FROM first_year f
    CROSS JOIN years y
    WHERE f.first_year <= y.year
),  

-- For each actor and year, construct a cumulative array of films up to that year
windowed AS (
    SELECT
        ay.actorid,
        ay.actor,
        ay.year,
         -- build an array of films structs across years using window function
        ARRAY_REMOVE(
            ARRAY_AGG(
                CASE WHEN af.year IS NOT NULL THEN
                    ROW(af.film, af.votes, af.rating, af.filmid, af.year)::films  
                END
            ) OVER (
                PARTITION BY ay.actorid
                ORDER BY COALESCE(af.year, ay.year)
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ),
            NULL -- remove NULLs introduced by years without films
        ) AS films
    FROM actors_and_years ay
    LEFT JOIN actor_films af 
        ON ay.actorid = af.actorid 
        AND ay.year = af.year
    ORDER BY ay.actorid, ay.year
),

-- Get static (unchanging) actor data like actor name and actor id
 static AS (
    SELECT
        actor,
        actorid
    FROM actor_films
    GROUP BY actorid, actor
 )

SELECT 
    w.actorid,
    w.actor,

    -- classify actors based on their latest film rating
    CASE
        WHEN (films[CARDINALITY(films)]::films).rating > 8 THEN
            'star'
        WHEN (films[CARDINALITY(films)]::films).rating > 7 THEN
            'good'
        WHEN (films[CARDINALITY(films)]::films).rating > 6 THEN
            'average'
        ELSE
            'bad'
    END :: quality_class AS quality_class,
    -- calculate years since last active release
    w.year - (films[CARDINALITY(films)]::films).year AS years_since_last_active,
    -- current year for this record
    w.year AS year,
    -- flag actor as active if their latest film year matches the current year
    CASE 
        WHEN CARDINALITY(films) = 0 THEN FALSE
        ELSE (films[CARDINALITY(films)]::films).year = w.year
        END AS is_active
FROM windowed w
JOIN static s ON w.actorid = s.actorid
GROUP BY w.actorid, w.actor, w.year, w.films
ORDER BY w.actorid,w.actor, w.year;

