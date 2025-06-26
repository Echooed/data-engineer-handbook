

-- Task 1:


CREATE TYPE Actors AS (
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

CREATE TABLE Actor_films (
    actor TEXT ,
    actorid TEXT ,
    film TEXT ,
    year INTEGER ,
    votes INTEGER ,
    rating REAL ,
    filmid TEXT ,
    Actors Actors[],
    quality quality_class,
    PRIMARY KEY (actorid, filmid)
);

SELECT MIN(year) FROM Actor_films;