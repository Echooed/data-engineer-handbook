CREATE TABLE filmss (
    film_id TEXT PRIMARY KEY,
    title TEXT NOT NULL,
    studio TEXT,
    duration_minutes INTEGER,
    release_date DATE
);


CREATE TABLE actorss (
    actor_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    gender TEXT,
    birth_year INTEGER
);


CREATE TABLE genres (
    genre_id TEXT PRIMARY KEY,
    genre_name TEXT NOT NULL
);

DROP TABLE IF EXISTS film_actor_fact CASCADE;
CREATE TABLE film_actor_fact (
    fact_id SERIAL PRIMARY KEY,
    film_id TEXT NOT NULL REFERENCES filmss(film_id),
    actor_id TEXT NOT NULL REFERENCES actorss(actor_id),
    genre_id TEXT NOT NULL REFERENCES genres(genre_id),
    box_office_revenue NUMERIC(12, 2),
    rating NUMERIC(3, 1)
);




INSERT INTO filmss (film_id, title, studio, duration_minutes, release_date) VALUES
('F1', 'The Quantum Enigma', 'Paramount Pictures', 140, '2024-01-01'),
('F2', 'Echoes of Tomorrow', 'Warner Bros.', 125, '2024-02-15'),
('F3', 'Digital Dreams', 'Universal Studios', 110, '2024-03-10');


INSERT INTO actorss (actor_id, name, gender, birth_year) VALUES
('A1', 'Amara Stone', 'F', 1987),
('A2', 'Julius Renner', 'M', 1979),
('A3', 'Lina Park', 'F', 1991),
('A4', 'Carlos Vega', 'M', 1985);


INSERT INTO genres (genre_id, genre_name) VALUES
('G1', 'Sci-Fi'),
('G2', 'Drama'),
('G3', 'Thriller'),
('G4', 'Romance'),
('G5', 'Adventure');

DROP TABLE IF EXIST film_actor_fact CASCADE;

INSERT INTO film_actor_fact (film_id, actor_id, genre_id, box_office_revenue, rating) VALUES
-- F1: 2 actors, 2 genres = 4 rows
('F1', 'A2', 'G1', 100000000.00, 8.5),
('F1', 'A1', 'G3', 100000000.00, 8.5),
('F2', 'A3', 'G2', 60000000.00, 7.2),
('F2', 'A4', 'G4', 60000000.00, 7.2),
('F3', 'A3', 'G1', 85000000.00, 8.0),
('F3', 'A4', 'G5', 85000000.00, 8.0);





SELECT * FROM film_actor_fact;


SELECT film_id,
  MAX(box_office_revenue) AS total_revenue
FROM film_actor_fact
GROUP BY film_id



SELECT SUM(box_office_revenue) AS total_revenue
FROM film_actor_fact;


SELECT 
    film_id, 
    actor_id, 
    genre_id,
    COUNT(1) AS num_rows
FROM film_actor_fact
GROUP BY film_id, actor_id, genre_id
HAVING COUNT(1) > 1;
