-- ENUM TYPES: Vertex and Edge Definitions
DROP TYPE IF EXISTS vertex_type CASCADE;
CREATE TYPE vertex_type AS ENUM (
                                'player', 
                                'team', 
                                'game'
);

DROP TYPE IF EXISTS edge_type CASCADE;
CREATE TYPE edge_type AS ENUM (
                                'plays_against', 
                                'plays_with', 
                                'plays_on',
                                'plays_in'
);


-- Graph table schema to hold Vertex and Edge data

DROP TABLE IF EXISTS vertices CASCADE;
CREATE TABLE vertices (
    identifier TEXT,
    type vertex_type,
    properties JSON,
    PRIMARY KEY (identifier, type)
);

DROP TABLE IF EXISTS edges CASCADE;
CREATE TABLE edges (
    subject_identifier TEXT,
    subject_type vertex_type,
    object_identifier TEXT,
    object_type vertex_type,
    edge_type edge_type,
    properties JSON,
    PRIMARY KEY (   
                    subject_identifier, 
                    subject_type, 
                    object_identifier, 
                    object_type, 
                    edge_type
                )
);