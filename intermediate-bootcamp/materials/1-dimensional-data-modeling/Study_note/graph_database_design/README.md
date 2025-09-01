*** The SQL graph model has been modularized into four logically separated files:**

1. `01_schema.sql` — Defines `vertex_type`, `edge_type`, and the `vertices` and `edges` tables.
2. `02_load_vertices.sql` — Loads games, players, and teams as vertex records.
3. `03_load_edges.sql` — Inserts `plays_in`, `plays_with`, and `plays_against` edge relationships.
4. `04_analysis_queries.sql` — Includes player performance and relationship-based analytics.

Let me know if you'd like this structured for a DBT project, a `.sql` directory, or something exportable.




Graph Design Logic

-- Vertex:
--   identifier: STRING              # Unique ID for the node
--   type: STRING                    # What kind of entity this is (e.g., "user", "product")
--   properties: MAP<STRING, STRING> # Key-value attributes (flexible)


-- Edge:
--   subject_identifier: STRING           # Source vertex ID
--   subject_type: VERTEX_TYPE            # Source vertex type (e.g., "user")
--   object_identifier: STRING            # Target vertex ID
--   object_type: VERTEX_TYPE             # Target vertex type (e.g., "product")
--   edge