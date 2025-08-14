-- Efficient aggregations using GROUPING SETS for basketball analytics
WITH game_aggregations AS (
    SELECT 
        dim_player_id AS player_id,
        dim_player_name AS player_name,
        dim_team_id AS team_id,
        dim_season AS season,
        
        -- Aggregate metrics for analysis
        SUM(m_points) AS total_points,
        COUNT(*) AS games_played,
        SUM(CASE WHEN m_plus_minus > 0 THEN 1 ELSE 0 END) AS games_won,
        
        -- Advanced metrics
        ROUND(AVG(m_points::NUMERIC), 2) AS avg_points_per_game
        
    FROM fct_game_details
    GROUP BY GROUPING SETS (
        -- Level 1: Player + Team (who scored most points for one team?)
        (dim_player_id, dim_player_name, dim_team_id),
        
        -- Level 2: Player + Season (who scored most points in one season?)
        (dim_player_id, dim_player_name, dim_season),
        
        -- Level 3: Team only (which team won most games/scored most points?)
        (dim_team_id)
    )
),

-- Add grouping level identification and rankings
ranked_results AS (
    SELECT *,
        -- Identify which aggregation level this row represents
        CASE 
            WHEN player_id IS NOT NULL AND team_id IS NOT NULL AND season IS NULL 
                THEN 'player_team'
            WHEN player_id IS NOT NULL AND team_id IS NULL AND season IS NOT NULL 
                THEN 'player_season'  
            WHEN player_id IS NULL AND team_id IS NOT NULL AND season IS NULL 
                THEN 'team_only'
            ELSE 'unknown'
        END AS aggregation_level,
        
        -- Rankings within each grouping level
        ROW_NUMBER() OVER (
            PARTITION BY 
                CASE 
                    WHEN player_id IS NOT NULL AND team_id IS NOT NULL AND season IS NULL THEN 'player_team'
                    WHEN player_id IS NOT NULL AND team_id IS NULL AND season IS NOT NULL THEN 'player_season'  
                    WHEN player_id IS NULL AND team_id IS NOT NULL AND season IS NULL THEN 'team_only'
                END
            ORDER BY total_points DESC
        ) AS points_rank,
        
        ROW_NUMBER() OVER (
            PARTITION BY 
                CASE 
                    WHEN player_id IS NOT NULL AND team_id IS NOT NULL AND season IS NULL THEN 'player_team'
                    WHEN player_id IS NOT NULL AND team_id IS NULL AND season IS NOT NULL THEN 'player_season'  
                    WHEN player_id IS NULL AND team_id IS NOT NULL AND season IS NULL THEN 'team_only'
                END
            ORDER BY games_won DESC, total_points DESC
        ) AS wins_rank
        
    FROM game_aggregations
)

-- Final formatted results
SELECT 
    aggregation_level,
    player_name,
    team_id,
    season,
    total_points,
    games_played,
    games_won,
    ROUND(100.0 * games_won / NULLIF(games_played, 0), 1) AS win_percentage,
    avg_points_per_game,
    points_rank,
    wins_rank
FROM ranked_results
ORDER BY 
    -- Group results by aggregation level
    CASE aggregation_level 
        WHEN 'player_team' THEN 1 
        WHEN 'player_season' THEN 2 
        WHEN 'team_only' THEN 3 
    END,
    points_rank;