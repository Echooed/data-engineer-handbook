INSERT INTO fct_game_details
WITH deduped_game_details AS (
    SELECT 
        g.game_date_est,
        g.season,
        g.home_team_id,
        gd.*,
        ROW_NUMBER() OVER (PARTITION BY gd.game_id, player_id, team_id) AS row_num 
    FROM game_details gd
    JOIN games g ON gd.game_id = g.game_id
)
SELECT 
    game_date_est                                        AS dim_game_date, 
    season                                               AS dim_season, 
    team_id                                              AS dim_team_id,
    player_id                                            AS dim_player_id,
    player_name                                          AS dim_player_name,
    start_position                                       AS dim_start_position,
    team_id = home_team_id                               AS dim_is_playing_at_home,
    COALESCE(POSITION('DNP' IN comment), 0) > 0          AS did_not_play,
    COALESCE(POSITION('DND' IN comment), 0) > 0          AS did_not_dress,
    COALESCE(POSITION('NWT' IN comment), 0) > 0          AS not_with_team,
    ROUND(((SPLIT_PART(min, ':', 1)::REAL + 
    (SPLIT_PART(min, ':', 2)::REAL) / 60))::NUMERIC, 5) AS m_minutes_played,                         -- Convert time string (MM:SS) to decimal minutes
    fgm                                                 AS m_field_goals_made,
    fga                                                 AS m_field_goals_attempted,
    fg3m                                                AS m_three_point_field_goals_made,
    fg3a                                                AS m_three_point_field_goals_attempted,
    ftm                                                 AS m_free_throws_made,
    fta                                                 AS m_free_throws_attempted,
    oreb                                                AS m_offensive_rebounds,
    dreb                                                AS m_defensive_rebounds,
    reb                                                 AS m_total_rebounds,
    ast                                                 AS m_assists,
    stl                                                 AS m_steals,
    blk                                                 AS m_blocks,
    "TO"                                                AS m_turnovers,
    pf                                                  AS m_personal_fouls,
    pts                                                 AS m_points,
    plus_minus                                          AS m_plus_minus  
FROM
     deduped_game_details
WHERE row_num = 1;


