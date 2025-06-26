--test
SELECT * FROM players WHERE years_since_last_season > 1 AND player_name = 'A.C. Green';

SELECT max(current_season) FROM players;

SELECT MAX(season) FROM player_seasons;