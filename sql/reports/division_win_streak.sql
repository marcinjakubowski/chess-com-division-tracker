WITH streak_count AS (
    SELECT *
         , SUM(CASE WHEN user_result != 'win' THEN 1 ELSE NULL END) OVER (PARTITION BY username, division ORDER BY end_time) AS streak_no
      FROM division_games
)
SELECT division
     , username
     , COUNT(*) AS streak
     , MIN(end_time) AT TIME ZONE 'UTC' AT TIME ZONE 'CET' as streak_start 
  FROM streak_count
 WHERE user_result = 'win'
 GROUP BY username, division, streak_no
HAVING COUNT(*) > 5
 ORDER BY COUNT(*) desc