WITH count_streak AS (
    SELECT g.username
         , g.division
         , g.end_time
         , SUM(CASE WHEN g.user_result != 'win' THEN 1 ELSE NULL END)
             OVER (PARTITION BY g.username, g.division ORDER BY g.end_time) AS streak_no
      FROM {{ ref('division_games') }} g
     WHERE EXISTS (SELECT 1 
                     FROM {{ source('chesscom', 'division') }} d 
                    WHERE d.id = g.division 
                      AND d.is_active)
)
SELECT cs.username
     , cs.division
     , COUNT(*) AS streak
     , MIN(end_time) AS streak_start
  FROM count_streak cs
 GROUP BY cs.username
        , cs.division
        , cs.streak_no
HAVING COUNT(*) > 1
