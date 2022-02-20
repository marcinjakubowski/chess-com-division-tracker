WITH data AS (
    SELECT *
         , EXTRACT(EPOCH FROM CASE -- exclude breaks longer than 20 min
             WHEN g.end_time - LAG(g.end_time) OVER win_user_division > INTERVAL '20 minutes'
             THEN NULL
             ELSE g.end_time - LAG(g.end_time) OVER win_user_division
           END) AS duration
      FROM {{ ref('division_games') }} g
    WINDOW win_user_division AS (PARTITION BY g.username, g.division ORDER BY g.end_time)
)
SELECT * FROM data