WITH data AS (
    SELECT *
         , EXTRACT(EPOCH FROM CASE -- exclude breaks longer than 20 min
             WHEN end_time - LAG(end_time) OVER (PARTITION BY username, division ORDER BY end_time) > INTERVAL '20 minutes'
             THEN NULL
             ELSE end_time - LAG(end_time) OVER (PARTITION BY username, division ORDER BY end_time)
           end) as duration
      FROM games_division
)
SELECT * FROM data