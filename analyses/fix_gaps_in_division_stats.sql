WITH ts_rounded AS (
    -- optmizalization, as this will get used a lot across the query
    -- and using a CTE allows the planner to materialize and reuse the data
    -- on mere ~100k division stat records with ~10k active, time goes down from 1:41 to 0:10
    SELECT * FROM {{ ref('active_stats_rounded_time') }} cur
), missing AS (
    SELECT DISTINCT 
           ts
         , ts + INTERVAL '5 minutes' AS missing_ts
      FROM ts_rounded cur
     WHERE NOT EXISTS (SELECT 1 
                         FROM ts_rounded
                        WHERE ts = cur.ts + INTERVAL '5 minutes'
                          AND division = cur.division)
       AND ts != (SELECT MAX(ts) FROM ts_rounded WHERE division = cur.division)
)
INSERT INTO {{ source('chesscom', 'division_stats') }} (ts, division, username, ranking, trophy)
SELECT missing.missing_ts
     , ts_rounded.division
     , ts_rounded.username
     , ts_rounded.ranking
     , ts_rounded.trophy
  FROM ts_rounded
  JOIN missing 
    ON missing.ts = ts_rounded.ts

