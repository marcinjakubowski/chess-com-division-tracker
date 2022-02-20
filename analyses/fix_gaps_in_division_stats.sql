WITH ts_rounded AS (
    SELECT (date_trunc('hour', stats.ts) + (date_part('minute', stats.ts)::integer / 5)::double precision * '00:05:00'::interval) AS ts,
         , stats.division
         , stats.username
         , stats.ranking
         , stats.trophy
      FROM {{ source('chesscom', 'division_stats' )}} stats
), missing AS (
    SELECT DISTINCT 
           ts
          , ts + INTERVAL '5 minutes' AS missing_ts
      FROM ts_rounded cur
     WHERE NOT EXISTS (SELECT 1 
                         FROM ts_rounded 
                        WHERE ts = cur.ts + INTERVAL '5 minutes')
       AND ts != (SELECT MAX(ts) FROM ts_rounded)
)
INSERT INTO division_stats (ts, division, username, ranking, trophy)
SELECT missing.missing_ts
     , ts_rounded.division
     , ts_rounded.username
     , ts_rounded.ranking
     , ts_rounded.trophy
  FROM ts_rounded
  JOIN missing 
    ON missing.ts = ts_rounded.ts

