WITH active_stats AS (
    -- optmizalization, as this will get used a lot across the query
    -- and using a CTE allows the planner to materialize and reuse the data
    -- on mere ~100k division stat records with ~10k active, time goes down from 1:41 to 0:10
    SELECT * FROM {{ ref('active_stats_rounded_time') }} cur
), min_max AS (
    -- find out the min and max time for which the data is available per division
    SELECT division
         , MIN(ts) AS min_ts
         , MAX(ts) AS max_ts 
      FROM active_stats
     GROUP BY division
), generator AS (
    -- generate all timestamps between min and max for each division
    SELECT min_max.division
         , gen.ts
      FROM min_max
     CROSS
      JOIN 
   LATERAL generate_series(min_max.min_ts, min_max.max_ts, '5 minutes'::interval) gen(ts)
), next_last AS (
    -- match the generated timestamp against real timestamps
    -- for the gen ts that are missing a match, get the last previously available
    -- and the next following available timestamps
    SELECT gen.division
         , gen.ts
         , stats.ts AS matched_ts
         , stats.username
         , stats.trophy
         , stats.ranking
         , MAX(stats.ts) OVER (user_division ORDER BY gen.ts)       AS last_available
         , MIN(stats.ts) OVER (user_division ORDER BY gen.ts DESC)  AS next_available
      FROM generator gen
      LEFT OUTER
      JOIN active_stats stats
        ON gen.division = stats.division
       AND gen.ts = stats.ts
    WINDOW user_division AS (PARTITION BY gen.division)
), calc AS (
    -- for the missing timestamps, join the last and next available
    -- find out how many 5 minute intervals are missing in total between the last and next (t_calc.total)
    -- and how many for a given generated ts have passed since the last available (t_calc.passed)
    -- trophy score delta is then multiplied by passed / total to create an approximation of how many
    -- points would be gained between the last available ts (for when the real score is available)
    -- and the generated ts; adding that many points to the last available score creates an approximation
    -- ranking also needs to be calculated as it could have changed between last and next, so
    -- it needs to be calculated dynamically for the generated points as well
    SELECT next_last.division
         , next_last.ts
         , stats_last.username
         , calc.trophy
         , ROW_NUMBER() OVER (PARTITION BY next_last.division, next_last.ts ORDER BY calc.trophy DESC) AS ranking
      FROM next_last
      JOIN active_stats stats_last
        ON stats_last.division = next_last.division
       AND stats_last.ts = next_last.last_available
      JOIN active_stats stats_next
        ON stats_next.division = next_last.division
       AND stats_next.ts = next_last.next_available
       AND stats_next.username = stats_last.username
     CROSS
      JOIN -- deltas for time and score
   LATERAL (SELECT EXTRACT(EPOCH FROM next_last.ts - next_last.last_available) / EXTRACT(EPOCH FROM '5 minutes'::interval) AS passed
                 , EXTRACT(EPOCH FROM next_last.next_available - next_last.last_available) / EXTRACT(EPOCH FROM '5 minutes'::interval) AS total
                 , stats_next.trophy - stats_last.trophy AS delta
         ) AS t_calc
     CROSS
      JOIN -- trophy score calc
   LATERAL (SELECT stats_last.trophy + FLOOR(t_calc.delta * (t_calc.passed / t_calc.total)) AS trophy
         ) AS calc
     WHERE next_last.matched_ts IS NULL -- no match against real stats
)
INSERT INTO {{ source('chesscom', 'division_stats') }}
(division, ts, username, trophy, ranking)
SELECT calc.division
     , calc.ts AT TIME ZONE 'CET' AT TIME ZONE 'UTC' -- convert back from CET to UTC
     , calc.username
     , calc.trophy
     , calc.ranking
  FROM calc
