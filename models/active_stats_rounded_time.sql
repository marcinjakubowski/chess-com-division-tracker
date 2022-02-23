SELECT (date_trunc('hour', stats.ts) + (date_part('minute', stats.ts) / 5) * '00:05:00'::interval)
       AT TIME ZONE 'UTC' AT TIME ZONE 'CET' AS ts
     , stats.division
     , stats.username
     , stats.ranking
     , stats.trophy
  FROM {{ source('chesscom', 'division_stats') }} stats
 WHERE {{ is_active_division('stats.division') }}