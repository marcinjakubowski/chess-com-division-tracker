WITH calc1 AS (
    SELECT ts.ts
         , ts.division
         , ts.username
         , ts.ranking
         , ts.trophy
         , CASE
             WHEN ts.ts = MAX(ts.ts) OVER (PARTITION BY ts.division) THEN 1
             ELSE 0
           END AS is_most_recent
      FROM {{ ref('active_stats_rounded_time') }} ts
), calc2 AS (
    SELECT cur.ts
         , cur.division
         , cur.username
         , cur.ranking
         , cur.trophy
         , cur.is_most_recent
         , CASE
               WHEN (cur.trophy - ago5.trophy) = 0 AND (cur.trophy - ago15.trophy) = 0 THEN 5
               ELSE 0
           END AS downtime
         , cur.trophy - ago60.trophy AS delta_60min
         , cur.trophy - ago1d.trophy AS delta_1d
         , cur.trophy - ago2d.trophy AS delta_2d
      FROM calc1 cur
      LEFT 
      JOIN calc1 ago5 
        ON ago5.username = cur.username   
       AND ago5.division = cur.division 
       AND ago5.ts = (cur.ts - '00:05:00'::interval)
      LEFT 
      JOIN calc1 ago15 
        ON ago15.username = cur.username 
       AND ago15.division = cur.division 
       AND ago15.ts = (cur.ts - '00:15:00'::interval)
      LEFT 
      JOIN calc1 ago60 
        ON ago60.username = cur.username 
       AND ago60.division = cur.division 
       AND ago60.ts = (cur.ts - '01:00:00'::interval)
      LEFT 
      JOIN calc1 ago1d 
        ON ago1d.username = cur.username 
       AND ago1d.division = cur.division 
       AND ago1d.ts = (cur.ts - '1 day'::interval)
      LEFT 
      JOIN calc1 ago2d 
        ON ago2d.username = cur.username 
       AND ago2d.division = cur.division 
       AND ago2d.ts = (cur.ts - '2 days'::interval)
), final AS (
    SELECT cur.ts
         , cur.division
         , cur.username
         , cur.ranking
         , cur.trophy
         , cur.is_most_recent
         , cur.downtime
         , cur.delta_60min
         , cur.delta_1d
         , cur.delta_2d
         , SUM(cur.downtime) OVER (user_division ROWS BETWEEN 288 PRECEDING AND CURRENT ROW) AS downtime_1d
         , SUM(cur.downtime) OVER (user_division ROWS BETWEEN (2 * 288) PRECEDING AND CURRENT ROW) AS downtime_2d
      FROM calc2 cur
    WINDOW user_division AS (PARTITION BY cur.division, cur.username ORDER BY cur.ts)
)
SELECT final.ts
     , final.division
     , final.username
     , final.ranking
     , final.trophy
     , final.is_most_recent
     , final.downtime
     , final.delta_60min
     , final.delta_1d
     , final.delta_2d
     , final.downtime_1d
     , final.downtime_2d
  FROM final
 WHERE (   final.ranking <= 20
        OR username = 'DamianoLew95')
  



