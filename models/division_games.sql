WITH data AS (
    SELECT games.id
         , games.division
         , games.username
         , games.white_username
         , games.white_rating
         , games.white_result
         , games.black_username
         , games.black_rating
         , games.black_result
         , games.url
         , games.time_control
         , games.time_class
         , games.rules
         , games.end_time
         , CASE
               WHEN games.white_username = games.username THEN 'white'
               ELSE 'black'
           END AS user_color
         , CASE
               WHEN games.white_username = games.username THEN games.white_rating
               ELSE games.black_rating
           END AS user_rating
         , CASE
               WHEN games.white_username = games.username THEN games.white_result
               ELSE games.black_result
           END AS user_result
         , CASE
               WHEN games.white_username = games.username THEN games.black_rating
               ELSE games.white_rating
           END AS opponent_rating
         , CASE
               WHEN games.white_username = games.username THEN games.black_result
               ELSE games.white_result
           END AS opponent_result
         , games.opening
         , games.opening_code
         , openings.name AS opening_short
      FROM {{ source('chesscom', 'games' )}} games
      LEFT
      JOIN {{ ref('opening') }} openings ON openings.code = games.opening_code
       AND games.rules = 'chess'
)
SELECT data.id
     , data.division
     , data.username
     , data.white_username
     , data.white_rating
     , data.white_result
     , data.black_username
     , data.black_rating
     , data.black_result
     , data.url
     , data.time_control
     , data.time_class
     , data.rules
     , data.end_time AT TIME ZONE 'UTC' AT TIME ZONE 'CET' AS end_time
     , data.user_color
     , data.user_rating
     , data.user_result
     , data.opponent_rating
     , data.opponent_result
     , data.opening
     , data.opening_code
     , data.opening_short
  FROM data
