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
         , {{ match_username('\'white\'', '\'black\'') }} AS user_color
         , {{ match_username('games.white_rating', 'games.black_rating') }} AS user_rating
         , {{ match_username('games.white_result', 'games.black_result') }} AS user_result
         , {{ match_username('games.black_rating', 'games.white_rating') }} AS opponent_rating
         , {{ match_username('games.black_rating', 'games.white_rating') }} AS opponent_result
         , games.opening
         , games.opening_code
         , openings.name AS opening_short
      FROM {{ source('chesscom', 'games' ) }} games
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
