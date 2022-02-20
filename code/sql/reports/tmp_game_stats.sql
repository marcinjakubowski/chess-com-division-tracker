with data as (
    select *
         , case -- exclude breaks longer than 20 min
             when end_time - lag(end_time) over (partition by username order by end_time) > interval '20 minutes'
             then NULL
             else end_time - lag(end_time) over (partition by username order by end_time)
           end as duration
      from division_games
)
select username
     , division
     , time_control as time_control
     , count(*) as games_count
     , floor(avg(opponent_rating)) as avg_opponent_rating
     , avg(duration) as avg_game_duration   
     , concat(floor(100.0*count(case when user_result = 'win' then 1 else null end)*1.0/count(*)),'%') as win_ratio
     , concat(floor(100.0*count(case when opponent_result = 'win' then 1 else null end)*1.0/count(*)),'%') as lose_ratio
     , concat(floor(100.0*count(case when user_result = opponent_result then 1 else null end)*1.0/count(*)),'%') as draw_ratio
     , concat(floor(100.0*count(case when user_result = 'agreed' and user_result = opponent_result then 1 else null end)*1.0/count(*)),'%') as draw_by_agreement_ratio
     , sum(count(*)) over (partition by username) as total_games_count
     , concat(floor(100.0*sum(count(case when user_result = 'win' then 1 else null end)) over (partition by username)/sum(count(*)) over (partition by username)),'%') as overall_win_ratio
  from data
 group by username, time_control, division
 having count(*) > 10
 order by division, username, time_control












     


     