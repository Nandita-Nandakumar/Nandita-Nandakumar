# NBA Campaign - Repull for Dec 2021 -- Users who watched 2+ games for > 2 mins per game
with users as (
Select distinct t1.day,t1.account_code,t2.user_id,
from `fubotv-prod.data_insights.daily_status_static_update` t1
inner join `fubotv-dev.business_analytics.account_code_mapping` t2 on t1.account_code=t2.account_code
where final_status_v2_sql like ('%paid%')
and lower(plan_code) not like '%canad%'
and lower(plan_code) not like '%spain%'
and day >= '2020-12-22' ----adjust dates here
and day <= '2021-05-16'
),
viewership as (

SELECT 
*,
sum(duration)/3600 as hours, 
COUNT(episode_title) as game_count
FROM 
(
Select date_trunc(t2.day,month) as month,
t2.day as streaming_day,
station_mapping, 
t1.user_id, 
t1.program_title,
CASE WHEN  t1.episode_title IS NULL THEN t1.program_title ELSE t1.episode_title END AS episode_title,
duration
from `fubotv-prod.data_insights.video_in_progress_via_va_view` t1
inner join users t2 on t1.user_id=t2.user_id and date(t1.start_time)=t2.day
inner join `fubotv-prod.data_insights.tmsid_genre_mapping` t3 on t1.tms_id=t3.tms_id   --Genre Mapping Table
--inner join `fubotv-dev.business_analytics.AdInsertable_Networks` t4 on t1.channel=t4.channel -- Ad Insertable Table
inner join `fubotv-dev.business_analytics.station_mapping_table_51220` t5 on t1.channel=t5.station_name -- Station Mapping Table
WHERE t3.tms_id IS NOT NULL
AND UPPER(t3.tms_id) NOT IN ("","00000000000000","TMS-ID-UNAVAILABLE")
and primary_genre = 'Basketball' 
and (t1.league_names like '%NBA%' OR t1.program_title IN ('NBA Basketball'))
and lower(playback_type) = 'live'
AND duration > 120 --  2+ minutes of the game
)
group by 1,2,3,4,5,6,7
)

select 
user_id, 
sum(hours) as hours,
SUM(game_count) as game_count
from viewership
group by 1
order by 1 asc

# NBA Campaign - Repull for Dec 2021 -- Users who watched 2+ games for > 5 mins per game

with users as (
Select distinct t1.day,t1.account_code,t2.user_id,
from `fubotv-prod.data_insights.daily_status_static_update` t1
inner join `fubotv-dev.business_analytics.account_code_mapping` t2 on t1.account_code=t2.account_code
where final_status_v2_sql like ('%paid%')
and lower(plan_code) not like '%canad%'
and lower(plan_code) not like '%spain%'
and day >= '2020-12-22' ----adjust dates here
and day <= '2021-05-16'
),
viewership as (

SELECT 
*,
sum(duration)/3600 as hours, 
COUNT(episode_title) as game_count
FROM 
(
Select date_trunc(t2.day,month) as month,
t2.day as streaming_day,
station_mapping, 
t1.user_id, 
t1.program_title,
CASE WHEN  t1.episode_title IS NULL THEN t1.program_title ELSE t1.episode_title END AS episode_title,
duration
from `fubotv-prod.data_insights.video_in_progress_via_va_view` t1
inner join users t2 on t1.user_id=t2.user_id and date(t1.start_time)=t2.day
inner join `fubotv-prod.data_insights.tmsid_genre_mapping` t3 on t1.tms_id=t3.tms_id   --Genre Mapping Table
--inner join `fubotv-dev.business_analytics.AdInsertable_Networks` t4 on t1.channel=t4.channel -- Ad Insertable Table
inner join `fubotv-dev.business_analytics.station_mapping_table_51220` t5 on t1.channel=t5.station_name -- Station Mapping Table
WHERE t3.tms_id IS NOT NULL
AND UPPER(t3.tms_id) NOT IN ("","00000000000000","TMS-ID-UNAVAILABLE")
and primary_genre = 'Basketball' 
and (t1.league_names like '%NBA%' OR t1.program_title IN ('NBA Basketball'))
and lower(playback_type) = 'live'
AND duration > 300 --  5+ minutes of the game
)
group by 1,2,3,4,5,6,7
)

select 
user_id, 
sum(hours) as hours,
SUM(game_count) as game_count
from viewership
group by 1
order by 1 asc
