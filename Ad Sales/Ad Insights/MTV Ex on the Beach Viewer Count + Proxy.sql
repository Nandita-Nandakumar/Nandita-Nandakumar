--------------- Requests for a bunch of programs to get uniques for target 1 and target 2 as a group.
----------- Entertainment Program

WITH active_users as
(
Select distinct t1.day, t2.user_id
from `fubotv-prod.data_insights.daily_status_static_update` t1
inner join `fubotv-dev.business_analytics.account_code_mapping` t2 on t1.account_code=t2.account_code
where final_status_v2_sql not in ('expired','paused','past due from trials and scheduled pause','failed and scheduled pause')
and lower(plan_code) not like '%canadian%'
and lower(plan_code) not like '%spain%'
and day = '2022-02-14' ----users who are active
)

,viewership AS (
select DISTINCT 
user_id, 
DATE_TRUNC(DATE(v.start_time, "EST"), month) as event_month_est,
date(start_time,"EST") as event_date_est, 
m.primary_genre,
v.playback_type,
station_mapping as channel_name, 
s.network_owner as network_name, 
v.program_title,
v.episode_title,
start_time,
v.duration
from `fubotv-prod.data_insights.video_in_progress_via_va_view` v
inner join `fubotv-prod.data_insights.tmsid_genre_mapping` m on v.tms_id =m.tms_id -- Genre Mapping Table
inner join `fubotv-dev.business_analytics.station_mapping_table_51220` s on s.station_name =v.channel -- Station Mapping Table
inner join `fubotv-dev.business_analytics.AdInsertable_Networks` t4 on v.channel=t4.channel  -- Ad Insertable Table
where v.tms_id IS NOT NULL
AND UPPER(v.tms_id) NOT IN ("","00000000000000","TMS-ID-UNAVAILABLE")
AND LOWER(playback_type) NOT IN ('live_preview')
AND LOWER(m.primary_genre_group) = 'entertainment'
and DATE(v.start_time, 'EST') >= '2019-12-01'
AND 
  ( LOWER(v.program_title) LIKE '	90 day fianc%'
  OR  LOWER(v.program_title) LIKE '90 day:%'
  OR LOWER(v.program_title) = 'the bachelor'
  OR LOWER(v.program_title) = 'the bachelorette'
  OR LOWER(v.program_title) = 'bachelor in paradise'
  OR LOWER(v.program_title) = 'ex on the beach'
  OR LOWER(v.program_title) = 'jersey shore: family vacation'
  OR LOWER(v.program_title) = 'love island'
  OR LOWER(v.program_title) = 'marriage boot camp'
  OR LOWER(v.program_title) = 'married at first sight'
  OR LOWER(v.program_title) = 'siesta key' 
  OR LOWER(v.program_title) = 'temptation island' )

)

, combined as (
Select 
t3.event_month_est, 
event_date_est,
primary_genre,
t3.program_title,
episode_title,
channel_name, 
network_name,
playback_type, 
t1.user_id,
CASE WHEN (LOWER(t3.program_title) = 'ex on the beach'
  OR LOWER(program_title) = 'jersey shore: family vacation'
  OR LOWER(program_title) = 'siesta key' ) THEN "Target 1"
  ELSE "Target 2"
  END as rounds,
CASE WHEN ( (t3.program_title) = 'Love island' OR (program_title) = 'Love Island' ) THEN "Love Island"
  WHEN ( (t3.program_title) LIKE '90 Day Fianc%' OR (program_title) LIKE '90 Day%' ) THEN "90 Day Fiance"
  ELSE program_title
  END as program_title_new,
duration,
from active_users t1
inner join viewership t3 on t1.user_id=t3.user_id --and t1.day = date(t3.start_time)
)

SELECT rounds, program_title_new, SUM(duration/3600) AS Hours, COUNT(DISTINCT user_id) as uniques
FROM combined
GROUP BY 1,2
ORDER BY 1,2