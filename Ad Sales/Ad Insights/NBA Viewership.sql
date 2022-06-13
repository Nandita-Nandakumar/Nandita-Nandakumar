with users as (
Select distinct t1.day,t1.account_code,t2.user_id,
case when t1.final_status_v2_sql IN ('in trial', 'past due from trials') then 'Trial User'
else 'Paying User' end as user_type
from `fubotv-prod.data_insights.daily_status_static_update` t1
inner join `fubotv-dev.business_analytics.account_code_mapping` t2 on t1.account_code=t2.account_code
where final_status_v2_sql like ('%paid%')
and lower(plan_code) not like '%canadian%'
and lower(plan_code) not like '%spain%'
and day >= '2021-05-19' ----adjust dates here
--and day <= '2021-03-31'
),
viewership as (
Select date_trunc(t2.day,month) as month,t2.day as streaming_day,station_mapping, user_type, t1.user_id, sum(duration)/3600 as hours
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
group by 1,2,3,4,5
)
select  user_id, sum(hours) as hours,count(distinct user_id) as uniques
from viewership
group by 1
order by 1 asc


------------------------------ NBA Viewership including everyone who watched 6_hours in 2021 -----------------------------------------

WITH viewership as (
Select user_id, sum(duration)/3600 as Hours
from `fubotv-prod.data_insights.video_in_progress_via_va_view` t1
inner join `fubotv-prod.data_insights.tmsid_genre_mapping` t3 on t1.tms_id=t3.tms_id   --Genre Mapping Table
--inner join `fubotv-dev.business_analytics.AdInsertable_Networks` t4 on t1.channel=t4.channel -- Ad Insertable Table
inner join `fubotv-dev.business_analytics.station_mapping_table_51220` t5 on t1.channel=t5.station_name -- Station Mapping Table
WHERE t3.tms_id IS NOT NULL
AND UPPER(t3.tms_id) NOT IN ("","00000000000000","TMS-ID-UNAVAILABLE")
and primary_genre = 'Basketball' 
and (t1.league_names like '%NBA%' OR t1.program_title IN ('NBA Basketball'))
and lower(playback_type) = 'live'
AND DATE_TRUNC(start_time,year) = '2021-01-01' -- 2021 viewers 
group by 1
)
, users AS 
(
select  user_id, Hours
from viewership
WHERE Hours >= 6
AND user_id IS NOT NULL
)

SELECT COUNT(DISTINCT user_id) as Nba_viewers_2021
FROM users
order by 1 asc