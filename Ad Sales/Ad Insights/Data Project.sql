# What are the highest viewed shows / station

WITH active_users as
(
Select distinct t1.day,t1.account_code,t2.user_id,
case when t1.final_status_v2_sql IN ('in trial', 'past due from trials') then 'Trial User'
else 'Paying User' end as user_type
from `fubotv-prod.data_insights.daily_status_static_update` t1
inner join `fubotv-dev.business_analytics.account_code_mapping` t2 on t1.account_code=t2.account_code
where final_status_v2_sql not in ('expired','paused','past due from trials and scheduled pause','failed and scheduled pause')
and lower(plan_code) not like '%canadian%'
and lower(plan_code) not like '%spain%'
and day >= '2020-01-01' ----adjust dates here for a user 
)

, viewership as (
SELECT DATE(v.start_time, 'EST') as event_date,
start_time as event_time,
station_mapping,
v.program_title,
v.duration,
v.user_id,
case when v.episode_title is null then v.program_title else v.episode_title end as event,
lower(t4.ad_insertable) as ad_insertable
from `fubotv-prod.data_insights.video_in_progress_via_va_view` v
inner join `fubotv-prod.data_insights.tmsid_genre_mapping` m on v.tms_id =m.tms_id -- Genre Mapping Table
inner join `fubotv-dev.business_analytics.station_mapping_table_51220` s on s.station_name =v.channel -- Station Mapping Table
inner join `fubotv-dev.business_analytics.AdInsertable_Networks` t4 on v.channel=t4.channel  -- Ad Insertable Table
where v.tms_id IS NOT NULL
AND UPPER(v.tms_id) NOT IN ("","00000000000000","TMS-ID-UNAVAILABLE")
and DATE(v.start_time, 'EST') >= '2020-01-01' -- adjust dates for program
)

, combined as (
Select t3.event_date,
t3.program_title,event, station_mapping, ad_insertable, t1.account_code, t1.user_type, sum(duration)/3600 as Hours
from active_users t1
inner join `fubotv-dev.business_analytics.account_code_mapping` t2 on t1.account_code=t2.user_id
inner join viewership t3 on t2.user_id=t3.user_id and t1.day = date(t3.event_time)
group by 1,2,3,4,5,6,7
)

# Top 10 shows per date
SELECT DISTINCT event_date, event_time, station_mapping, program_title, event, COUNT(user_id) AS viewers, COUNT(DISTINCT(user_id)) AS uniques, sum(duration/3600) as viewing_hours
from viewership
GROUP BY 1,2,3,4,5
ORDER BY 1 DESC, 6 DESC