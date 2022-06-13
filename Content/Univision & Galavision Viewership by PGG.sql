/* Unision & Galavision live hours by PGG for the past 12 months */


with users as (
Select distinct t1.day,t1.account_code,t2.user_id
from `fubotv-prod.data_insights.daily_status_static_update` t1
inner join `fubotv-dev.business_analytics.account_code_mapping` t2 on t1.account_code=t2.account_code
where final_status_v2_sql not in ('expired','paused','past due from trials and scheduled pause','failed and scheduled pause')
and lower(plan_code) not like '%spain%'
)

, viewership AS
(
Select
t2.day as Date_range, t1.user_id, t5.station_mapping as Channel_name, 
t5.network_owner as Network_Name,
CASE WHEN playback_type = 'live_preview' THEN 'live_preview'
WHEN playback_type = 'live'
 OR playback_type = 'stream ' 
 OR playback_type = 'lite' 
 OR playback_type LIKE 'liv%'THEN 'live'
WHEN playback_type = 'vod' THEN 'vod'
WHEN playback_type = 'dvr' THEN 'dvr'
WHEN playback_type = 'lookback' THEN 'lookback'
WHEN playback_type = 'dvr_startover' THEN 'dvr_startover'
WHEN playback_type = 'start_over' 
 OR playback_type = 'startover' THEN 'startover'
ELSE 'others'
END AS playback_type
, 
primary_genre_group,
primary_genre,
t1.league_names,
t1.program_title,
LOWER(t4.ad_insertable) AS ad_insertable_network, SUM(duration)/3600 as Hours, COUNT(DISTINCT(t1.user_id)) AS Uniques, COUNT(distinct(t1.start_time)) as Streams
from `fubotv-prod.data_insights.video_in_progress_via_va_view` t1
inner join users t2 on t1.user_id=t2.user_id and date(t1.start_time)=t2.day
inner join `fubotv-prod.data_insights.tmsid_genre_mapping` t3 on t1.tms_id=t3.tms_id
left join `fubotv-dev.business_analytics.AdInsertable_Networks` t4 on t1.channel=t4.channel
inner join `fubotv-dev.business_analytics.station_mapping_table_51220` t5 ON t5.station_name = t1.channel
WHERE t3.tms_id IS NOT NULL
AND UPPER(t3.tms_id) NOT IN ("","00000000000000","TMS-ID-UNAVAILABLE")
group by 1,2,3,4,5,6,7,8,9,10
order by 1 DESC
)


SELECT DISTINCT 
# DATE_TRUNC(Date_range,month) as Date_Month ,
CASE WHEN league_names IS NULL THEN program_title ELSE league_names END AS league, 
# primary_genre
 COUNT(DISTINCT user_id) as uniques, SUM(Hours) AS Hours
FROM viewership
WHERE LOWER(channel_name) IN ('univision' , 'galavision')
AND DATE_TRUNC(Date_range,month) >= '2021-05-01'
AND DATE_TRUNC(Date_range,month) <= '2022-04-01'
AND PLAYBACK_TYPE = 'live'
AND LOWER(primary_genre_group) = 'sports'
AND LOWER(primary_genre) = 'soccer'
GROUP BY 1 --,2
ORDER BY 1,2
