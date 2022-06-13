----------------------------------------- VIEWERSHIP ----------------------------------------
with users as (
Select distinct t1.day,t1.account_code,t2.user_id, t1.plan_code, plan_type, final_status_v2_sql,
case when t1.final_status_v2_sql IN ('in trial') then 'Trial User'
else 'Paying User' end as user_type,
from `fubotv-prod.data_insights.daily_status_static_update` t1
inner join `fubotv-dev.business_analytics.account_code_mapping` t2 on t1.account_code=t2.account_code
inner join `fubotv-prod.business_analytics.NN_plancode_to_plantype_v2` t3 on t1.plan_code = t3.plan_code
where LOWER(final_status_restated) IN ('paid', 'paid and scheduled pause','paid but canceled', 'in trial') 
and lower(t1.plan_code) not like '%spain%'
and day >= '2020-01-01'
)

, viewership_excl_dis AS
(
Select
t1.user_id,
t2.day as Date_range, t5.station_mapping as Channel_Name, 
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
END AS playback_type, 
t1.program_title,
LOWER(t4.ad_insertable) AS ad_insertable_network, SUM(duration)/3600 as Hours, COUNT(DISTINCT(t1.user_id)) AS Uniques, COUNT(distinct(t1.start_time)) as Streams
from `fubotv-prod.data_insights.video_in_progress_via_va_view` t1
inner join users t2 on t1.user_id=t2.user_id and date(t1.start_time)=t2.day
inner join `fubotv-prod.data_insights.tmsid_genre_mapping` t3 on t1.tms_id=t3.tms_id
inner join `fubotv-dev.business_analytics.AdInsertable_Networks` t4 on t1.channel=t4.channel
inner join `fubotv-dev.business_analytics.station_mapping_table_51220` t5 ON t5.station_name = t1.channel
WHERE t3.tms_id IS NOT NULL
AND UPPER(t3.tms_id) NOT IN ("","00000000000000","TMS-ID-UNAVAILABLE")
AND lower(station_mapping) NOT IN ('disney channel' , 'disney junior')
group by 1,2,3,4,5,6,7
order by 1 DESC
)

, viewership_for_dis_vod AS
(
Select
t1.user_id,
t2.day as Date_range, t5.station_mapping as Channel_Name,
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
END AS playback_type, 
t1.program_title,
'yes' AS ad_insertable_network, SUM(duration)/3600 as Hours, COUNT(DISTINCT(t1.user_id)) AS Uniques, COUNT(distinct(t1.start_time)) as Streams
from `fubotv-prod.data_insights.video_in_progress_via_va_view` t1
inner join users t2 on t1.user_id=t2.user_id and date(t1.start_time)=t2.day
inner join `fubotv-prod.data_insights.tmsid_genre_mapping` t3 on t1.tms_id=t3.tms_id
inner join `fubotv-dev.business_analytics.AdInsertable_Networks` t4 on t1.channel=t4.channel
inner join `fubotv-dev.business_analytics.station_mapping_table_51220` t5 ON t5.station_name = t1.channel
WHERE t3.tms_id IS NOT NULL
AND UPPER(t3.tms_id) NOT IN ("","00000000000000","TMS-ID-UNAVAILABLE")
AND lower(station_mapping) IN ('disney channel' , 'disney junior')
AND LOWER(playback_type) IN ('vod')
group by 1,2,3,4,5,6,7
order by 1 DESC
)
, viewership_for_other_dis AS
(
Select
t1.user_id,
t2.day as Date_range, t5.station_mapping as Channel_Name,
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
END AS playback_type, 
t1.program_title,
'no' AS ad_insertable_network, SUM(duration)/3600 as Hours, COUNT(DISTINCT(t1.user_id)) AS Uniques, COUNT(distinct(t1.start_time)) as Streams
from `fubotv-prod.data_insights.video_in_progress_via_va_view` t1
inner join users t2 on t1.user_id=t2.user_id and date(t1.start_time)=t2.day
inner join `fubotv-prod.data_insights.tmsid_genre_mapping` t3 on t1.tms_id=t3.tms_id
inner join `fubotv-dev.business_analytics.AdInsertable_Networks` t4 on t1.channel=t4.channel
inner join `fubotv-dev.business_analytics.station_mapping_table_51220` t5 ON t5.station_name = t1.channel
WHERE t3.tms_id IS NOT NULL
AND UPPER(t3.tms_id) NOT IN ("","00000000000000","TMS-ID-UNAVAILABLE")
AND lower(station_mapping) IN ('disney channel' , 'disney junior')
AND LOWER(playback_type) NOT IN ('vod')
group by 1,2,3,4,5,6,7
order by 1 DESC
)

, viewership_deets AS (
SELECT DISTINCT t1.*
FROM viewership_excl_dis t1
UNION ALL
SELECT DISTINCT t2.*
FROM viewership_for_dis_vod t2
UNION ALL
SELECT DISTINCT t3.*
FROM viewership_for_other_dis t3
ORDER BY 1,2
)

SELECT Date_Range, user_type, t2.plan_code, t2. plan_type, ad_insertable_network, Channel_Name, Network_Name, program_title, playback_type,COUNT(DISTINCT t1.user_id) as Users, sum(hours) as Hours
from viewership_deets t1
INNER JOIN users t2 ON t1.user_id=t2.user_id and t1.Date_range=t2.day
WHERE Date_range >= '2020-01-01'
GROUp BY 1,2,3,4,5,6,7,8,9


------------------------------------------ DAU -----------------------------------------------
Select
DATE(start_time) as DAU_date,  COUNT(DISTINCT(t1.user_id)) AS DAU_count,
from `fubotv-prod.data_insights.video_in_progress_via_va_view` t1
WHERE DATE(start_time) >= '2020-01-01'
group by 1
order by 1 DESC

----------------------------------------- SUBS -----------------------------------------------

/* NN -- Sub query */

SELECT 
t1.day AS Sub_Date,
t1.plan_code,
plan_type,
final_status_restated,
count(DISTINCT(t1.account_code)) AS Sub_Count
FROM `fubotv-prod.data_insights.daily_status_static_update` t1
INNER JOIN `fubotv-prod.business_analytics.NN_plancode_to_plantype_v2` t2 on t1.plan_code = t2.plan_code
WHERE day >= '2020-01-01' -- adjust according to the dates looking for
AND LOWER(final_status_restated) IN ('paid', 'paid and scheduled pause','paid but canceled', 'in trial') 
GROUP BY 1,2,3,4
ORDER BY 1 DESC
