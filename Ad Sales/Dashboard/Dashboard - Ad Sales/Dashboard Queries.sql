/* NN - Station Tables - Gets refreshed to append everyday */

/* NN - Station Tables - Gets refreshed to append everyday */

SELECT DISTINCT Event_Date, Station_Name_Mapped, Network_Group_Mapped, SUM(Net_Counted_Ads) as Net_Counted_Ads 
FROM 
(
SELECT DISTINCT Date(Event_date) as Event_Date, t2.Station_Name_Mapped, t2.Network_Group_Mapped,  Net_Counted_Ads
FROM `fubotv-dev.business_analytics.FW_MRM_Analytics_Station_Data` t1
INNER JOIN `fubotv-dev.business_analytics.FW_MRM_Analytics_Station_Mapping_Data` t2 on t1.Video_Group_Name = t2.Video_Group_Name
WHERE station_name_mapped IS NOT NULL
ORDER BY 1
)
GROUP BY 1,2,3


/* NN -- Sub query */

SELECT t1.day as Sub_Date, count(DISTINCT(t1.account_code)) AS Sub_Count
FROM `fubotv-prod.data_insights.daily_status_static_update` t1
WHERE LOWER(final_status_restated) IN ('paid', 'paid and scheduled pause','paid but canceled')
GROUP BY 1
ORDER BY 1 DESC

/* NN Ad Sales X Viwership Dash 
 Viewership Part of query */


with users as (
Select distinct t1.day,t1.account_code,t2.user_id
from `fubotv-prod.data_insights.daily_status_static_update` t1
inner join `fubotv-dev.business_analytics.account_code_mapping` t2 on t1.account_code=t2.account_code
where final_status_v2_sql not in ('expired','paused','past due from trials and scheduled pause','failed and scheduled pause')and lower(plan_code) not like '%spain%'
)

, viewership_excl_dis AS
(
Select
t2.day as Date_range, t5.station_mapping as Channel_N, 
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
LOWER(t4.ad_insertable) AS ad_insertable_network, SUM(duration)/3600 as Hours, COUNT(DISTINCT(t1.user_id)) AS Uniques, COUNT(distinct(t1.start_time)) as Streams
from `fubotv-prod.data_insights.video_in_progress_via_va_view` t1
inner join users t2 on t1.user_id=t2.user_id and date(t1.start_time)=t2.day
inner join `fubotv-prod.data_insights.tmsid_genre_mapping` t3 on t1.tms_id=t3.tms_id
inner join `fubotv-dev.business_analytics.AdInsertable_Networks` t4 on t1.channel=t4.channel
inner join `fubotv-dev.business_analytics.station_mapping_table_51220` t5 ON t5.station_name = t1.channel
WHERE t3.tms_id IS NOT NULL
AND UPPER(t3.tms_id) NOT IN ("","00000000000000","TMS-ID-UNAVAILABLE")
AND lower(station_mapping) NOT IN ('disney channel' , 'disney junior')
group by 1,2,3,4,5
order by 1 DESC
)

, viewership_for_dis_vod AS
(
Select
t2.day as Date_range, t5.station_mapping as Channel_N,
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
group by 1,2,3,4,5
order by 1 DESC
)
, viewership_for_other_dis AS
(
Select
t2.day as Date_range, t5.station_mapping as Channel_N,
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
group by 1,2,3,4,5
order by 1 DESC
)

,results AS (
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

SELECT Date_range, 
 CASE WHEN LOWER(Channel_N) = 'disney channel' AND ad_insertable_network = 'no' THEN 'Disney Channel (live only)'
     WHEN LOWER(Channel_N) = 'disney junior' AND ad_insertable_network = 'no' THEN 'Disney Junior (live only)'
     WHEN LOWER(Channel_N) = 'bet' AND ad_insertable_network = 'no' THEN 'BET 4K'
     WHEN LOWER(Channel_N) = 'the golf channel' AND ad_insertable_network = 'no' THEN 'The Golf Channel 4K'
     WHEN LOWER(Channel_N) = 'nickelodeon' AND ad_insertable_network = 'no' THEN 'Nickelodeon 4K'
     WHEN LOWER(Channel_N) = 'estrella' AND ad_insertable_network = 'no' THEN 'Estrella 4K'
     WHEN LOWER(Channel_N) = 'telemundo' AND ad_insertable_network = 'no' THEN 'Telemundo (affiliates)'
     WHEN LOWER(Channel_N) = 'unimas' AND ad_insertable_network = 'no' THEN 'Unimas (affiliates)'
     WHEN LOWER(Channel_N) = 'univision' AND ad_insertable_network = 'no' THEN 'Univision (affiliates)'
     ELSE Channel_N
END AS Channel_Name,
Network_Name, 
playback_type,
ad_insertable_network, 
SUM(Hours) AS Hours,
Uniques,
Streams,
FROM results 
WHERE Date_range >= '2020-01-01'
GROUP BY 1,2,3,4,5,7,8