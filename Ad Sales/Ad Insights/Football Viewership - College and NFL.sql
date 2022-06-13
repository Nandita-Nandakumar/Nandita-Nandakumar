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
END AS playback_type,
primary_genre,
t3.league_names,
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
primary_genre,
t3.league_names,
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
primary_genre,
t3.league_names,
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

, all_results AS (
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
primary_genre,
league_names,
ad_insertable_network, 
SUM(Hours) AS Hours,
FROM results 
GROUP BY 1,2,3,4,5,6,7
)

,coll_football_results AS (

SELECT Date_Range,Channel_Name, Network_Name, ad_insertable_network, league_names, SUM(Hours) as Hours
FROM all_results
WHERE LOWER(primary_genre) LIKE '%football%'
AND LOWER(league_names) LIKE '%college football%'
GROUP BY 1,2,3,4,5
)

,nfl_results AS (
SELECT Date_range, Channel_Name, Network_Name, ad_insertable_network, league_names, SUM(Hours) as Hours
FROM all_results
WHERE LOWER(primary_genre) LIKE '%football%'
AND LOWER(league_names) LIKE '%nfl%'
GROUP BY 1,2,3,4,5
)

,combined AS (
SELECT DISTINCT t1.*
FROM coll_football_results t1
UNION ALL
SELECT DISTINCT t2.*
FROM nfl_results t2
)

SELECT * 
FROM combined
WHERE DATE_Range >= '2021-01-01'
AND Date_Range <= '2021-10-10'