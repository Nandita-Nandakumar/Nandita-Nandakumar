# View Name: NN_Dashboard_FSN_Pacing_On_Off_YT_Stations_Impressions

/* NN - Station Impressions + FSN Pacing Impressions + FSN YT Impressions */


WITH all_excl_fsn AS 
(
SELECT DISTINCT Event_Date, 
"On Platform" AS Platform_Type, 
"fuboTV" AS Platform, 
Station_Name_Mapped, 
Network_Group_Mapped, 
0 AS CPM,
(SUM(Net_Counted_Ads)/1000) AS Revenue, 
SUM(Net_Counted_Ads) as Net_Counted_Ads
FROM 
(
SELECT DISTINCT Date(Event_date) as Event_Date, station_mapping AS  Station_Name_Mapped, network_owner AS Network_Group_Mapped,  Net_Counted_Ads
FROM `fubotv-dev.business_analytics.FW_MRM_Analytics_Station_Data` t1
INNER JOIN `fubotv-dev.business_analytics.FW_MRM_Analytics_Station_Mapping_Data_New` t2 on t1.Video_Group_Name = t2.Video_Group_Name
INNER JOIN `fubotv-dev.business_analytics.station_mapping_table_51220` t3 ON t2.Channel_Name = t3.station_name
WHERE station_mapping IS NOT NULL
ORDER BY 1
)
WHERE LOWER(Station_Name_Mapped) NOT IN ('fubo sports network')
GROUP BY 1,2,3,4,5,6
ORDER BY 1
)

, fsn AS
(
SELECT DISTINCT
Event_Date,
Platform_Type,
Platform,
Station_Name AS Station_Name_Mapped,
'fubo' AS Network_Group_Mapped,
CPM,
Revenue,
Net_Counted_Ads
FROM `fubotv-dev.business_analytics.NN_fsn_pacing_impressions`
)

,yt AS 
(
SELECT DISTINCT
Event_Date,
"Off Platform" AS Platform_Type,
"youtube" AS Platform,
"fubo Sports Network" AS Station_Name_Mapped,
"fubo" AS Network_Group_Mapped,
CPM,
0 AS Revenue, 
Net_Counted_Ads
FROM `fubotv-dev.business_analytics.NN_fsn_pacing_yt_impressions`   
)

, fsn_only AS (
SELECT 
t1.Event_Date,
t1.Platform_Type,
t1.Platform,
t1.Station_Name_Mapped,
t1.Network_Group_Mapped,
t1.CPM,
t1.Revenue,
CASE WHEN t1.Platform = 'youtube' AND t1.Net_Counted_Ads IS NULL AND t1.Event_Date = t2.Event_Date AND t1.Platform = t2.Platform THEN t2.Net_Counted_Ads
ELSE t1.Net_Counted_Ads END AS Net_Counted_Adss
FROM fsn t1
LEFT JOIN yt t2 ON t1.Event_Date = t2.Event_Date AND t2.Platform_Type = t1.Platform_Type AND t1.Platform = t2.Platform
)

,all_data_final AS (
SELECT DISTINCT t1.*
FROM all_excl_fsn t1
UNION ALL 
SELECT DISTINCT t2.*
FROM fsn_only t2
)

SELECT DISTINCT t1.*, Corp, Explanation
FROM all_data_final t1
JOIN `fubotv-dev.business_analytics.NN_Station_DAI_Explanation` t2 ---Explanation for DAI Eligible Stations
 ON t1.Station_Name_Mapped = t2.Station
WHERE DATE_TRUNC(Event_Date, month) <= DATE_TRUNC(current_date-1,month)

----------------------------------------------------------------------------------------------------------------------------------------------------------------------------

# View Name: NN_Dashboard_FSN_Pacing_On_Off_YT_Viewership

/* NN - Viewership for ALL + Including FSN On and Off Platforms + YT */

/* NN - Viewership for ALL + Including FSN On and Off Platforms*/

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
fast_channel, 
LOWER(t4.ad_insertable) AS ad_insertable_network, SUM(duration)/3600 as Hours, COUNT(DISTINCT(t1.user_id)) AS Uniques, COUNT(distinct(t1.start_time)) as Streams
from `fubotv-prod.data_insights.video_in_progress_via_va_view` t1
inner join users t2 on t1.user_id=t2.user_id and date(t1.start_time)=t2.day
inner join `fubotv-prod.data_insights.tmsid_genre_mapping` t3 on t1.tms_id=t3.tms_id
inner join `fubotv-dev.business_analytics.AdInsertable_Networks` t4 on t1.channel=t4.channel
inner join `fubotv-dev.business_analytics.station_mapping_table_51220` t5 ON t5.station_name = t1.channel
WHERE t3.tms_id IS NOT NULL
AND UPPER(t3.tms_id) NOT IN ("","00000000000000","TMS-ID-UNAVAILABLE")
AND lower(station_mapping) NOT IN ('disney channel' , 'disney junior')
group by 1,2,3,4,5,6
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
fast_channel,
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
group by 1,2,3,4,5,6
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
fast_channel,
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
group by 1,2,3,4,5,6
order by 1 DESC
)

,viewership AS (
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
, all_data AS (
SELECT Date_range, 
"fubotv" as platform_partner, 
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
CASE WHEN LOWER(Network_Name) LIKE '%gusto%' THEN "Gusto"
     ELSE Network_Name
END AS Network_Name, 
playback_type,
CASE WHEN fast_channel = true THEN "yes" ElSE "no" END AS fast_channel_group,
ad_insertable_network, 
SUM(Hours) AS Hours,
Uniques,
Streams,
"On Platform" AS on_off_plat
FROM viewership 
WHERE Date_range >= '2020-01-01'
AND LOWER(Channel_N) NOT IN ('fubo sports network')
GROUP BY 1,2,3,4,5,6,7,9,10,11
)

--------------------------------------- FSN DATA --------------------------------------------------

, fsn_prelim_data as (

-- xumo, samsung, roku,.com
select t1.platform_partner, 'live' as playback_type, date(t1.start_time, 'America/New_York') as airdate,  SUM(t1.num_uniques) as tot_streams, null as num_uniques, SUM(t1.view_time_sec) as view_time_sec,
from `fubotv-prod.Sportsnet.viewership_detail_by_schedule` t1
inner join `fubotv-prod.data_insights.programming_schedule_fsn_only` t2 
on t1.asset_id = t2.asset_id
where date(t1.start_time) NOT IN ('2020-10-17','2020-10-19','2020-10-23','2020-10-24','2020-10-26','2020-11-19','2020-11-20','2020-11-25','2020-11-28','2020-12-03','2020-12-06','2020-12-09','2020-12-10','2020-12-12',
'2020-12-13','2020-12-14','2020-12-15','2020-12-16','2020-12-28','2020-12-29')
GROUP BY 1,2,3

union all

-- xumo, samsung, roku,.com
select t1.platform_partner,  'live' as playback_type, date(t1.start_time, 'America/New_York') as airdate,  SUM(t1.num_uniques) as tot_streams, null as num_uniques, SUM(t1.view_time_sec) as view_time_sec,
from `fubotv-prod.Sportsnet.viewership_detail_by_schedule` t1
inner join `fubotv-prod.data_insights.programming_schedule` t2 
on t1.asset_id = t2.asset_id
where date(t1.start_time) IN ('2020-10-17','2020-10-19','2020-10-23','2020-10-24','2020-10-26','2020-11-19','2020-11-20','2020-11-25','2020-11-28','2020-12-03','2020-12-06','2020-12-09','2020-12-10','2020-12-12',
'2020-12-13','2020-12-14','2020-12-15','2020-12-16','2020-12-28','2020-12-29')
GROUP BY 1,2,3

union all

-- fuboTV
select 'fuboTV' as platform_partner, playback_type, date(t3.start_time, 'America/New_York') as airdate, count(t1.start_time) as tot_streams,
count(distinct user_id) as num_uniques, sum(t1.duration) as view_time_sec
from `fubotv-prod.data_insights.video_in_progress_via_va_view` t1
inner join `fubotv-prod.data_insights.programming_schedule_fsn_only` t2 on t1.asset_id = t2.asset_id
inner join `fubotv-prod.Sportsnet.viewership_detail_by_schedule` t3 
on t1.asset_id = t3.asset_id
where (lower(t1.channel) like '%fubotv%'
or lower(t1.channel) like '%fubo sports%')
and playback_type != 'live_preview'
and lower(t3.platform_partner) = 'samsung' /*need to restrict it to just one platform from viewership detail table to avoid duplication*/
group by 1,2,3

union all

-- fuboTV (VOD)
select 'fuboTV' as platform_partner, playback_type, date(t1.start_time, 'America/New_York') as airdate,count(t1.start_time) as tot_streams,
count(distinct user_id) as num_uniques, sum(t1.duration) as view_time_sec
from `fubotv-prod.data_insights.video_in_progress_via_va_view` t1
where t1.channel = 'fubo Sports Network'
and lower(playback_type) = 'vod'
group by 1,2,3
)

, fsn_program as 
(
select platform_partner, airdate,  playback_type,sum(num_uniques) as total_uniques, sum(tot_streams) as total_streams,
sum(view_time_sec)/60 as minutes, sum(view_time_sec)/3600 as hours,'fubo Sports Network' as Channel_Name, 'fubo Sports Network' as Network_Name, 'Yes' as ad_insertable_network ,case when platform_partner = 'fuboTV' then 'On Platform' else 'Off Platform' end as on_off_plat 
from fsn_prelim_data
where  airdate < current_date()
group by 1,2,3
)

, fsn_data AS
(
Select airdate as Date_range, platform_partner,Channel_Name, Network_Name, playback_type, 'no' AS fast_channel_group, LOWER(ad_insertable_network) as ad_insertable_network ,

case when airdate < '2020-05-01' and platform_partner = 'fsn' then NULL else Hours end as Hours,  
total_uniques as Uniques,
case when airdate < '2020-05-01' and platform_partner = 'fsn' then NULL else total_streams end as Streams,
on_off_plat
from fsn_program
where airdate >= '2019-09-01'
)

,fsn_yt_data AS
(
SELECT
Date_Range as Date_range, 
Platform_Partner AS platform_partner,
Channel_Name, 
CASE WHEN LOWER(Network_Name) = 'fubotv' THEN 'fubo'
    ELSE Network_Name
    END AS Network_Name, 
Playback_Type AS playback_type, 
'no' AS fast_channel_group,
LOWER(ad_insertable_network) as ad_insertable_network ,
Hours,
Uniques,
Streams,
on_off_plat
FROM `fubotv-dev.business_analytics.NN_fsn_pacing_yt_viewership`
)

, all_data_final AS (
SELECT DISTINCT t1.*
FROM all_data t1
UNION ALL 
SELECT DISTINCT t2.*
FROM fsn_data t2
UNION ALL 
SELECT DISTINCT t3.*
FROM fsn_yt_data t3
)

SELECT DISTINCT t1.*, Corp, Explanation
FROM all_data_final t1
LEFT JOIN `fubotv-dev.business_analytics.NN_Station_DAI_Explanation` t2 ---Explanation for DAI Eligible Stations
 ON t1.Channel_Name = t2.Station
WHERE DATE_TRUNC(Date_range, month) < DATE_TRUNC(current_date,month)