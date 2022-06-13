WITH core_test_group_list AS
(
    SELECT DISTINCT fubo_account_code as test_group_core_user_id
    FROM `fubotv-prod.business_analytics.NN_NBA Test Group List_FW
    WHERE LOWER(viewer_type) = 'core'
)
, v4_logs AS 
(
    SELECT DISTINCT t2.user_id, event_time, event_type, event_name, placement_id
    FROM `fubotv-prod.dmp.de_sync_freewheel_logs` t1
    INNER JOIN `fubotv-prod.FW_Views.FW_Logs_View` t2 ON t1.transaction_id = t2.transaction_id
    WHERE user_id IS NOT NULL
    AND LOWER(event_name) = 'defaultimpression'
    AND PARSE_TIMESTAMP('%Y%m%d%H%M%S',event_time) >= '2021-05-22'
    GROUP BY 1,2,3,4,5
)

, Campaign_Info AS
(
    SELECT DISTINCT t2.placement_id_mrm as placement_id, t2.campaign_id_mrm as campaign_id, t2.campaign_name as campaign_name, t2.io_id_mrm, t2.io_name
    FROM `fubotv-prod.dmp.de_sync_freewheel_reports_custom_audit_report_v2` as t2 
)

-- understand the impressions served for NBA CORE test group that ran between 05/20 - 05/23 
,Impressions_Served AS (
SELECT DISTINCT t1.user_id, DATE(PARSE_TIMESTAMP('%Y%m%d%H%M%S',event_time)) as event_date,campaign_name , io_name, count(*) as impressions
FROM v4_logs t1
JOIN Campaign_Info AS t2 ON t2.placement_id  = t1.placement_id
WHERE campaign_id = '53987435'
GROUP BY 1,2,3,4
)

-- 1. all Core Test group Impression Stats
, core_test_group_stats AS (
SELECT t1.test_group_core_user_id , t2.event_date, t2.campaign_name , t2.io_name, t2.impressions 
FROM core_test_group_list  t1
JOIN Impressions_Served t2 ON t1.test_group_core_user_id  = t2.user_id
ORDER BY impressions DESC
)

,core_test_no_imp AS (
SELECT t1.test_group_core_user_id as control_group_core_user_id 
FROM  core_test_group_list t1
LEFT JOIN Impressions_Served t2 ON t1.test_group_core_user_id  = t2.user_id
WHERE t2.user_id IS NULL 
)

--2. core control group User List
,core_control_group_list AS (
SELECT DISTINCT t1.* 
FROM core_test_no_imp t1
UNION ALL 
SELECT DISTINCT t2.fubo_account_code as control_group_core_user_id
FROM `fubotv-prod.business_analytics.NN_NBA Control Group List t2
WHERE LOWER(t2.viewer_type) = 'core'
)

--3. Viewership data for both exposed and non-exposed users

,viewership_test as (
Select DATE(t1.start_time) as streaming_day, t1.user_id, t1.episode_title, station_mapping, SUm(duration/3600) as hours, "Exposed" AS Group_Type 
from `fubotv-prod.data_insights.video_in_progress_via_va_view` t1
inner join core_test_group_stats   t2 on t1.user_id= t2.test_group_core_user_id  -- tables for test
inner join `fubotv-prod.data_insights.tmsid_genre_mapping` t3 on t1.tms_id=t3.tms_id   --Genre Mapping Table
--inner join `fubotv-dev.business_analytics.AdInsertable_Networks` t4 on t1.channel=t4.channel -- Ad Insertable Table
inner join `fubotv-dev.business_analytics.station_mapping_table_51220` t5 on t1.channel=t5.station_name -- Station Mapping Table
WHERE t3.tms_id IS NOT NULL
AND UPPER(t3.tms_id) NOT IN ("","00000000000000","TMS-ID-UNAVAILABLE")
and primary_genre = 'Basketball' 
and (t1.league_names like 'NBA Basketball' OR t1.program_title IN ('NBA Basketball'))
and lower(playback_type) = 'live'
and lower(playback_type) != 'live-preview'
and t3.tms_id NOT LIKE ('%SH%')
and date(start_time) >= '2021-05-22' -- adjust date
group by 1,2,3,4,6
)

,viewership_control as (
Select DATE(t1.start_time) as streaming_day, t1.user_id, t1.episode_title, station_mapping, SUm(duration/3600) as hours, "Non Exposed" AS Group_Type
from `fubotv-prod.data_insights.video_in_progress_via_va_view` t1
inner join core_control_group_list   t2 on t1.user_id= t2.control_group_core_user_id -- table for control
inner join `fubotv-prod.data_insights.tmsid_genre_mapping` t3 on t1.tms_id=t3.tms_id   --Genre Mapping Table
--inner join `fubotv-dev.business_analytics.AdInsertable_Networks` t4 on t1.channel=t4.channel -- Ad Insertable Table
inner join `fubotv-dev.business_analytics.station_mapping_table_51220` t5 on t1.channel=t5.station_name -- Station Mapping Table
WHERE t3.tms_id IS NOT NULL
AND UPPER(t3.tms_id) NOT IN ("","00000000000000","TMS-ID-UNAVAILABLE")
and primary_genre = 'Basketball' 
and (t1.league_names like 'NBA Basketball' OR t1.program_title IN ('NBA Basketball'))
and lower(playback_type) = 'live'
and lower(playback_type) != 'live-preview'
and t3.tms_id NOT LIKE ('%SH%')
and date(start_time) >= '2021-05-22' -- adjust date
group by 1,2,3,4,6
)

,viewership_combined AS (
SELECT DISTINCT t1.*
FROM viewership_test t1
UNION ALL 
SELECT DISTINCT t2.*
FROM viewership_control t2
)

select  user_id, streaming_day, t1.episode_title, station_mapping
,sum(hours) as hours , Group_Type 
from viewership_combined t1
group by 1,2,3,4,6
order by 6 desc


--- EST Impressions

WITH core_test_group_list AS
(
    SELECT DISTINCT fubo_account_code as test_group_core_user_id
    FROM `fubotv-prod.business_analytics.NN_NBA Test Group List_FW
    WHERE LOWER(viewer_type) = 'core'
)
, v4_logs AS 
(
    SELECT DISTINCT t2.user_id, event_time, event_type, event_name, placement_id
    FROM `fubotv-prod.dmp.de_sync_freewheel_logs` t1
    INNER JOIN `fubotv-prod.FW_Views.FW_Logs_View` t2 ON t1.transaction_id = t2.transaction_id
    WHERE user_id IS NOT NULL
    AND LOWER(event_name) = 'defaultimpression'
    AND DATE(PARSE_TIMESTAMP('%Y%m%d%H%M%S',event_time),'EST') >= '2021-05-19'
    GROUP BY 1,2,3,4,5
)

, Campaign_Info AS
(
    SELECT DISTINCT t2.placement_id_mrm as placement_id, t2.campaign_id_mrm as campaign_id, t2.campaign_name as campaign_name, t2.io_id_mrm, t2.io_name
    FROM `fubotv-prod.dmp.de_sync_freewheel_reports_custom_audit_report_v2` as t2 
)

-- understand the impressions served for NBA CORE test group that ran between 05/20 - 05/23 
,Impressions_Served AS (
SELECT DISTINCT t1.user_id, DATE(PARSE_TIMESTAMP('%Y%m%d%H%M%S',event_time),'EST') as event_date,campaign_name , io_name, count(*) as impressions
FROM v4_logs t1
JOIN Campaign_Info AS t2 ON t2.placement_id  = t1.placement_id
WHERE campaign_id = '53987435'
GROUP BY 1,2,3,4
)

-- 1. all Core Test group Impression Stats
, core_test_group_stats AS (
SELECT t1.test_group_core_user_id , t2.event_date, t2.campaign_name , t2.io_name, t2.impressions 
FROM core_test_group_list  t1
JOIN Impressions_Served t2 ON t1.test_group_core_user_id  = t2.user_id
ORDER BY impressions DESC
)

,core_test_no_imp AS (
SELECT t1.test_group_core_user_id as control_group_core_user_id 
FROM  core_test_group_list t1
LEFT JOIN Impressions_Served t2 ON t1.test_group_core_user_id  = t2.user_id
WHERE t2.user_id IS NULL 
)

--2. core control group User List
,core_control_group_list AS (
SELECT DISTINCT t1.* 
FROM core_test_no_imp t1
UNION ALL 
SELECT DISTINCT t2.fubo_account_code as control_group_core_user_id
FROM `fubotv-prod.business_analytics.NN_NBA Control Group List t2
WHERE LOWER(t2.viewer_type) = 'core'
)


--- Viewership EST

Select DISTINCT DATE(t1.start_time) as streaming_day, t1.user_id, t1.episode_title, SUm(duration/3600) as hours
from `fubotv-prod.data_insights.video_in_progress_via_va_view` t1
inner join `fubotv-prod.data_insights.tmsid_genre_mapping` t3 on t1.tms_id=t3.tms_id   --Genre Mapping Table
--inner join `fubotv-dev.business_analytics.AdInsertable_Networks` t4 on t1.channel=t4.channel -- Ad Insertable Table
inner join `fubotv-dev.business_analytics.station_mapping_table_51220` t5 on t1.channel=t5.station_name -- Station Mapping Table
WHERE t3.tms_id IS NOT NULL
AND UPPER(t3.tms_id) NOT IN ("","00000000000000","TMS-ID-UNAVAILABLE")
and primary_genre = 'Basketball' 
and (t1.league_names like 'NBA Basketball' OR t1.program_title IN ('NBA Basketball'))
and lower(playback_type) = 'live'
and lower(playback_type) != 'live-preview'
and t3.tms_id NOT LIKE ('%SH%')
and date(start_time,'EST') >= '2021-05-22' -- adjust date
and date(start_time,'EST') <= '2021-05-23'
AND station_mapping IN ('ABC','ESPN')
--AND user_id = '5f583116990d57000197e521'
group by 1,2,3


-- Flight 2 - Core - 05/27 - 05/30


WITH core_test_group_list AS
(
    SELECT DISTINCT fubo_account_code as test_group_core_user_id
    FROM `fubotv-prod.business_analytics.NN_NBA Test Group List_FW
    WHERE LOWER(viewer_type) = 'core'
)
, v4_logs AS 
(
    SELECT DISTINCT t2.user_id, event_time, event_type, event_name, placement_id
    FROM `fubotv-prod.dmp.de_sync_freewheel_logs` t1
    INNER JOIN `fubotv-prod.FW_Views.FW_Logs_View` t2 ON t1.transaction_id = t2.transaction_id
    WHERE user_id IS NOT NULL
    AND LOWER(event_name) = 'defaultimpression'
    AND DATE(PARSE_TIMESTAMP('%Y%m%d%H%M%S',event_time),'EST') >= '2021-05-20'
    GROUP BY 1,2,3,4,5
)

, Campaign_Info AS
(
    SELECT DISTINCT t2.placement_id_mrm as placement_id, t2.campaign_id_mrm as campaign_id, t2.campaign_name as campaign_name, t2.io_id_mrm, t2.io_name
    FROM `fubotv-prod.dmp.de_sync_freewheel_reports_custom_audit_report_v2` as t2 
)

-- understand the impressions served for NBA CORE test group for TWO FLIGHTS
,Impressions_Served AS (
SELECT DISTINCT t1.user_id, DATE(PARSE_TIMESTAMP('%Y%m%d%H%M%S',event_time),'EST') as event_date,campaign_name , io_name, count(*) as impressions
FROM v4_logs t1
JOIN Campaign_Info AS t2 ON t2.placement_id  = t1.placement_id
WHERE (campaign_id = '54164358' OR campaign_id = '53987435' )
GROUP BY 1,2,3,4
)

-- 1. all Core Test group Impression Stats
, OLD_core_test_group_stats AS (

SELECT t1.test_group_core_user_id , t2.event_date, t2.campaign_name , t2.io_name, t2.impressions 
FROM core_test_group_list  t1
JOIN Impressions_Served t2 ON t1.test_group_core_user_id  = t2.user_id
WHERE LOWER(campaign_name) LIKE  '%one%'
AND event_date >= '2021-05-20'
AND event_date  <= '2021-05-23'
ORDER BY impressions DESC
)

, NEW_core_test_group_stats AS (
SELECT t1.test_group_core_user_id , t2.event_date, t2.campaign_name , t2.io_name, t2.impressions 
FROM core_test_group_list  t1
JOIN Impressions_Served t2 ON t1.test_group_core_user_id  = t2.user_id
WHERE LOWER(campaign_name) LIKE  '%two%'
AND event_date >= '2021-05-27'
AND event_date  <= '2021-05-30'
--AND test_group_core_user_id = '600320655815940001c8f86a'
ORDER BY impressions DESC
)

SELECT *
FROM NEW_core_test_group_stats t1

-- Targeted in both flights 
SELECT DISTINCT t1.test_group_core_user_id 
FROM NEW_core_test_group_stats t1
JOIN OLD_core_test_group_stats t2 on t1.test_group_core_user_id = t2.test_group_core_user_id 


-- viewership Stats

Select DISTINCT DATE(t1.start_time) as streaming_day, station_mapping, t1.league_name, t1.program_title, t1.episode_title, prim SUm(duration/3600) as hours, COUNT(DISTINCT user_id) as uniques
from `fubotv-prod.data_insights.video_in_progress_via_va_view` t1
inner join `fubotv-prod.data_insights.tmsid_genre_mapping` t3 on t1.tms_id=t3.tms_id   --Genre Mapping Table
inner join `fubotv-dev.business_analytics.AdInsertable_Networks` t4 on t1.channel=t4.channel -- Ad Insertable Table
inner join `fubotv-dev.business_analytics.station_mapping_table_51220` t5 on t1.channel=t5.station_name -- Station Mapping Table
WHERE t3.tms_id IS NOT NULL
AND UPPER(t3.tms_id) NOT IN ("","00000000000000","TMS-ID-UNAVAILABLE")
and primary_genre = 'Basketball' 
and (t1.league_names like 'NBA Basketball' OR t1.program_title IN ('NBA Basketball'))
and lower(playback_type) = 'live'
and lower(playback_type) != 'live-preview'
and t3.tms_id NOT LIKE ('%SH%')
and date(start_time,'EST') = '2021-05-30' -- adjust date
--and date(start_time,'EST') <= '2021-05-23'
--AND station_mapping IN ('ABC','ESPN')
--AND user_id = '5f583116990d57000197e521'
group by 1,2,3,4


-- Testing specific user with Max impressions - 600320655815940001c8f86a
WITH core_test_group_list AS
(
    SELECT DISTINCT fubo_account_code as test_group_core_user_id
    FROM `fubotv-prod.business_analytics.NN_NBA Test Group List_FW
    WHERE LOWER(viewer_type) = 'core'
)
, v4_logs AS 
(
    SELECT DISTINCT t2.user_id, event_time, event_type, event_name, placement_id, t2.station_id
    FROM `fubotv-prod.dmp.de_sync_freewheel_logs` t1
    INNER JOIN `fubotv-prod.FW_Views.FW_Logs_View` t2 ON t1.transaction_id = t2.transaction_id
    WHERE user_id IS NOT NULL
    AND LOWER(event_name) = 'defaultimpression'
    AND DATE(PARSE_TIMESTAMP('%Y%m%d%H%M%S',event_time),'EST') >= '2021-05-20'
    GROUP BY 1,2,3,4,5,6
)

, Campaign_Info AS
(
    SELECT DISTINCT t2.placement_id_mrm as placement_id, t2.campaign_id_mrm as campaign_id, t2.campaign_name as campaign_name, t2.io_id_mrm, t2.io_name
    FROM `fubotv-prod.dmp.de_sync_freewheel_reports_custom_audit_report_v2` as t2 
)

-- understand the impressions served for NBA CORE test group for TWO FLIGHTS
,Impressions_Served AS (
SELECT DISTINCT t1.user_id, DATETIME(PARSE_TIMESTAMP('%Y%m%d%H%M%S',event_time),'EST') as event_date,campaign_name , io_name, station_id, count(*) as impressions
FROM v4_logs t1
JOIN Campaign_Info AS t2 ON t2.placement_id  = t1.placement_id
WHERE (campaign_id = '54164358' OR campaign_id = '53987435' )
GROUP BY 1,2,3,4,5
)

-- 1. all Core Test group Impression Stats
, OLD_core_test_group_stats AS (

SELECT t1.test_group_core_user_id , t2.event_date, t2.campaign_name , t2.io_name, station_id, t2.impressions 
FROM core_test_group_list  t1
JOIN Impressions_Served t2 ON t1.test_group_core_user_id  = t2.user_id
WHERE LOWER(campaign_name) LIKE  '%one%'
AND event_date >= '2021-05-20'
AND event_date  <= '2021-05-23'
ORDER BY impressions DESC
)

, NEW_core_test_group_stats AS (
SELECT t1.test_group_core_user_id , t2.event_date, t2.campaign_name , t2.io_name, station_id, t2.impressions 
FROM core_test_group_list  t1
JOIN Impressions_Served t2 ON t1.test_group_core_user_id  = t2.user_id
WHERE LOWER(campaign_name) LIKE  '%two%'
AND event_date >= '2021-05-27'
--AND event_date  <= '2021-05-30'
AND test_group_core_user_id = '600320655815940001c8f86a'
ORDER BY impressions DESC
)

SELECT *
FROM NEW_core_test_group_stats t1
ORDER BY event_date DESC

--viewership for the same user 600320655815940001c8f86a

Select DISTINCT DATETIME(t1.start_time,'EST') as stream_time, t1.user_id, t1.series_title,t1.program_title, t1.episode_title, channel, station_mapping, SUm(duration/3600) as hours
from `fubotv-prod.data_insights.video_in_progress_via_va_view` t1
inner join `fubotv-prod.data_insights.tmsid_genre_mapping` t3 on t1.tms_id=t3.tms_id   --Genre Mapping Table
inner join `fubotv-dev.business_analytics.station_mapping_table_51220` t5 on t1.channel=t5.station_name -- Station Mapping Table
WHERE t3.tms_id IS NOT NULL
AND UPPER(t3.tms_id) NOT IN ("","00000000000000","TMS-ID-UNAVAILABLE")
AND user_id = '600320655815940001c8f86a'
group by 1,2,3,4,5,6,7
order by 1 desc

----------------------------------------------- CASUAL USER GROUP TESTS --------------------------------------------------

WITH casual_test_group_list AS
(
    SELECT DISTINCT fubo_account_code as test_group_casual_user_id
    FROM `fubotv-prod.business_analytics.NN_NBA Test Group List_FW
    WHERE LOWER(viewer_type) = 'casual'
)
, v4_logs AS 
(
    SELECT DISTINCT t2.user_id, event_time, event_type, event_name, placement_id
    FROM `fubotv-prod.dmp.de_sync_freewheel_logs` t1
    INNER JOIN `fubotv-prod.FW_Views.FW_Logs_View_EST` t2 ON t1.transaction_id = t2.transaction_id
    WHERE user_id IS NOT NULL
    AND LOWER(event_name) = 'defaultimpression'
    AND DATE(PARSE_TIMESTAMP('%Y%m%d%H%M%S',event_time),'EST') >= '2021-06-01'
    GROUP BY 1,2,3,4,5
)

, Campaign_Info AS
(
    SELECT DISTINCT t2.placement_id_mrm as placement_id, t2.campaign_id_mrm as campaign_id, t2.campaign_name as campaign_name, t2.io_id_mrm, t2.io_name
    FROM `fubotv-prod.dmp.de_sync_freewheel_reports_custom_audit_report_v2` as t2 
)

-- understand the impressions served for NBA CASUAL test group for Flight #3,4,5,6,7
,Impressions_Served AS (
SELECT DISTINCT t1.user_id, DATE(PARSE_TIMESTAMP('%Y%m%d%H%M%S',event_time),'EST') as event_date,campaign_name , io_name, count(*) as impressions
FROM v4_logs t1
JOIN Campaign_Info AS t2 ON t2.placement_id  = t1.placement_id
WHERE (campaign_id = '55113541' )
GROUP BY 1,2,3,4
)

-- 1. all Casual Test group Impression Stats

, casual_test_group_stats AS (
SELECT t1.test_group_casual_user_id , t2.event_date, t2.campaign_name , t2.io_name, t2.impressions 
FROM casual_test_group_list  t1
JOIN Impressions_Served t2 ON t1.test_group_casual_user_id  = t2.user_id
AND event_date >= '2021-07-05'
AND event_date  <= '2021-07-08'
ORDER BY impressions DESC
)

SELECT DISTINCT *
FROM casual_test_group_stats t1

-------------------------------------------------------- 08/17 

WITH casual_test_group_list AS
(
    SELECT DISTINCT fubo_account_code as test_group_casual_user_id 
    FROM `fubotv-prod.business_analytics.NN_NBA Test Group List_FW`
    WHERE LOWER(viewer_type) = 'casual'
)
, v4_logs AS 
(
    SELECT DISTINCT t2.user_id, event_time, event_type, event_name, placement_id
    FROM `fubotv-prod.dmp.de_sync_freewheel_logs` t1
    INNER JOIN `fubotv-prod.FW_Views.FW_Logs_View_EST` t2 ON t1.transaction_id = t2.transaction_id
    WHERE user_id IS NOT NULL
    AND LOWER(event_name) = 'defaultimpression'
    AND DATE(PARSE_TIMESTAMP('%Y%m%d%H%M%S',event_time),'EST') >= '2021-06-01'
    GROUP BY 1,2,3,4,5
)

, Campaign_Info AS
(
    SELECT DISTINCT t2.placement_id_mrm as placement_id, t2.campaign_id_mrm as campaign_id, t2.campaign_name as campaign_name, t2.io_id_mrm, t2.io_name
    FROM `fubotv-prod.dmp.de_sync_freewheel_reports_custom_audit_report_v2` as t2 
)

-- understand the impressions served for NBA CASUAL test group for Flight #3,4,5,6,7
,Impressions_Served AS (
SELECT DISTINCT t1.user_id, DATE(PARSE_TIMESTAMP('%Y%m%d%H%M%S',event_time),'EST') as event_date,campaign_name , io_name, count(*) as impressions
FROM v4_logs t1
JOIN Campaign_Info AS t2 ON t2.placement_id  = t1.placement_id
WHERE (campaign_id = '54887327' )
GROUP BY 1,2,3,4
)

-- 1. all Casual Test group Impression Stats

, casual_test_group_stats AS (
SELECT t1.test_group_casual_user_id , t2.event_date, t2.campaign_name , t2.io_name, t2.impressions 
FROM casual_test_group_list  t1
JOIN Impressions_Served t2 ON t1.test_group_casual_user_id  = t2.user_id
AND event_date >= '2021-06-26'
AND event_date  <= '2021-06-29'
ORDER BY impressions DESC
)

SELECT DISTINCT *
FROM casual_test_group_stats t1