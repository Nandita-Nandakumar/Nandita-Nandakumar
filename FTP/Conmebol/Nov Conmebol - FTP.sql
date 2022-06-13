---------------------------------------------- Nov Conmebol 2021 - HOURS -----------------------------------
WITH view_match AS 
(
SELECT
--device_category,
user_id AS userid,
m.tms_id,
MIN(start_time) AS timestamp,
SUM(duration)/ 3600 as hours
FROM `fubotv-prod.dmp.viewership_activities` v
INNER JOIN `fubotv-dev.business_analytics.nov_conmebol_live_tmsid` m ON v.tms_id = m.tms_id
JOIN `fubotv-prod.data_insights.daily_status_static_update` t1 on v.user_id = t1.account_code and t1.day = DATE(v.start_time)
--JOIN `fubotv-prod.business_analytics.NN_plancode_to_plantype_v2` p ON p.plan_code = t1.plan_code
WHERE v.playback_type not like '%dvr%' AND v.playback_type not like '%vod%' AND v.playback_type not like '%lookback%' -- include live & startover
AND lower(v.device_category) IN ('roku','web','iphone','fire_tv','android_tv','smart_tv', 'android_phone','android_tablet', 'ipad')
AND (v.start_time between '2021-11-01' and '2021-11-18'
OR TIMESTAMP_ADD(v.start_time, INTERVAL v.duration SECOND) between '2021-11-01' and '2021-11-18')
GROUP BY 1,2
)

, played_ftp AS (
SELECT distinct
--properties_device_category AS device_category,
timestamp,
userid,
properties_profile_id AS profile_id,
properties_data_event_context_program_id AS tms_id,
properties_data_event_context_ftp_game_question_id AS ftp_question_id
FROM `fubotv-prod.data_engineering.de_archiver_other` t1
JOIN `fubotv-dev.business_analytics.nov_conmebol_live_tmsid` t2 ON t2.tms_id = t1.properties_data_event_context_program_id
WHERE receivedat >= '2021-11-01' and receivedat <= '2021-11-18'
AND lower(properties_device_category) in ('roku','web','iphone','fire_tv','android_tv','smart_tv', 'android_phone','android_tablet', 'ipad')
AND event IN ('ui_interaction')
AND properties_event_sub_category IN ('ftp_game')
AND properties_data_event_context_component = 'ftp_game_question'  
AND userid IS NOT NULL
)

,users_who_played AS
(
SELECT DISTINCT t1.userid,
CASE WHEN t1.userid = t2.userid --and LOWER(t1.device_category) = LOWER(t2.device_category) 
THEN 'yes' ELSE 'no' END AS played_ftp,
FROM view_match t1
left join played_ftp t2 ON t1.userid = t2.userid 
)

,conmebol_viewership AS
(
SELECT DISTINCT
user_id,
played_ftp,
--t2.device_category,
sum(t1.duration)/3600 as Hours,
COUNT (DISTINCT t3.tms_id) as num_matches
FROM `fubotv-prod.data_insights.video_in_progress_via_va_view` t1
INNER JOIN `fubotv-dev.business_analytics.nov_conmebol_live_tmsid` t3 on t1.tms_id=t3.tms_id
INNER JOIN users_who_played t2 ON t1.user_id = t2.userid --AND t1.device_category = t2.device_category
WHERE DATE(start_time,'America/New_York') IN ('2021-11-11','2021-11-12', '2021-11-16')
AND LOWER(t1.device_category) IN ('roku','web','iphone','android_tv', 'android_phone','android_tablet', 'ipad','fire_tv', 'smart_tv')
AND t1.playback_type not like '%dvr%' AND t1.playback_type not like '%vod%' AND t1.playback_type not like '%lookback%'
GROUP BY 1,2--,3--,4
)

SELECT *
FROM conmebol_viewership


---------------------------------------------- Nov Conmebol 2021 - QS COUNT -----------------------------------
WITH view_match AS 
(
SELECT
--device_category,
user_id AS userid,
m.tms_id,
MIN(start_time) AS timestamp,
SUM(duration)/ 3600 as hours
FROM `fubotv-prod.dmp.viewership_activities` v
INNER JOIN `fubotv-dev.business_analytics.nov_conmebol_live_tmsid` m ON v.tms_id = m.tms_id
JOIN `fubotv-prod.data_insights.daily_status_static_update` t1 on v.user_id = t1.account_code and t1.day = DATE(v.start_time)
--JOIN `fubotv-prod.business_analytics.NN_plancode_to_plantype_v2` p ON p.plan_code = t1.plan_code
WHERE v.playback_type not like '%dvr%' AND v.playback_type not like '%vod%' AND v.playback_type not like '%lookback%' -- include live & startover
AND lower(v.device_category) IN ('roku','web','iphone','fire_tv','android_tv','smart_tv', 'android_phone','android_tablet', 'ipad')
AND (v.start_time between '2021-11-01' and '2021-11-18'
OR TIMESTAMP_ADD(v.start_time, INTERVAL v.duration SECOND) between '2021-11-01' and '2021-11-18')
GROUP BY 1,2
)

, played_ftp AS (
SELECT distinct
--properties_device_category AS device_category,
timestamp,
userid,
properties_profile_id AS profile_id,
properties_data_event_context_program_id AS tms_id,
properties_data_event_context_ftp_game_question_id AS ftp_question_id
FROM `fubotv-prod.data_engineering.de_archiver_other` t1
JOIN `fubotv-dev.business_analytics.nov_conmebol_live_tmsid` t2 ON t2.tms_id = t1.properties_data_event_context_program_id
WHERE receivedat >= '2021-11-01' and receivedat <= '2021-11-18'
AND lower(properties_device_category) in ('roku','web','iphone','fire_tv','android_tv','smart_tv', 'android_phone','android_tablet', 'ipad')
AND event IN ('ui_interaction')
AND properties_event_sub_category IN ('ftp_game')
AND properties_data_event_context_component = 'ftp_game_question'  
AND userid IS NOT NULL
)

,users_who_played AS
(
SELECT DISTINCT t1.userid,
CASE WHEN t1.userid = t2.userid --and LOWER(t1.device_category) = LOWER(t2.device_category) 
THEN 'yes' ELSE 'no' END AS played_ftp,
COUNT (DISTINCT ftp_question_id) AS question_count
FROM view_match t1
left join played_ftp t2 ON t1.userid = t2.userid 
GROUP BY 1,2
)

,conmebol_viewership AS
(
SELECT DISTINCT
user_id,
played_ftp,
question_count,
--t2.device_category,
sum(t1.duration)/3600 as Hours,
COUNT (DISTINCT t3.tms_id) as num_matches
FROM `fubotv-prod.data_insights.video_in_progress_via_va_view` t1
INNER JOIN `fubotv-dev.business_analytics.nov_conmebol_live_tmsid` t3 on t1.tms_id=t3.tms_id
INNER JOIN users_who_played t2 ON t1.user_id = t2.userid --AND t1.device_category = t2.device_category
WHERE DATE(start_time,'America/New_York') IN ('2021-11-11','2021-11-12', '2021-11-16')
AND LOWER(t1.device_category) IN ('roku','web','iphone','android_tv', 'android_phone','android_tablet', 'ipad','fire_tv', 'smart_tv')
AND t1.playback_type not like '%dvr%' AND t1.playback_type not like '%vod%' AND t1.playback_type not like '%lookback%'
GROUP BY 1,2,3--,4
)

SELECT *
FROM conmebol_viewership 

---------------------------------------------- Nov Conmebol 2021 - DEVICE -----------------------------------
WITH view_match AS 
(
SELECT
--device_category,
user_id AS userid,
m.tms_id,
MIN(start_time) AS timestamp,
SUM(duration)/ 3600 as hours
FROM `fubotv-prod.dmp.viewership_activities` v
INNER JOIN `fubotv-dev.business_analytics.nov_conmebol_live_tmsid` m ON v.tms_id = m.tms_id
JOIN `fubotv-prod.data_insights.daily_status_static_update` t1 on v.user_id = t1.account_code and t1.day = DATE(v.start_time)
--JOIN `fubotv-prod.business_analytics.NN_plancode_to_plantype_v2` p ON p.plan_code = t1.plan_code
WHERE v.playback_type not like '%dvr%' AND v.playback_type not like '%vod%' AND v.playback_type not like '%lookback%' -- include live & startover
AND lower(v.device_category) IN ('roku','web','iphone','fire_tv','android_tv','smart_tv', 'android_phone','android_tablet', 'ipad')
AND (v.start_time between '2021-11-01' and '2021-11-18'
OR TIMESTAMP_ADD(v.start_time, INTERVAL v.duration SECOND) between '2021-11-01' and '2021-11-18')
GROUP BY 1,2--,3
)

, played_ftp AS (
SELECT distinct
properties_device_category AS device_category,
timestamp,
userid,
properties_profile_id AS profile_id,
properties_data_event_context_program_id AS tms_id,
properties_data_event_context_ftp_game_question_id AS ftp_question_id
FROM `fubotv-prod.data_engineering.de_archiver_other` t1
JOIN `fubotv-dev.business_analytics.nov_conmebol_live_tmsid` t2 ON t2.tms_id = t1.properties_data_event_context_program_id
WHERE receivedat >= '2021-11-01' and receivedat <= '2021-11-18'
AND lower(properties_device_category) in ('roku','web','iphone','fire_tv','android_tv','smart_tv', 'android_phone','android_tablet', 'ipad')
AND event IN ('ui_interaction')
AND properties_event_sub_category IN ('ftp_game')
AND properties_data_event_context_component = 'ftp_game_question'  
AND userid IS NOT NULL
)

,users_who_played AS
(
SELECT DISTINCT t1.userid,
CASE WHEN t1.userid = t2.userid --and LOWER(t1.device_category) = LOWER(t2.device_category) 
THEN 'yes' ELSE 'no' END AS played_ftp,
COUNT (DISTINCT ftp_question_id) AS question_count
FROM view_match t1
left join played_ftp t2 ON t1.userid = t2.userid 
GROUP BY 1,2
)

,conmebol_viewership AS
(
SELECT DISTINCT
user_id AS userid,
played_ftp,
question_count,
sum(t1.duration)/3600 as Hours,
COUNT (DISTINCT t3.tms_id) as num_matches
FROM `fubotv-prod.data_insights.video_in_progress_via_va_view` t1
INNER JOIN `fubotv-dev.business_analytics.nov_conmebol_live_tmsid` t3 on t1.tms_id=t3.tms_id
INNER JOIN users_who_played t2 ON t1.user_id = t2.userid --AND t1.device_category = t2.device_category
WHERE DATE(start_time,'America/New_York') IN ('2021-11-11','2021-11-12', '2021-11-16')
AND LOWER(t1.device_category) IN ('roku','web','iphone','android_tv', 'android_phone','android_tablet', 'ipad','fire_tv', 'smart_tv')
AND t1.playback_type not like '%dvr%' AND t1.playback_type not like '%vod%' AND t1.playback_type not like '%lookback%'
GROUP BY 1,2,3
)

SELECT DISTINCT userid, device_category
FROM played_ftp 

---------------------------------------------- Nov Conmebol 2021 - Lift in all 3 Days -----------------------------------
WITH view_match AS 
(
SELECT DISTINCT *
FROM `fubotv-dev.business_analytics.NN_view_match_nov_conmebol_2021`
)

, played_ftp AS (
SELECT distinct
--properties_device_category AS device_category,
DATE(receivedat, "EST") as match_date,
DATE_TRUNC(receivedat, hour) as clock_hour,
timestamp,
t1.userid,
properties_profile_id AS profile_id,
properties_data_event_context_program_id AS tms_id,
properties_data_event_context_ftp_game_question_id AS ftp_question_id
FROM `fubotv-prod.data_engineering.de_archiver_other` t1
INNER JOIN `view_match` t2 ON t1.userid = t2.userid AND t2.clock_hour = DATE_TRUNC(receivedat, hour)
--JOIN `fubotv-dev.business_analytics.nov_conmebol_live_tmsid` t2 ON t2.tms_id = t1.properties_data_event_context_program_id
WHERE receivedat >= '2021-11-01' and receivedat <= '2021-11-18'
AND lower(properties_device_category) in ('roku','web','iphone','fire_tv','android_tv','smart_tv', 'android_phone','android_tablet', 'ipad')
AND event IN ('ui_interaction')
AND properties_event_sub_category IN ('ftp_game')
AND properties_data_event_context_component = 'ftp_game_question'  
AND t1.userid IS NOT NULL
)


,users AS (
SELECT DISTINCT t1.userid, 
CASE WHEN t1.userid = t2.userid THEN 'yes' ELSE 'no' END AS played_ftp
FROM view_match t1
LEFT JOIN played_ftp t2 on t1.userid = t2.userid 
ORDER BY 1,2
)

, users_day AS
(
SELECT DISTINCT t1.userid,
t1.match_date,
CASE WHEN t1.userid = t2.userid THEN 'yes' ELSE 'no' END AS date_ftp
FROM view_match t1
LEFT JOIN played_ftp t2 ON t1.userid = t2.userid AND t1.match_date = t2.match_date
)

,users_who_played AS
(
SELECT DISTINCT t1.userid,
t1.tms_id,
t1.clock_hour,
played_ftp,
CASE WHEN t1.userid = t3.userid THEN 'yes' ELSE 'no' END AS match_ftp,
t3.userid as user_id,
t3.clock_hour
FROM view_match t1
inner join users t2 ON t1.userid = t2.userid 
left join played_ftp t3 ON t1.userid = t3.userid AND t1.clock_hour = t3.clock_hour
)

, users_who_played_final AS (
SELECT *
FROM (
SELECT DISTINCT userid, tms_id, played_ftp, match_ftp,
ROW_NUMBER() OVER (PARTITION BY userid, tms_id ORDER BY match_ftp DESC) as r1
FROM users_who_played 
ORDER BY 1,2,3
    )
WHERE r1=1
)

, ftp_users AS (
SELECT userid, tms_id, played_ftp, match_ftp
FROM users_who_played_final 
)

, ftp_user2 AS (
SELECT t1.userid, t1.tms_id, t2.start_time, t1.played_ftp, t3.date_ftp, t1.match_ftp
FROM ftp_users t1 
JOIN ( SELECT DISTINCT tms_id, start_time
FROM `fubotv-dev.business_analytics.ref_timeslot_hours_tmsid_view`) t2 ON t1.tms_id = t2.tms_id 
JOIN users_day t3 ON t1.userid = t3.userid AND DATE(t2.start_time, "EST") = t3.match_date
)

,conmebol_viewership AS
(
SELECT DISTINCT
t1.user_id as userid,
t1.tms_id,
sum(t1.duration)/3600 as Hours,
1 AS count_tms_id
FROM `fubotv-prod.data_insights.video_in_progress_via_va_view` t1
INNER JOIN `fubotv-dev.business_analytics.nov_conmebol_live_tmsid` t3 on t1.tms_id=t3.tms_id
INNER JOIN users_who_played t2 ON t1.user_id = t2.userid --AND t1.device_category = t2.device_category
WHERE DATE(start_time,'America/New_York') IN ('2021-11-11','2021-11-12', '2021-11-16')
AND LOWER(t1.device_category) IN ('roku','web','iphone','android_tv', 'android_phone','android_tablet', 'ipad','fire_tv', 'smart_tv')
AND t1.playback_type not like '%dvr%' AND t1.playback_type not like '%vod%' AND t1.playback_type not like '%lookback%'
GROUP BY 1,2,4
)

,final AS (
SELECT t1.*, t2.* EXCEPT (userid, tms_id)
FROM ftp_user2 t1
INNER JOIN conmebol_viewership t2 on t1.userid = t2.userid AND t1.tms_id = t2.tms_id
ORDER BY userid, tms_id
)

, lift_in_days AS (
SELECT DISTINCT DATE(start_time) AS m_date,
date_ftp, 
COUNT(DISTINCT userid) as uniques,
SUM(Hours) as Hours,
SUM(count_tms_id) as match_count
FROM final 
GROUP BY 1,2
)

SELECT *
FROM lift_in_days 

------------------------------------- Retention --------------------------------------------------------
WITH view_match AS 
(
SELECT DISTINCT *
FROM `fubotv-dev.business_analytics.NN_view_match_nov_conmebol_2021`
)

, played_ftp AS (
SELECT distinct *
FROM `fubotv-dev.business_analytics.NN_played_ftp_nov_conmebol`
)


,users AS (
SELECT DISTINCT t1.userid, 
CASE WHEN t1.userid = t2.userid THEN 'yes' ELSE 'no' END AS played_ftp
FROM view_match t1
LEFT JOIN played_ftp t2 on t1.userid = t2.userid 
ORDER BY 1,2
)

, users_day AS
(
SELECT DISTINCT t1.userid,
t1.match_date,
CASE WHEN t1.userid = t2.userid THEN 'yes' ELSE 'no' END AS date_ftp
FROM view_match t1
LEFT JOIN played_ftp t2 ON t1.userid = t2.userid AND t1.match_date = t2.match_date
)

,users_who_played AS
(
SELECT DISTINCT t1.userid,
t1.tms_id,
t1.clock_hour,
played_ftp,
CASE WHEN t1.userid = t3.userid THEN 'yes' ELSE 'no' END AS match_ftp,
t3.userid as user_id,
t3.clock_hour
FROM view_match t1
inner join users t2 ON t1.userid = t2.userid 
left join played_ftp t3 ON t1.userid = t3.userid AND t1.clock_hour = t3.clock_hour
)

, users_who_played_final AS (
SELECT *
FROM (
SELECT DISTINCT userid, tms_id, played_ftp, match_ftp,
ROW_NUMBER() OVER (PARTITION BY userid, tms_id ORDER BY match_ftp DESC) as r1
FROM users_who_played 
ORDER BY 1,2,3
    )
WHERE r1=1
)

, ftp_users AS (
SELECT userid, tms_id, played_ftp, match_ftp
FROM users_who_played_final 
)

, ftp_user2 AS (
SELECT t1.userid, t1.tms_id, t2.start_time, t1.played_ftp, t3.date_ftp, t1.match_ftp
FROM ftp_users t1 
JOIN ( SELECT DISTINCT tms_id, start_time
FROM `fubotv-dev.business_analytics.ref_timeslot_hours_tmsid_view`) t2 ON t1.tms_id = t2.tms_id 
JOIN users_day t3 ON t1.userid = t3.userid AND DATE(t2.start_time, 'EST') = t3.match_date
)

,conmebol_viewership AS
(
SELECT DISTINCT
t1.user_id as userid,
t1.tms_id,
sum(t1.duration)/3600 as Hours,
1 AS count_tms_id
FROM `fubotv-prod.data_insights.video_in_progress_via_va_view` t1
INNER JOIN `fubotv-dev.business_analytics.nov_conmebol_live_tmsid` t3 on t1.tms_id=t3.tms_id
INNER JOIN users_who_played t2 ON t1.user_id = t2.userid --AND t1.device_category = t2.device_category
WHERE DATE(start_time,'America/New_York') IN ('2021-11-11','2021-11-12', '2021-11-16')
AND LOWER(t1.device_category) IN ('roku','web','iphone','android_tv', 'android_phone','android_tablet', 'ipad','fire_tv', 'smart_tv')
AND t1.playback_type not like '%dvr%' AND t1.playback_type not like '%vod%' AND t1.playback_type not like '%lookback%'
GROUP BY 1,2,4
)

,final AS (
SELECT 
t1.*, 
t2.* EXCEPT (userid, tms_id)
FROM ftp_user2 t1
INNER JOIN conmebol_viewership t2 on t1.userid = t2.userid AND t1.tms_id = t2.tms_id
ORDER BY userid, tms_id
)

, ret_2days AS (

    SELECT DISTINCT userid
    FROM final 
    WHERE DATE(start_time, 'EST') >='2021-11-11' AND DATE(start_time, 'EST') <'2021-11-13'
    AND date_ftp ='yes'   
)

,ret_3days AS (
SELECT DISTINCT 
t1.userid
FROM final t1
INNER JOIN ret_2days t2 ON t1.userid = t2.userid
WHERE DATE(start_time, 'EST') >= '2021-11-16'
AND date_ftp ='yes' 
)

SELECT COUNT(DISTINCt userid) 
FROM ret_3days