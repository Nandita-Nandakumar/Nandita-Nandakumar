--------------------------------------------------------------------------- FTP - PFL - Week 5 ----------------------------------------------------------------------------------
WITH view_match AS 
(
SELECT
user_id AS userid,
v.tms_id,
MIN(start_time) AS timestamp,
SUM(duration)/ 3600 as hours
FROM `fubotv-prod.dmp.viewership_activities` v
JOIN `fubotv-prod.data_insights.daily_status_static_update` t1 on v.user_id = t1.account_code and t1.day = DATE(v.start_time)
WHERE v.playback_type not like '%dvr%' AND v.playback_type not like '%vod%' AND v.playback_type not like '%lookback%' -- include live & startover
AND lower(v.device_category) IN ('roku','web','iphone','android_tv', 'android_phone','android_tablet', 'ipad', 'fire_tv', 'smart_tv')
AND (v.start_time between '2022-03-16' and '2022-03-20'
OR TIMESTAMP_ADD(v.start_time, INTERVAL v.duration SECOND) between '2022-03-16' and '2022-03-20')
AND tms_id = 'EP041888580005'
GROUP BY 1,2
)


, played_ftp AS (
SELECT DISTINCT
timestamp,
userid,
properties_device_category AS device_category,
properties_profile_id AS profile_id,
properties_data_event_context_program_id AS tms_id,
properties_data_event_context_ftp_game_question_id AS ftp_question_id
FROM `fubotv-prod.data_engineering.de_archiver_other` t1
WHERE receivedat >= '2022-03-16' and receivedat <= '2022-03-20'
AND lower(properties_device_category) IN ('roku','web','iphone','android_tv','android_phone','android_tablet', 'ipad', 'fire_tv', 'smart_tv')
AND event IN ('ui_interaction')
AND properties_event_sub_category IN ('ftp_game')
AND properties_data_event_context_component = 'ftp_game_question'  
AND properties_data_event_context_program_id = 'EP041888580005'
AND userid IS NOT NULL
)

/*
--- Total Questions ---
SELECT COUNT (DISTINCT ftp_question_id)
FROM played_ftp
*/

,users_who_played AS
(
SELECT DISTINCT 
t1.userid,
CASE WHEN t1.userid = t2.userid THEN 'yes' ELSE 'no' END AS played_ftp,
COUNT (DISTINCT ftp_question_id) AS question_count
FROM view_match t1
LEFT JOIN played_ftp t2 ON t1.userid = t2.userid 
GROUP BY 1,2
)

-- SELECT DISTINCT *
-- FROM users_who_played 

,pfl_viewership AS
(
SELECT DISTINCT
user_id,
played_ftp,
question_count,
SUM(t1.duration)/3600 as Hours,
COUNT (DISTINCT t1.tms_id) as num_matches --- equals total users since only one TMS id
FROM `fubotv-prod.data_insights.video_in_progress_via_va_view` t1
INNER JOIN users_who_played t2 ON t1.user_id = t2.userid 
WHERE DATE(start_time,'America/New_York') IN ('2022-03-18', '2022-03-19')
AND LOWER(t1.device_category) IN ('roku','web','iphone','android_tv', 'android_phone','android_tablet', 'ipad','fire_tv', 'smart_tv')
AND t1.playback_type not like '%dvr%' AND t1.playback_type not like '%vod%' AND t1.playback_type not like '%lookback%'
AND t1.tms_id = 'EP041888580005'
GROUP BY 1,2,3
)

SELECT DISTINCT *
FROM pfl_viewership 

--------------------------------------------------------------------------------------- PFL - WEEK 5 - DEVICE -------------------------------------------------------------------
WITH view_match AS 
(
SELECT
device_category,
COUNT(DISTINCT user_id) AS count_users,
FROM `fubotv-prod.dmp.viewership_activities` v
JOIN `fubotv-prod.data_insights.daily_status_static_update` t1 on v.user_id = t1.account_code and t1.day = DATE(v.start_time)
WHERE v.playback_type not like '%dvr%' AND v.playback_type not like '%vod%' AND v.playback_type not like '%lookback%' -- include live & startover
AND lower(v.device_category) IN ('roku','web','iphone','android_tv', 'android_phone','android_tablet', 'ipad', 'fire_tv', 'smart_tv')
AND (v.start_time between '2022-03-16' and '2022-03-20'
OR TIMESTAMP_ADD(v.start_time, INTERVAL v.duration SECOND) between '2022-03-16' and '2022-03-20')
AND tms_id = 'EP041888580005'
GROUP BY 1
)

, played_ftp AS (
SELECT distinct
properties_device_category AS device_category,
timestamp,
userid,
properties_profile_id AS profile_id,
properties_data_event_context_program_id AS tms_id,
properties_data_event_context_ftp_game_question_id AS ftp_question_id
FROM `fubotv-prod.data_engineering.de_archiver_other`
WHERE receivedat >= '2022-03-16' and receivedat <= '2022-03-20'
AND lower(properties_device_category) IN ('roku','web','iphone','android_tv','android_phone','android_tablet', 'ipad', 'fire_tv', 'smart_tv')
AND event IN ('ui_interaction')
AND properties_event_sub_category IN ('ftp_game')
AND properties_data_event_context_component = 'ftp_game_question'  
AND properties_data_event_context_program_id = 'EP041888580005'
AND userid IS NOT NULL
)

/*
SELECT DISTINCT userid, device_category
FROM played_ftp */

SELECT DISTINCT device_category, count_users
FROM view_match
