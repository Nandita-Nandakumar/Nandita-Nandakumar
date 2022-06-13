---------------------------------------------- FTP - NHL Game EP029977174592 -----------------------------------
WITH view_match AS 
(
SELECT
user_id AS userid,
tms_id,
DATE(start_time) as date_s,
MIN(start_time) AS timestamp,
SUM(duration)/ 3600 as hours
FROM `fubotv-prod.dmp.viewership_activities` v
JOIN `fubotv-prod.data_insights.daily_status_static_update` t1 on v.user_id = t1.account_code and t1.day = DATE(v.start_time)
WHERE v.playback_type not like '%dvr%' AND v.playback_type not like '%vod%' AND v.playback_type not like '%lookback%' -- include live & startover
AND lower(v.device_category) IN ('roku','web','iphone','fire_tv','android_tv','smart_tv', 'android_phone','android_tablet', 'ipad')
AND (v.start_time between '2021-12-07' and '2021-12-10'
OR TIMESTAMP_ADD(v.start_time, INTERVAL v.duration SECOND) between '2021-12-07' and '2021-12-10')
AND tms_id = 'EP029977174592'
GROUP BY 1,2,3
)

, played_ftp AS (
SELECT distinct
timestamp,
userid,
properties_profile_id AS profile_id,
properties_data_event_context_program_id AS tms_id,
properties_data_event_context_ftp_game_question_id AS ftp_question_id
FROM `fubotv-prod.data_engineering.de_archiver_other` t1
WHERE receivedat >= '2021-12-08' and receivedat <= '2021-12-10'
AND lower(properties_device_category) in ('roku','web','iphone','fire_tv','android_tv','smart_tv', 'android_phone','android_tablet', 'ipad')
AND event IN ('ui_interaction')
AND properties_event_sub_category IN ('ftp_game')
AND properties_data_event_context_component = 'ftp_game_question'  
AND userid IS NOT NULL
AND properties_data_event_context_program_id = 'EP029977174592'
)

,users_who_played AS
(
SELECT DISTINCT t1.userid,
CASE WHEN t1.userid = t2.userid 
THEN 'yes' ELSE 'no' END AS played_ftp,
FROM view_match t1
left join played_ftp t2 ON t1.userid = t2.userid 
)

,nhl_viewership AS
(
SELECT DISTINCT
user_id as userid,
played_ftp,
sum(t1.duration)/3600 as Hours,
COUNT (DISTINCT tms_id) as num_matches
FROM `fubotv-prod.data_insights.video_in_progress_via_va_view` t1
INNER JOIN users_who_played t2 ON t1.user_id = t2.userid
WHERE DATE(start_time,'America/New_York') IN ('2021-12-07','2021-12-08', '2021-12-09')
AND tms_id = 'EP029977174592'
AND LOWER(t1.device_category) IN ('roku','web','iphone','android_tv', 'android_phone','android_tablet', 'ipad','fire_tv', 'smart_tv')
AND t1.playback_type not like '%dvr%' AND t1.playback_type not like '%vod%' AND t1.playback_type not like '%lookback%'
GROUP BY 1,2
)

SELECT *
FROM nhl_viewership