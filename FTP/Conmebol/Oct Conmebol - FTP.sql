WITH view_match AS 
(
SELECT
device_category,
user_id AS userid,
m.tms_id,
--playback_type,
plan_type,
MIN(start_time) AS timestamp,
SUM(duration)/ 3600 as hours
FROM `fubotv-prod.dmp.viewership_activities` v
INNER JOIN `fubotv-dev.business_analytics.oct_conmebol_live_tmsid` m ON v.tms_id = m.tms_id
JOIN `fubotv-prod.data_insights.daily_status_static_update` t1 on v.user_id = t1.account_code and t1.day = DATE(v.start_time)
--JOIN `fubotv-prod.business_analytics.NN_plancode_to_plantype_v2` p ON p.plan_code = t1.plan_code
WHERE v.playback_type not like '%dvr%' AND v.playback_type not like '%vod%' AND v.playback_type not like '%lookback%' -- include live & startover
AND lower(v.device_category) IN ('roku','web','iphone','fire_tv','android_tv','smart_tv', 'android_phone','android_tablet', 'ipad')
AND (v.start_time between '2021-10-01' and '2021-10-11'
OR TIMESTAMP_ADD(v.start_time, INTERVAL v.duration SECOND) between '2021-10-01' and '2021-10-11')
GROUP BY 1,2,3,4--,5
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
--JOIN `fubotv-dev.business_analytics.oct_conmebol_live_tmsid` t2 ON t2.tms_id = t1.properties_data_event_context_program_id
WHERE receivedat >= '2021-10-01' and receivedat <= '2021-10-11'
AND lower(properties_device_category) in ('roku','web','iphone','fire_tv','android_tv','smart_tv', 'android_phone','android_tablet', 'ipad')
AND event IN ('ui_interaction')
AND properties_event_sub_category IN ('ftp_game')
AND properties_data_event_context_component = 'ftp_game_question'  
AND userid IS NOT NULL
)

,users_who_played AS
(
SELECT DISTINCT 
t1.userid,
--plan_type,
CASE WHEN t1.userid = t2.userid and LOWER(t1.device_category) = LOWER(t2.device_category) THEN 'yes' ELSE 'no' END AS played_ftp,
t1.device_category, COUNT(DISTINCT ftp_question_id) as ftp_qs_count
FROM view_match t1
left join played_ftp t2 ON t1.userid = t2.userid and LOWER(t1.device_category) = LOWER(t2.device_category)
GROUP BY 1,2,3--,4
)

,conmebol_viewership AS
(
SELECT DISTINCT
t1.device_category,user_id,
--plan_type, 
played_ftp,ftp_qs_count, 
sum(t1.duration)/3600 as Hours, COUNT(DISTINCT t3.tms_id) as num_matches
FROM `fubotv-prod.data_insights.video_in_progress_via_va_view` t1
INNER JOIN `fubotv-dev.business_analytics.oct_conmebol_live_tmsid` t3 on t1.tms_id=t3.tms_id
INNER JOIN users_who_played t2 ON t1.user_id = t2.userid and LOWER(t1.device_category) = LOWER(t2.device_category)
WHERE DATE(DATETIME(start_time,'America/New_York')) IN ('2021-10-07','2021-10-10')
AND LOWER(t1.device_category) IN ('roku','web','iphone','fire_tv','android_tv','smart_tv', 'android_phone','android_tablet', 'ipad')
GROUP BY 1,2,3,4--,5
)

SELECT *
FROM conmebol_viewership

------------------------------------- OCT Conmebol Viewership --------------------
WITH view_match AS 
(
SELECT
user_id AS userid,
--device_category,
m.tms_id,
MIN(start_time) AS timestamp,
SUM(duration)/ 3600 as hours
FROM `fubotv-prod.dmp.viewership_activities` v
INNER JOIN `fubotv-dev.business_analytics.oct_conmebol_live_tmsid` m ON v.tms_id = m.tms_id
JOIN `fubotv-prod.data_insights.daily_status_static_update` t1 on v.user_id = t1.account_code and t1.day = DATE(v.start_time)
WHERE v.playback_type not like '%dvr%' AND v.playback_type not like '%vod%' AND v.playback_type not like '%lookback%' -- include live & startover
AND lower(v.device_category) IN ('roku','web','iphone','android_tv', 'android_phone','android_tablet', 'ipad', 'fire_tv', 'smart_tv')
AND (v.start_time between '2021-10-01' and '2021-10-17'
OR TIMESTAMP_ADD(v.start_time, INTERVAL v.duration SECOND) between '2021-10-01' and '2021-10-17')
GROUP BY 1,2--,3
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
--JOIN `fubotv-dev.business_analytics.oct_conmebol_live_tmsid` t2 ON t2.tms_id = t1.properties_data_event_context_program_id
WHERE receivedat >= '2021-10-01' and receivedat <= '2021-10-17'
AND lower(properties_device_category) in ('roku','web','iphone','android_tv','android_phone','android_tablet', 'ipad', 'fire_tv', 'smart_tv'
)
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
sum(t1.duration)/3600 as Hours,
COUNT(DISTINCT t3.tms_id) as num_matches
FROM `fubotv-prod.data_insights.video_in_progress_via_va_view` t1
INNER JOIN `fubotv-dev.business_analytics.oct_conmebol_live_tmsid` t3 on t1.tms_id=t3.tms_id
INNER JOIN users_who_played t2 ON t1.user_id = t2.userid 
WHERE DATE(start_time,'America/New_York') IN ('2021-10-07','2021-10-10', '2021-10-14')
AND LOWER(t1.device_category) IN ('roku','web','iphone','android_tv', 'android_phone','android_tablet', 'ipad','fire_tv', 'smart_tv'
)
AND t1.playback_type not like '%dvr%' AND t1.playback_type not like '%vod%' AND t1.playback_type not like '%lookback%'
GROUP BY 1,2--,3
)

SELECT *
FROM conmebol_viewership

------------------------------------------------  ALL 3 DAYS ----------------------------------------------------
WITH view_match AS 
(
SELECT
user_id AS userid,
--device_category,
m.tms_id,
MIN(start_time) AS timestamp,
SUM(duration)/ 3600 as hours
FROM `fubotv-prod.dmp.viewership_activities` v
INNER JOIN `fubotv-dev.business_analytics.oct_conmebol_live_tmsid` m ON v.tms_id = m.tms_id
JOIN `fubotv-prod.data_insights.daily_status_static_update` t1 on v.user_id = t1.account_code and t1.day = DATE(v.start_time)
WHERE v.playback_type not like '%dvr%' AND v.playback_type not like '%vod%' AND v.playback_type not like '%lookback%' -- include live & startover
AND lower(v.device_category) IN ('roku','web','iphone','android_tv', 'android_phone','android_tablet', 'ipad', 'fire_tv', 'smart_tv')
AND (v.start_time between '2021-10-01' and '2021-10-17'
OR TIMESTAMP_ADD(v.start_time, INTERVAL v.duration SECOND) between '2021-10-01' and '2021-10-17')
GROUP BY 1,2--,3
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
JOIN `fubotv-dev.business_analytics.oct_conmebol_live_tmsid` t2 ON t2.tms_id = t1.properties_data_event_context_program_id
WHERE receivedat >= '2021-10-01' and receivedat <= '2021-10-17'
AND lower(properties_device_category) in ('roku','web','iphone','android_tv','android_phone','android_tablet', 'ipad', 'fire_tv', 'smart_tv')
AND event IN ('ui_interaction')
AND properties_event_sub_category IN ('ftp_game')
AND properties_data_event_context_component = 'ftp_game_question'  
AND userid IS NOT NULL
)

,users_who_played AS
(
SELECT DISTINCT t1.userid,
--t1.device_category,
CASE WHEN t1.userid = t2.userid --and LOWER(t1.device_category) = LOWER(t2.device_category) 
THEN 'yes' ELSE 'no' END AS played_ftp,
COUNT (DISTINCT ftp_question_id) AS question_count
FROM view_match t1
left join played_ftp t2 ON t1.userid = t2.userid 
GROUP BY 1,2--,3
)

,conmebol_viewership AS
(
SELECT DISTINCT
user_id,
played_ftp,
question_count,
--t2.device_category,
sum(t1.duration)/3600 as Hours,
COUNT (DISTINCT t3.tms_id) as match_count
FROM `fubotv-prod.data_insights.video_in_progress_via_va_view` t1
INNER JOIN `fubotv-dev.business_analytics.oct_conmebol_live_tmsid` t3 on t1.tms_id=t3.tms_id
INNER JOIN users_who_played t2 ON t1.user_id = t2.userid --AND t1.device_category = t2.device_category
WHERE DATE(start_time,'America/New_York') IN ('2021-10-07','2021-10-10', '2021-10-14')
AND LOWER(t1.device_category) IN ('roku','web','iphone','android_tv', 'android_phone','android_tablet', 'ipad','fire_tv', 'smart_tv')
AND t1.playback_type not like '%dvr%' AND t1.playback_type not like '%vod%' AND t1.playback_type not like '%lookback%'
GROUP BY 1,2,3--,4
)

SELECT *
FROM conmebol_viewership

------------------------------------- Timeslot Hours OCT Conmebol Viewership --------------------
WITH view_match AS 
(
SELECT
user_id AS userid,
m.tms_id,
t2.* EXCEPT (tms_id)
FROM `fubotv-prod.dmp.viewership_activities` v
INNER JOIN `fubotv-dev.business_analytics.oct_conmebol_live_tmsid` m ON v.tms_id = m.tms_id
INNER JOIN `fubotv-dev.business_analytics.ref_timeslot_hours_tmsid_view` t2 ON v.tms_id = t2.tms_id
WHERE v.playback_type not like '%dvr%' AND v.playback_type not like '%vod%' AND v.playback_type not like '%lookback%' -- include live & startover
AND lower(v.device_category) IN ('roku','web','iphone','android_tv', 'android_phone','android_tablet', 'ipad', 'fire_tv', 'smart_tv')
AND (v.start_time between '2021-10-01' and '2021-10-17'
OR TIMESTAMP_ADD(v.start_time, INTERVAL v.duration SECOND) between '2021-10-01' and '2021-10-17')
)

, played_ftp AS (
SELECT distinct
DATE_TRUNC(receivedat, hour) as clock_hour,
t1.userid,
properties_data_event_context_ftp_game_question_id AS ftp_question_id
FROM `fubotv-prod.data_engineering.de_archiver_other` t1
--JOIN `fubotv-dev.business_analytics.oct_conmebol_live_tmsid` t2 ON t2.tms_id = t1.properties_data_event_context_program_id
INNER JOIN `view_match` t2 ON t1.userid = t2.userid AND t2.clock_hour = DATE_TRUNC(receivedat, hour)
WHERE receivedat >= '2021-10-01' and receivedat <= '2021-10-17'
AND lower(properties_device_category) in ('roku','web','iphone','android_tv','android_phone','android_tablet', 'ipad', 'fire_tv', 'smart_tv')
AND event IN ('ui_interaction')
AND properties_event_sub_category IN ('ftp_game')
AND properties_data_event_context_component = 'ftp_game_question'  
)

SELECT DISTINCT *
FROM view_match
ORDER BY userid,tms_id,clock_hour



---------------------------------------- match ftp and day ftp using Timeslot Hours - Oct FTP Conmebol----------------------

WITH view_match AS 
(
SELECT
user_id AS userid,
m.tms_id,
DATE(v.start_time, "EST") AS match_date,
t2.* EXCEPT (tms_id)
FROM `fubotv-prod.dmp.viewership_activities` v
INNER JOIN `fubotv-dev.business_analytics.oct_conmebol_live_tmsid` m ON v.tms_id = m.tms_id
INNER JOIN `fubotv-dev.business_analytics.ref_timeslot_hours_tmsid_view` t2 ON v.tms_id = t2.tms_id
WHERE v.playback_type not like '%dvr%' AND v.playback_type not like '%vod%' AND v.playback_type not like '%lookback%' -- include live & startover
AND lower(v.device_category) IN ('roku','web','iphone','android_tv', 'android_phone','android_tablet', 'ipad', 'fire_tv', 'smart_tv')
AND (v.start_time between '2021-10-01' and '2021-10-17'
OR TIMESTAMP_ADD(v.start_time, INTERVAL v.duration SECOND) between '2021-10-01' and '2021-10-17')
)

, played_ftp AS (
SELECT distinct
DATE(receivedat, "EST") as match_date,
DATE_TRUNC(receivedat, hour) as clock_hour,
t1.userid,
properties_data_event_context_ftp_game_question_id AS ftp_question_id
FROM `fubotv-prod.data_engineering.de_archiver_other` t1
--JOIN `fubotv-dev.business_analytics.oct_conmebol_live_tmsid` t2 ON t2.tms_id = t1.properties_data_event_context_program_id
INNER JOIN `view_match` t2 ON t1.userid = t2.userid AND t2.clock_hour = DATE_TRUNC(receivedat, hour)
WHERE receivedat >= '2021-10-01' and receivedat <= '2021-10-17'
AND lower(properties_device_category) in ('roku','web','iphone','android_tv','android_phone','android_tablet', 'ipad', 'fire_tv', 'smart_tv')
AND event IN ('ui_interaction')
AND properties_event_sub_category IN ('ftp_game')
AND properties_data_event_context_component = 'ftp_game_question'  
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
FROM view_match 
LEFT JOIN played_ftp t2 ON t1.userid = t2.userid AND t1.match_date = t2.match_date
)

,users_who_played AS
(
SELECT DISTINCT t1.userid,
tms_id,
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
SELECT t1.userid, t1.tms_id, t2.start_date, t1.played_ftp, t3.date_ftp, t1.match_ftp
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
INNER JOIN `fubotv-dev.business_analytics.oct_conmebol_live_tmsid` t3 on t1.tms_id=t3.tms_id
WHERE DATE(start_time,'America/New_York') IN ('2021-10-07','2021-10-10', '2021-10-14')
AND LOWER(t1.device_category) IN ('roku','web','iphone','android_tv', 'android_phone','android_tablet', 'ipad','fire_tv', 'smart_tv')
AND t1.playback_type not like '%dvr%' AND t1.playback_type not like '%vod%' AND t1.playback_type not like '%lookback%'
GROUP BY 1,2,4
)

SELECT t2.*, t1.* EXCEPT (userid, tms_id)
FROM conmebol_viewership t1
INNER JOIN ftp_users t2 on t1.userid = t2.userid AND t1.tms_id = t2.tms_id
ORDER BY userid, tms_id


SELECT DISTINCT t1.tms_id, t2.program_title, start_time
FROM `fubotv-dev.business_analytics.ref_timeslot_hours_tmsid_view` t1
INNER JOIN `fubotv-prod.data_insights.video_in_progress_via_va_view` t2 ON t1.tms_id = t2.tms_id


---------------------------------------- match ftp and day ftp using Timeslot Hours - Oct FTP Conmebol----------------------

WITH view_match AS 
(
SELECT *
FROM `fubotv-dev.business_analytics.NN_ftp_view_match_oct`
)

, played_ftp AS (
SELECT distinct
DATE(receivedat, "EST") as match_date,
DATE_TRUNC(receivedat, hour) as clock_hour,
t1.userid,
properties_data_event_context_ftp_game_question_id AS ftp_question_id
FROM `fubotv-prod.data_engineering.de_archiver_other` t1
INNER JOIN `view_match` t2 ON t1.userid = t2.userid AND t2.clock_hour = DATE_TRUNC(receivedat, hour)
WHERE receivedat >= '2021-10-01' and receivedat <= '2021-10-17'
AND lower(properties_device_category) in ('roku','web','iphone','android_tv','android_phone','android_tablet', 'ipad', 'fire_tv', 'smart_tv')
AND event IN ('ui_interaction')
AND properties_event_sub_category IN ('ftp_game')
AND properties_data_event_context_component = 'ftp_game_question'  
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
LEFT JOIN played_ftp t2 ON t1.userid = t2.userid AND t1.clock_hour = t2.clock_hour 
)

,date_ftp AS (
SELECT * EXCEPT (r)
FROM (
SELECT DISTINCT *,
ROW_NUMBER() OVER (PARTITION BY userid, match_date ORDER BY date_ftp DESC) AS r
FROM users_day 
ORDER BY 1,2
)
WHERE r=1
ORDER BY 1,2
)

,users_who_played AS
(
SELECT DISTINCT t1.userid,
tms_id,
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
FROM 
(
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
SELECT t1.userid, t1.tms_id, DATETIME(t2.start_time,"EST") as start_time, t1.played_ftp, t3.date_ftp, t1.match_ftp
FROM ftp_users t1 
JOIN ( SELECT DISTINCT tms_id, start_time
FROM `fubotv-dev.business_analytics.ref_timeslot_hours_tmsid_view`) t2 ON t1.tms_id = t2.tms_id 
JOIN date_ftp t3 ON t1.userid = t3.userid AND DATE(t2.start_time, "EST") = t3.match_date
)

,conmebol_viewership AS
(
SELECT DISTINCT
t1.user_id as userid,
t1.tms_id,
sum(t1.duration)/3600 as Hours,
1 AS count_tms_id
FROM `fubotv-prod.data_insights.video_in_progress_via_va_view` t1
INNER JOIN `fubotv-dev.business_analytics.ref_timeslot_hours_tmsid_view` t3 on t1.tms_id=t3.tms_id
WHERE DATE(t1.start_time,'America/New_York') IN ('2021-10-07','2021-10-10', '2021-10-14')
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

, match_day12 AS (
SELECT DISTINCT tms_id, 
match_ftp,
COUNT(DISTINCT userid) as uniques,
SUM(Hours) as Hours,
FROM final 
WHERE DATE(start_time) > '2021-10-10'
GROUP BY 1,2
)

SELECT *
-- FROM lift_in_days 
FROM match_day12 ----- USE THIS FOR TOTAL HOURS


---------------------------------------- NEW match ftp and day ftp using Timeslot Hours - Oct FTP Conmebol----------------------

WITH view_match AS 
(
SELECT *
FROM `fubotv-dev.business_analytics.NN_ftp_view_match_oct`
)

, played_ftp AS (
SELECT distinct *
FROM `fubotv-dev.business_analytics.NN_ftp_played_ftp_oct_conmebol`
)

,users AS (
SELECT DISTINCT t1.userid, 
CASE WHEN t1.userid = t2.userid THEN 'yes' ELSE 'no' END AS played_ftp,
ftp_question_id AS ftp_qs
FROM view_match t1
LEFT JOIN played_ftp t2 on t1.userid = t2.userid 
ORDER BY 1,2
)


, users_day AS
(
SELECT DISTINCT t1.userid,
t1.match_date,
CASE WHEN t1.userid = t2.userid THEN 'yes' ELSE 'no' END AS date_ftp,
FROM view_match t1
LEFT JOIN played_ftp t2 ON t1.userid = t2.userid AND t1.clock_hour = t2.clock_hour 
)

,date_ftp AS (
SELECT * EXCEPT (r)
FROM (
SELECT DISTINCT *,
ROW_NUMBER() OVER (PARTITION BY userid, match_date ORDER BY date_ftp DESC) AS r
FROM users_day 
ORDER BY 1,2
)
WHERE r=1
ORDER BY 1,2
)

,users_who_played AS
(
SELECT DISTINCT t1.userid,
tms_id,
t1.clock_hour,
played_ftp,
CASE WHEN t1.userid = t3.userid THEN 'yes' ELSE 'no' END AS match_ftp,
t3.userid as user_id,
FROM view_match t1
inner join users t2 ON t1.userid = t2.userid 
left join played_ftp t3 ON t1.userid = t3.userid AND t1.clock_hour = t3.clock_hour
)

, users_who_played_final AS (
SELECT *
FROM 
(
SELECT DISTINCT userid, tms_id, played_ftp, match_ftp,clock_hour,
ROW_NUMBER() OVER (PARTITION BY userid, tms_id ORDER BY match_ftp DESC) as r1
FROM users_who_played 
ORDER BY 1,2,3,4
)
WHERE r1=1
)

, ftp_users AS (
SELECT userid, tms_id, played_ftp, match_ftp, clock_hour
FROM users_who_played_final 
)

, ftp_user2 AS (
SELECT t1.userid, t1.tms_id, DATETIME(t2.start_time,"EST") as start_time, t1.played_ftp, t3.date_ftp, t1.match_ftp, clock_hour
FROM ftp_users t1 
JOIN ( SELECT DISTINCT tms_id, start_time
FROM `fubotv-dev.business_analytics.ref_timeslot_hours_tmsid_view`) t2 ON t1.tms_id = t2.tms_id 
JOIN date_ftp t3 ON t1.userid = t3.userid AND DATE(t2.start_time, "EST") = t3.match_date
)

,conmebol_viewership AS
(
SELECT DISTINCT
t1.user_id as userid,
t1.tms_id,
sum(t1.duration)/3600 as Hours,
1 AS count_tms_id
FROM `fubotv-prod.data_insights.video_in_progress_via_va_view` t1
INNER JOIN `fubotv-dev.business_analytics.ref_timeslot_hours_tmsid_view` t3 on t1.tms_id=t3.tms_id
WHERE DATE(t1.start_time,'America/New_York') IN ('2021-10-07','2021-10-10', '2021-10-14')
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
SELECT DISTINCT 
t1.userid
FROM final t1
INNER JOIN 
(
    SELECT DISTINCT userid
    FROM final 
    WHERE DATE(start_time) = '2021-10-07'
    AND date_ftp ='yes'   
) t2 ON t1.userid =t2.userid
WHERE DATE(start_time) = '2021-10-10'
AND date_ftp ='yes'
)

,ret_3days AS (
SELECT DISTINCT 
t1.userid
FROM final t1
INNER JOIN ret_2days t2 ON t1.userid = t2.userid
WHERE DATE(start_time) = '2021-10-14'
AND date_ftp ='yes' 
)

, match_day12 AS (
SELECT DISTINCT
start_time,
t1.clock_hour,
tms_id, 
match_ftp,
t1.userid,
Hours as Hours,
FROM final  t1
INNER JOIN played_ftp t2 on t1.userid = t2.userid AND DATE(t1.start_time) = t2.match_date
WHERE DATE(start_time) > '2021-10-10'
AND played_ftp = 'yes'
GROUP BY 1,2,3,4,5
)

SELECT t1.start_time,t1.tms_id,t1.match_ftp,COUNT(DISTINCT t1.userid) as user_count, SUM(Hours), COUNT(DISTINCT t2.ftp_question_id) as ftp_qs_count
FROM match_day12 t1
LEFT JOIN played_ftp t2 ON t1.userid = t2.userid AND (t1.clock_hour) = t2.clock_hour ----- FOR HOURS and User Counts USE PREV ITERATION OF QUERY
GROUP BY 1,2,3


----- Match Day 12 Pull -------
WITH data AS (
SELECT t1.userid, t1.tms_id, t1.device_category, t1.clock_hour, COUNT(ftp_question_id) AS ftp_qs_count
FROM `fubotv-dev.business_analytics.NN_ftp_view_match_oct_14` t1
JOIN `fubotv-dev.business_analytics.NN_ftp_played_ftp_oct_conmebol` t2 ON t1.userid = t2.userid AND LOWER(t1.device_category)= LOWER(t2.device_category) AND t1.clock_hour = t2.clock_hour
WHERE t1.match_date = '2021-10-14'
GROUP BY 1,2,3,4
ORDER BY 1,2,4
)

SELECT tms_id, device_category, SUM(ftp_qs_count) as qs_count, COUNT(DISTINCT userid) as users
FROM data 
GROUP BY 1,2
ORDER BY 1,2