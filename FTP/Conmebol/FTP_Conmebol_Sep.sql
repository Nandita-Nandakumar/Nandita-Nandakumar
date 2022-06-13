WITH view_match AS 
(
SELECT
'1_watched_match_ftp_window' AS event, # who was watching the game
device_category,
MIN(start_time) AS timestamp,
user_id AS userid,
profile_id,
plan_type,
v.tms_id,
v.playback_type,
SUM(v.duration)/60 AS watch_min,
'' AS ftp_exp_loc,
'' AS ftp_exp_loc_section,
'' AS ftp_game_id,
'' AS ftp_question_id,
'' AS ftp_question_pos
FROM `fubotv-prod.dmp.viewership_activities` v
JOIN `fubotv-dev.business_analytics.conmebol_sep_tmsid` m ON v.tms_id = m.tms_id
JOIN `fubotv-prod.data_insights.daily_status_static_update` t1 on v.user_id = t1.account_code and t1.day = DATE(v.start_time)
JOIN `fubotv-prod.business_analytics.NN_plancode_to_plantype` p ON p.plan_code = t1.plan_code
WHERE v.playback_type not like '%dvr%' AND v.playback_type not like '%vod%' AND v.playback_type not like '%lookback%' -- include live & startover
AND lower(v.device_category) IN ('roku','web','iphone','fire_tv','android_tv','smart_tv', 'android_phone','android_tablet', 'ipad')
AND (v.start_time between '2021-09-01' and '2021-09-14'
OR TIMESTAMP_ADD(v.start_time, INTERVAL v.duration SECOND) between '2021-09-01' and '2021-09-14')
GROUP BY 1,2,4,5,6,7,8
)

, played_ftp AS (
SELECT distinct
'4_played_ftp' as event, # played FTP
properties_device_category AS device_category,
timestamp,
userid,
properties_profile_id AS profile_id,
properties_data_event_context_program_id AS tms_id,
'' AS playback_type,
NULL AS watch_min,
CASE WHEN properties_data_event_context_page = 'player' THEN 'player' ELSE 'out-of-player' END AS ftp_exp_loc,
properties_data_event_context_section AS ftp_exp_loc_section,
properties_data_event_context_ftp_game_id AS ftp_game_id,
properties_data_event_context_ftp_game_question_id AS ftp_question_id,
properties_data_event_context_ftp_game_question_position AS ftp_question_pos,
# receivedat,
# event,
# properties_event_sub_category,
# properties_data_event_context_asset_id,
# properties_data_event_context_page,
# properties_data_event_context_section,
# properties_data_event_context_element,
# properties_data_event_context_component,
# properties_data_event_context_action,
# properties_data_event_context_ftp_game_type,
# properties_data_event_context_ftp_game_player_email,
# properties_data_event_context_ftp_game_question,
# properties_data_event_context_ftp_game_question_state
FROM `fubotv-prod.data_engineering.de_archiver_other`
WHERE receivedat >= '2021-09-01' and receivedat < '2021-09-14'
AND lower(properties_device_category) in ('roku','web','iphone','fire_tv','android_tv','smart_tv', 'android_phone','android_tablet', 'ipad')
AND event IN ('ui_interaction')
AND properties_event_sub_category IN ('ftp_game')
AND properties_data_event_context_component = 'ftp_game_question'    ----- REMAINDER
# AND properties_data_event_context_page = 'player' -- player: in player exp; <>player: out of player exp
# AND properties_data_event_context_section = 'fanview' -- fanview: for roku
)

/*
SELECT DISTINCT userid
FROM played_ftp
*/

,users_who_played AS
(
SELECT DISTINCT t1.userid, plan_type,
CASE WHEN t1.userid = t2.userid THEN 'yes' ELSE 'no' END AS played_ftp
FROM view_match t1
LEFT JOIN played_ftp t2 ON t1.userid = t2.userid 
)

# SELECT DISTINCT userid, plan_type, played_ftp
# FROM users_who_played

,viewership AS
(
SELECT DATE(start_time) as day, 
plan_type,
played_ftp,
COUNT(DISTINCT userid) as uniques, SUM(duration)/3600 as hours
FROM `fubotv-prod.data_insights.video_in_progress_via_va_view` t1 
INNER JOIN users_who_played t2 on t1.user_id = t2.userid 
INNER JOIN `fubotv-prod.data_insights.tmsid_genre_mapping` t3 ON t1.tms_id = t3.tms_id
WHERE DATE(start_time) >= '2021-09-10'
AND LOWER(device_category) IN ('roku','web','iphone','fire_tv','android_tv','smart_tv', 'android_phone','android_tablet', 'ipad')
GROUP BY 1,2,3
)

SELECT *
FROM viewership


/*
------------------------------------------------------- OLD CONMEBOL ----------------------------------------------------------
WITH view_match AS 
(
SELECT
'1_watched_match_ftp_window' AS event, # who was watching the game
device_category,
MIN(start_time) AS timestamp,
user_id AS userid,
profile_id,
plan_type,
v.tms_id,
v.playback_type,
SUM(v.duration)/60 AS watch_min,
'' AS ftp_exp_loc,
'' AS ftp_exp_loc_section,
'' AS ftp_game_id,
'' AS ftp_question_id,
'' AS ftp_question_pos
FROM `fubotv-prod.dmp.viewership_activities` v
#JOIN `fubotv-dev.business_analytics.conmebol_sep_tmsid` m ON v.tms_id = m.tms_id
JOIN `fubotv-dev.business_analytics.Conmebol - June 2021 - TMS_id` m ON v.tms_id = m.tms_id
JOIN `fubotv-prod.data_insights.daily_status_static_update` t1 on v.user_id = t1.account_code and t1.day = DATE(v.start_time)
JOIN `fubotv-prod.business_analytics.NN_plancode_to_plantype` p ON p.plan_code = t1.plan_code
#where LOWER(device_category) in ('roku','web','iphone','fire_tv','android_tv','smart_tv', 'android_phone','android_tablet')
WHERE  lower(device_category) in ('roku','iphone','ipad','android_phone','android_tablet')
AND v.playback_type not like '%dvr%' AND v.playback_type not like '%vod%' AND v.playback_type not like '%lookback%' -- include live & startover
AND (v.start_time between '2021-06-01' and '2021-06-09'
OR TIMESTAMP_ADD(v.start_time, INTERVAL v.duration SECOND) between '2021-06-01' and '2021-06-09')
GROUP BY 1,2,4,5,6,7,8
)

, played_ftp AS (
SELECT distinct
'4_played_ftp' as event, # played FTP
properties_device_category AS device_category,
timestamp,
userid,
properties_profile_id AS profile_id,
properties_data_event_context_program_id AS tms_id,
'' AS playback_type,
NULL AS watch_min,
CASE WHEN properties_data_event_context_page = 'player' THEN 'player' ELSE 'out-of-player' END AS ftp_exp_loc,
properties_data_event_context_section AS ftp_exp_loc_section,
properties_data_event_context_ftp_game_id AS ftp_game_id,
properties_data_event_context_ftp_game_question_id AS ftp_question_id,
properties_data_event_context_ftp_game_question_position AS ftp_question_pos,
# receivedat,
# event,
# properties_event_sub_category,
# properties_data_event_context_asset_id,
# properties_data_event_context_page,
# properties_data_event_context_section,
# properties_data_event_context_element,
# properties_data_event_context_component,
# properties_data_event_context_action,
# properties_data_event_context_ftp_game_type,
# properties_data_event_context_ftp_game_player_email,
# properties_data_event_context_ftp_game_question,
# properties_data_event_context_ftp_game_question_state
FROM `fubotv-prod.data_engineering.de_archiver_other`
WHERE receivedat >= '2021-06-01' and receivedat <= '2021-06-09'
AND lower(properties_device_category) in ('roku','iphone','ipad','android_phone','android_tablet')
#AND lower(properties_device_category) in ('roku','web','iphone','fire_tv','android_tv','smart_tv', 'android_phone','android_tablet')
AND event IN ('ui_interaction')
AND properties_event_sub_category IN ('ftp_game')
AND properties_data_event_context_component = 'ftp_game_question'    ----- REMAINDER
# AND properties_data_event_context_page = 'player' -- player: in player exp; <>player: out of player exp
# AND properties_data_event_context_section = 'fanview' -- fanview: for roku
)

,users_who_played AS
(
SELECT DISTINCT t1.userid, plan_type,
CASE WHEN t1.userid = t2.userid THEN 'yes' ELSE 'no' END AS played_ftp
FROM view_match t1
LEFT JOIN played_ftp t2 ON t1.userid = t2.userid 
)

# SELECT DISTINCT userid, plan_type, played_ftp
# FROM users_who_played

,viewership AS
(
SELECT DATE(start_time) as day, played_ftp, t4.plan_type, COUNT(DISTINCT userid)  as uniques,SUM(duration)/3600 as hours
FROM `fubotv-prod.data_insights.video_in_progress_via_va_view` t1 
INNER JOIN users_who_played t2 on t1.user_id = t2.userid
JOIN `fubotv-prod.data_insights.daily_status_static_update` t3 on t1.user_id = t3.account_code and t3.day = DATE(t1.start_time)
JOIN `fubotv-prod.business_analytics.NN_plancode_to_plantype` t4 ON t3.plan_code = t4.plan_code
INNER JOIN `fubotv-prod.data_insights.tmsid_genre_mapping` t5 ON t1.tms_id = t5.tms_id
WHERE DATE(start_time) >= '2021-06-10'
AND DATE(start_time) <= '2021-06-14'
#AND device_category in ('roku','web','iphone','fire_tv','android_tv','smart_tv', 'android_phone','android_tablet')
AND lower(device_category) in ('roku','iphone','ipad','android_phone','android_tablet')
GROUP BY 1,2,3
)

SELECT *
FROM viewership
*/

-------------------------------------------------------------------------------- Tenure of FTP Players --------------------------------------------

WITH view_match AS 
(
SELECT
device_category,
user_id AS userid,
profile_id,
v.tms_id,
--plan_type,
MIN(start_time) AS timestamp,
SUM(v.duration)/60 AS watch_min,
FROM `fubotv-prod.dmp.viewership_activities` v
JOIN `fubotv-dev.business_analytics.conmebol_sep_tmsid` m ON v.tms_id = m.tms_id
JOIN `fubotv-prod.data_insights.daily_status_static_update` t1 on v.user_id = t1.account_code and t1.day = DATE(v.start_time)
--JOIN `fubotv-prod.business_analytics.NN_plancode_to_plantype_v2` p ON p.plan_code = t1.plan_code
WHERE v.playback_type not like '%dvr%' AND v.playback_type not like '%vod%' AND v.playback_type not like '%lookback%' -- include live & startover
AND lower(v.device_category) IN ('roku','web','iphone','fire_tv','android_tv','smart_tv', 'android_phone','android_tablet', 'ipad')
AND (v.start_time between '2021-09-01' and '2021-09-14'
OR TIMESTAMP_ADD(v.start_time, INTERVAL v.duration SECOND) between '2021-09-01' and '2021-09-14')
GROUP BY 1,2,3,4--,5
)

, played_ftp AS (
SELECT distinct
properties_device_category AS device_category,
timestamp,
userid,
properties_profile_id AS profile_id,
properties_data_event_context_program_id AS tms_id,
properties_data_event_context_ftp_game_question_id AS ftp_question_id,
FROM `fubotv-prod.data_engineering.de_archiver_other`
WHERE receivedat >= '2021-09-01' and receivedat < '2021-09-14'
AND lower(properties_device_category) in ('roku','web','iphone','fire_tv','android_tv','smart_tv', 'android_phone','android_tablet', 'ipad')
AND event IN ('ui_interaction')
AND properties_event_sub_category IN ('ftp_game')
AND properties_data_event_context_component = 'ftp_game_question'    ----- REMINDER

,users_who_played AS
(
SELECT DISTINCT 
t1.userid, 
--plan_type, 
CASE WHEN t1.userid = t2.userid THEN 'yes' ELSE 'no' END AS played_ftp,
COUNT (DISTINCT ftp_question_id) as ftp_questions
FROM view_match t1
LEFT JOIN played_ftp t2 ON t1.userid = t2.userid 
GROUP BY 1,2--,3
)

,viewership AS
(
SELECT 
DATE(start_time) as day, 
played_ftp,
ftp_questions,
user_id,
--plan_type,
SUM(duration)/3600 as hours
FROM `fubotv-prod.data_insights.video_in_progress_via_va_view` t1 
INNER JOIN users_who_played t2 on t1.user_id = t2.userid
INNER JOIN `fubotv-prod.data_insights.tmsid_genre_mapping` t3 ON t1.tms_id = t3.tms_id
WHERE DATE(start_time) >= '2021-09-10'
AND DATE(start_time) <= '2021-09-19'
AND LOWER(device_category) IN ('roku','web','iphone','fire_tv','android_tv','smart_tv', 'android_phone','android_tablet', 'ipad')
GROUP BY 1,2,3,4--,5
)


------------------ SEP FTP Conmebol Viewership --------

WITH view_match AS 
(
SELECT
device_category,
user_id AS userid,
MIN(start_time) AS timestamp,
SUM(duration)/ 3600 as hours
FROM `fubotv-prod.dmp.viewership_activities` v
JOIN `fubotv-dev.business_analytics.conmebol_sep_tmsid` m ON v.tms_id = m.tms_id
JOIN `fubotv-prod.data_insights.daily_status_static_update` t1 on v.user_id = t1.account_code and t1.day = DATE(v.start_time)
--JOIN `fubotv-prod.business_analytics.NN_plancode_to_plantype_v2` p ON p.plan_code = t1.plan_code
WHERE v.playback_type not like '%dvr%' AND v.playback_type not like '%vod%' AND v.playback_type not like '%lookback%' -- include live & startover
AND lower(v.device_category) IN ('roku','web','iphone','fire_tv','android_tv','smart_tv', 'android_phone','android_tablet', 'ipad')
AND (v.start_time between '2021-09-01' and '2021-09-14'
OR TIMESTAMP_ADD(v.start_time, INTERVAL v.duration SECOND) between '2021-09-01' and '2021-09-14')
GROUP BY 1,2
)

, played_ftp AS (
SELECT distinct
properties_device_category AS device_category,
timestamp,
userid,
properties_profile_id AS profile_id,
properties_data_event_context_program_id AS tms_id,
properties_data_event_context_ftp_game_question_id AS ftp_question_id,
FROM `fubotv-prod.data_engineering.de_archiver_other`
WHERE receivedat >= '2021-09-01' and receivedat < '2021-09-14'
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
CASE WHEN t1.userid = t2.userid and LOWER(t1.device_category) = LOWER(t2.device_category) THEN 'yes' ELSE 'no' END AS played_ftp,
t1.device_category, 
COUNT(DISTINCT ftp_question_id) as ftp_qs
FROM view_match t1
LEFT JOIN played_ftp t2 ON t1.userid = t2.userid and LOWER(t1.device_category) = LOWER(t2.device_category)
GROUP BY 1,2,3
)

,viewership AS
(
SELECT DISTINCT 
t1.device_category,
user_id,
played_ftp,
ftp_qs,
SUM(duration)/3600 as hours, 
COUNT (DISTINCT t3.tms_id) as num_matches
FROM `fubotv-prod.data_insights.video_in_progress_via_va_view` t1
INNER JOIN `fubotv-dev.business_analytics.conmebol_sep_tmsid` t3 on t1.tms_id=t3.tms_id
INNER JOIN users_who_played t2 ON t1.user_id = t2.userid and LOWER(t1.device_category) = LOWER(t2.device_category)
WHERE DATE(DATETIME(start_time,'America/New_York')) IN ('2021-09-02','2021-09-05','2021-09-09')
AND LOWER(t1.device_category) IN ('roku','web','iphone','fire_tv','android_tv','smart_tv', 'android_phone','android_tablet', 'ipad')
GROUP BY 1,2,3,4
)

SELECT *
FROM viewership 
