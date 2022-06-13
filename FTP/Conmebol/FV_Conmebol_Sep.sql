WITH view_match AS 
(
SELECT
'1_watched_match_ftp_window' AS event, # who was watching the game
device_category,
MIN(start_time) AS timestamp,
user_id AS userid,
profile_id,
sub_first_dt,
modified_at,
first_billed_dt,
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
AND lower(v.device_category) IN ('roku','web','fire_tv','android_tv','mobile_web')
AND (v.start_time between '2021-09-01' and '2021-09-14'
OR TIMESTAMP_ADD(v.start_time, INTERVAL v.duration SECOND) between '2021-09-01' and '2021-09-14')
GROUP BY 1,2,4,5,6,7,8,9,10,11
)

, fv_users AS (
 SELECT distinct
'2_opened_fanview' as event, #opened Fan view
properties_device_category AS device_category,
a.timestamp,
a.receivedat,
a.userid,
properties_profile_id AS profile_id,
SPLIT(properties_data_event_context_asset_id,"_")[OFFSET(0)] AS tms_id,
'' AS playback_type,
NULL AS watch_min,
properties_data_event_context_page AS ftp_exp_loc,
properties_data_event_context_section AS ftp_exp_loc_section,
'' AS ftp_game_id,
'' AS ftp_question_id,
'' AS ftp_question_pos,
FROM `fubotv-prod.data_engineering.de_archiver_other` a
JOIN `fubotv-dev.business_analytics.conmebol_sep_tmsid` b ON SPLIT(a.properties_data_event_context_asset_id,"_")[OFFSET(0)] = b.tms_id
WHERE receivedat >= '2021-09-01' and receivedat <= '2021-09-10'
AND a.event IN ('ui_interaction')
AND properties_event_sub_category IN ('fanview')
AND properties_data_event_context_page = 'player'
AND properties_data_event_context_action = 'open'
)


,users_who_watched AS (
SELECT DISTINCT t1.userid,  plan_type,  --, DATE_TRUNC(first_billed_dt, Month) first_billed_month, DATE_TRUNC(sub_first_dt, Month) sub_first_month, modified_at,
CASE WHEN t1.userid = t2.userid THEN 'yes' ELSE 'no' END AS watched_fv,
COUNT(DATE_TRUNC(receivedat, hour)) AS total_hours_FV --- Total Count of Hours Spent on FV by User
FROM view_match t1
LEFT JOIN fv_users t2 ON t1.userid = t2.userid 
GROUP BY 1,2,3
)


,viewership AS
(
SELECT 
DATE(start_time) as day, 
user_id,
watched_fv,
total_hours_FV,
--plan_type,
--primary_genre_group,
COUNT(DISTINCT userid) as uniques, SUM(duration)/3600 as hours
FROM `fubotv-prod.data_insights.video_in_progress_via_va_view` t1 
INNER JOIN users_who_watched  t2 on t1.user_id = t2.userid
INNER JOIN `fubotv-prod.data_insights.tmsid_genre_mapping` t3 ON t1.tms_id = t3.tms_id
WHERE DATE(start_time) >= '2021-09-10'
AND DATE(start_time) <= '2021-09-19'
AND LOWER(device_category) IN ('roku','web','fire_tv','android_tv','mobile_web')
GROUP BY 1,2,3,4
)

SELECT user_id, total_hours_fv
FROM viewership 


------------------------ LATEST 09/23 --------------------------------
WITH view_match AS 
(
SELECT
'1_watched_match_ftp_window' AS event, # who was watching the game
# device_category,
MIN(start_time) AS timestamp,
user_id AS userid,
profile_id,
sub_first_dt,
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
AND lower(v.device_category) IN ('roku','web','fire_tv','android_tv','mobile_web')
 AND user_id = '6004996cc44a8f0001ae23af'
AND (v.start_time between '2021-09-01' and '2021-09-14'
OR TIMESTAMP_ADD(v.start_time, INTERVAL v.duration SECOND) between '2021-09-01' and '2021-09-14')
GROUP BY 1,3,4,5,6,7,8
)


, fv_users AS (
 SELECT distinct
'2_opened_fanview' as event, #opened Fan view
# properties_device_category AS device_category,
a.timestamp,
a.receivedat,
a.userid,
DATE_TRUNC(receivedat, hour) AS clock_hours_FV,
properties_profile_id AS profile_id,
#SPLIT(properties_data_event_context_asset_id,"_")[OFFSET(0)] AS tms_id,
'' AS playback_type,
NULL AS watch_min,
properties_data_event_context_page AS ftp_exp_loc,
properties_data_event_context_section AS ftp_exp_loc_section,
properties_data_event_context_action AS ftp_exp_action,
'' AS ftp_game_id,
'' AS ftp_question_id,
'' AS ftp_question_pos,
FROM `fubotv-prod.data_engineering.de_archiver_other` a
JOIN `fubotv-dev.business_analytics.conmebol_sep_tmsid` b ON SPLIT(a.properties_data_event_context_asset_id,"_")[OFFSET(0)] = b.tms_id
WHERE receivedat >= '2021-09-01' and receivedat <= '2021-09-10'
# AND userid = '5fdbea2ae909140001ba225b'
AND a.event IN ('ui_interaction')
AND properties_event_sub_category IN ('fanview')
#AND properties_data_event_context_page = 'player'
#AND properties_data_event_context_action = 'open'
)


SELECT DISTINCT *
FROM fv_users
WHERE userid = '6004996cc44a8f0001ae23af'
 
, fv_users_temp AS (
SELECT DISTINCT t1.userid --as all_users , t2.userid AS non_open_users, 
,COUNT(DISTINCT(DATE_TRUNC(t1.receivedat, hour))) AS clock_hours_FV
FROM
(
    SELECT * 
    FROM fv_users
) AS t1
LEFT JOIN 
(
    SELECT * 
    FROM fv_users
    WHERE ftp_exp_action = 'open'
    AND fv_users.ftp_exp_loc = 'player'
) AS t2 
ON t1.userid = t2.userid
WHERE t2.userid IS NULL
GROUP BY 1
)

SELECT * FROM fv_users_temp

,fv_consolidated AS (
SELECT DISTINCT userid, COUNT(DISTINCT clock_hours_FV) AS total_hours_FV
FROM fv_users_temp ----- update 
GROUP BY 1
)

,users_who_watched AS (
SELECT DISTINCT t1.userid,  plan_type,
CASE WHEN t1.userid = t2.userid THEN 'yes' ELSE 'no' END AS watched_fv,
total_hours_fv
FROM view_match t1
LEFT JOIN fv_consolidated t2 ON t1.userid = t2.userid 
)



,viewership AS (
SELECT userid,sum(t1.duration)/3600 as Hours , COUNT(DISTINCT t2.tms_id) as num_matches
FROM `fubotv-prod.data_insights.video_in_progress_via_va_view` t1
JOIN users_who_watched t5 ON t5.userid = t1.user_id -- AND t5.watched_device_category = t1.device_category
INNER JOIN `fubotv-dev.business_analytics.conmebol_sep_tmsid` t2 on t1.tms_id=t2.tms_id
WHERE lower(t1.device_category) in ('roku','web','fire_tv','android_tv','mobile_web')
and DATE(DATETIME(start_time,'America/New_York')) IN ('2021-09-02','2021-09-05','2021-09-09')
GROUP BY 1
)
,results AS (
SELECT DISTINCT t1.userid as watched_userid, t2.userid as fv_userid,
CASE WHEN t1.userid = t2.userid THEN 'yes' ELSE 'no' END AS watched_fv, t2.total_hours_fv, t3.hours, num_matches
FROM view_match t1
LEFT JOIN fv_consolidated t2 ON t1.userid = t2.userid
INNER JOIN viewership t3 ON t1.userid = t3.userid
)

SELECT *
FROM results
WHERE watched_fv = 'yes'


-------------------------------------------------- Integrating  FTP for the same devices as FV ---------------------------

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
FROM `fubotv-prod.data_engineering.de_archiver_other`
WHERE receivedat >= '2021-09-01' and receivedat < '2021-09-14'
AND lower(properties_device_category) in ('roku','web','fire_tv','android_tv','mobile_web')
# AND LOWER(device_category) IN ('roku','web','iphone','fire_tv','android_tv','smart_tv', 'android_phone','android_tablet', 'ipad')
AND event IN ('ui_interaction')
AND properties_event_sub_category IN ('ftp_game')
AND properties_data_event_context_component = 'ftp_game_question'    ----- REMINDER
)

SELECT DISTINCT device_category 
FROM played_ftp
WHERE userid = '5d719d8510a7e10001504d36'
/*,users_who_played AS
(
SELECT DISTINCT t1.userid, plan_type, DATE_TRUNC(first_billed_dt, Month) first_billed_month, DATE_TRUNC(sub_first_dt, Month) sub_first_month, modified_at,
CASE WHEN t1.userid = t2.userid THEN 'yes' ELSE 'no' END AS played_ftp
FROM view_match t1
LEFT JOIN played_ftp t2 ON t1.userid = t2.userid 
)

,users_in_fv_also_ftp AS (
SELECT DISTINCT t1.userid, watched_FV, total_hours_FV, played_ftp
FROM users_who_watched t1
LEFT JOIN users_who_played t2 ON t1.userid = t2.userid
)
*/

, ftp_consolidated AS (
    SELECT DISTINCt userid, COUNT(DISTINCT ftp_question_id) as ftp_questions
    FROM played_ftp
    GROUP BY 1
)

SELECT t1.*, CASE WHEN t1.watched_userid = t2.userid THEN 'yes' ELSE 'no' END AS played_ftp, t2.ftp_questions,
FROM results t1
LEFT JOIN ftp_consolidated t2 ON t1.watched_userid = t2.userid


,viewership AS
(
SELECT 
 DATE(start_time) as day, 
# user_id,
watched_fv,
total_hours_FV,
played_ftp,
--plan_type,
--primary_genre_group,
COUNT(DISTINCT userid) as uniques, SUM(duration)/3600 as hours
FROM `fubotv-prod.data_insights.video_in_progress_via_va_view` t1 
INNER JOIN users_in_fv_also_ftp  t2 on t1.user_id = t2.userid
INNER JOIN `fubotv-prod.data_insights.tmsid_genre_mapping` t3 ON t1.tms_id = t3.tms_id
WHERE DATE(start_time) >= '2021-09-10'
AND DATE(start_time) <= '2021-09-19'
AND LOWER(device_category) IN ('roku','web','fire_tv','android_tv','mobile_web')
AND watched_fv = 'yes'
GROUP BY 1,2,3,4
)

SELECT DISTINCT *
FROM viewership
*/


---------------------- -------------------------09/24
WITH view_match AS 
(
SELECT
'1_watched_match_ftp_window' AS event, # who was watching the game
# device_category,
MIN(start_time) AS timestamp,
user_id AS userid,
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
#JOIN `fubotv-prod.data_insights.daily_status_static_update` t1 on v.user_id = t1.account_code and t1.day = DATE(DATETIME(v.start_time,'America/New_York'))
JOIN `fubotv-prod.business_analytics.NN_plancode_to_plantype_v2` p ON p.plan_code = t1.plan_code
WHERE v.playback_type not like '%dvr%' AND v.playback_type not like '%vod%' AND v.playback_type not like '%lookback%' -- include live & startover
AND lower(v.device_category) IN ('roku','web','fire_tv','android_tv','mobile_web')
#AND user_id = '5e4b074eb6c244000127be62'
AND (v.start_time between '2021-09-01' and '2021-09-14'
OR TIMESTAMP_ADD(v.start_time, INTERVAL v.duration SECOND) between '2021-09-01' and '2021-09-14')
GROUP BY 1,3,4,5
)

, fv_users AS (
 SELECT distinct
'2_opened_fanview' as event, #opened Fan view
# properties_device_category AS device_category,
a.timestamp,
a.receivedat,
a.userid,
DATE_TRUNC(receivedat, hour) AS clock_hours_FV,
properties_profile_id AS profile_id,
#SPLIT(properties_data_event_context_asset_id,"_")[OFFSET(0)] AS tms_id,
'' AS playback_type,
NULL AS watch_min,
properties_data_event_context_page AS ftp_exp_loc,
properties_data_event_context_section AS ftp_exp_loc_section,
properties_data_event_context_action AS ftp_exp_action,
'' AS ftp_game_id,
'' AS ftp_question_id,
'' AS ftp_question_pos,
FROM `fubotv-prod.data_engineering.de_archiver_other` a
JOIN `fubotv-dev.business_analytics.conmebol_sep_tmsid` b ON SPLIT(a.properties_data_event_context_asset_id,"_")[OFFSET(0)] = b.tms_id
WHERE receivedat >= '2021-09-01' and receivedat <= '2021-09-10'
AND lower(properties_device_category) IN ('roku','web','fire_tv','android_tv','mobile_web')
# AND userid = '5fdbea2ae909140001ba225b'
AND a.event IN ('ui_interaction')
AND properties_event_sub_category IN ('fanview')
AND properties_data_event_context_page = 'player'
AND properties_data_event_context_action = 'open'
)

, fv_users_temp AS (
SELECT DISTINCT t1.userid --as all_users , t2.userid AS non_open_users, 
,COUNT(DISTINCT(DATE_TRUNC(t1.receivedat, hour))) AS clock_hours_FV
FROM
(
    SELECT * 
    FROM fv_users
) AS t1
/*LEFT JOIN 
(
    SELECT * 
    FROM fv_users
    WHERE ftp_exp_action = 'open'
    AND fv_users.ftp_exp_loc = 'player'
) AS t2 
ON t1.userid = t2.userid
WHERE t2.userid IS NULL */
GROUP BY 1
)
/*
SELECT clock_hours_FV, COUNT (DISTINCT userid)
FROM fv_users_temp
GROUP BY 1
ORDER BY 1
*/

,fv_consolidated AS (
SELECT DISTINCT userid, COUNT(DISTINCT clock_hours_FV) AS total_hours_FV
FROM fv_users ----- update 
GROUP BY 1
ORDER BY 1
)


,users_who_watched AS (
SELECT DISTINCT t1.userid,
CASE WHEN t1.userid = t2.userid THEN 'yes' ELSE 'no' END AS watched_fv,
total_hours_fv
FROM view_match t1
LEFT JOIN fv_consolidated t2 ON t1.userid = t2.userid 
)


/*
SELECT DISTINCT  t1.userid as og_fvuserid, t2.fv_user as new_fvuserid
FROM fv_consolidated t1
LEFT JOIN users_who_watched t2 on t1.userid = t2.fv_user
WHERE t2.fv_user IS NULL 
*/

,viewership AS 
(
SELECT userid,sum(t1.duration)/3600 as Hours , COUNT(DISTINCT t3.tms_id) as num_matches
FROM `fubotv-prod.data_insights.video_in_progress_via_va_view` t1
JOIN users_who_watched t2 ON t2.userid = t1.user_id -- AND t5.watched_device_category = t1.device_category
INNER JOIN `fubotv-dev.business_analytics.conmebol_sep_tmsid` t3 on t1.tms_id=t3.tms_id
WHERE lower(t1.device_category) in ('roku','web','fire_tv','android_tv','mobile_web')
AND DATE(DATETIME(start_time,'America/New_York')) IN ('2021-09-02','2021-09-05','2021-09-09')
GROUP BY 1
)

,results AS (
SELECT DISTINCT t1.userid as watched_userid, watched_fv, t1.total_hours_fv, t3.hours, num_matches
FROM users_who_watched  t1
--LEFT JOIN fv_consolidated t2 ON t1.userid = t2.userid
INNER JOIN viewership t3 ON t1.userid = t3.userid
)

/*
SELECT *
FROM results

*/
-------------------------------------------------- Integrating  FTP for the same devices as FV ---------------------------

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
FROM `fubotv-prod.data_engineering.de_archiver_other`
WHERE receivedat >= '2021-09-01' and receivedat < '2021-09-14'
AND lower(properties_device_category) NOT IN ('roku','web','fire_tv','android_tv','mobile_web') --- TEST
# AND LOWER(device_category) IN ('roku','web','iphone','fire_tv','android_tv','smart_tv', 'android_phone','android_tablet', 'ipad')
AND event IN ('ui_interaction')
AND properties_event_sub_category IN ('ftp_game')
AND properties_data_event_context_component = 'ftp_game_question'    ----- REMINDER
)

,users_who_played AS
(
SELECT DISTINCT t1.userid,
CASE WHEN t1.userid = t2.userid THEN 'yes' ELSE 'no' END AS played_ftp
FROM view_match t1
LEFT JOIN played_ftp t2 ON t1.userid = t2.userid 
)

,users_in_fv_also_ftp AS (
SELECT DISTINCT t1.userid, watched_FV, total_hours_FV, played_ftp
FROM users_who_watched t1
LEFT JOIN users_who_played t2 ON t1.userid = t2.userid
)

, ftp_consolidated AS (
    SELECT DISTINCt userid, COUNT(DISTINCT ftp_question_id) as ftp_questions
    FROM played_ftp
    GROUP BY 1
)
, fv_ftp_results AS (
SELECT t1.*, CASE WHEN t1.watched_userid = t2.userid THEN 'yes' ELSE 'no' END AS played_ftp, t2.ftp_questions,
FROM results t1
LEFT JOIN ftp_consolidated t2 ON t1.watched_userid = t2.userid
)

SELECT *
FROM fv_ftp_results
