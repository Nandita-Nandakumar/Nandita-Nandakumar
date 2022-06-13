-- ########################## --
-- ########## ROKU ########## --
-- ########################## --

WITH view_match AS (
SELECT
'1_watched_match_ftp_window' AS event, # who was watching the game
device_category,
MIN(start_time) AS timestamp,
user_id AS userid,
profile_id,
v.tms_id,
v.playback_type,
m.match,
m.language,
SUM(v.duration)/60 AS watch_min,
'' AS ftp_exp_loc,
'' AS ftp_exp_loc_section,
'' AS ftp_game_id,
'' AS ftp_question_id,
'' AS ftp_question_pos
FROM `fubotv-prod.dmp.viewership_activities` v
JOIN `fubotv-dev.product_analytics.ftp_gameid_mapping` m
ON v.tms_id = m.tms_id
#WHERE lower(v.device_category) IN ('roku','iphone','ipad','android_phone','android_tablet')
WHERE v.playback_type not like '%dvr%' AND v.playback_type not like '%vod%' AND v.playback_type not like '%lookback%' -- include live & startover
AND (v.start_time between '2021-06-03' and '2021-06-10'
OR TIMESTAMP_ADD(v.start_time, INTERVAL v.duration SECOND) between '2021-06-03' and '2021-06-10')
GROUP BY 1,2,4,5,6,7,8,9,11,12,13,14
)

, roku_open_fanview AS (
SELECT distinct
'2_opened_fanview' as event, #opened Fan view
properties_device_category AS device_category,
timestamp,
userid,
properties_profile_id AS profile_id,
SPLIT(properties_data_event_context_asset_id,"_")[OFFSET(0)] AS tms_id,
'' AS playback_type,
b.match,
b.language,
NULL AS watch_min,
properties_data_event_context_page AS ftp_exp_loc,
properties_data_event_context_section AS ftp_exp_loc_section,
'' AS ftp_game_id,
'' AS ftp_question_id,
'' AS ftp_question_pos,
# receivedat,
# event,
# properties_event_sub_category,
# properties_data_event_context_asset_id,
# properties_data_event_context_program_id,
# properties_data_event_context_page,
# properties_data_event_context_section,
# properties_data_event_context_element,
# properties_data_event_context_component,
# properties_data_event_context_action
FROM `fubotv-prod.data_engineering.de_archiver_other` a
JOIN `fubotv-dev.product_analytics.ftp_gameid_mapping` b
ON SPLIT(a.properties_data_event_context_asset_id,"_")[OFFSET(0)] = b.tms_id
WHERE receivedat > '2021-06-03' and receivedat < '2021-06-10'
AND lower(properties_device_category) in ('roku')
AND event IN ('ui_interaction')
AND properties_event_sub_category IN ('fanview')
AND properties_data_event_context_page = 'player'
AND properties_data_event_context_action = 'open'
--order by userid, timestamp
)

, roku_agreed_to_play AS (
SELECT distinct
'3_agreed_to_play' as event, #came to fanview page and accept 
properties_device_category AS device_category,
timestamp,
userid,
properties_profile_id AS profile_id,
properties_data_event_context_program_id AS tms_id,
'' AS playback_type,
NULL AS watch_min,
properties_data_event_context_page AS ftp_exp_loc,
properties_data_event_context_section AS ftp_exp_loc_section,
properties_data_event_context_ftp_game_id AS ftp_game_id,
'' AS ftp_question_id,
'' AS ftp_question_pos,
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
# properties_data_event_context_ftp_game_question_id,
# properties_data_event_context_ftp_game_question,
# properties_data_event_context_ftp_game_question_position,
# properties_data_event_context_ftp_game_question_state
FROM `fubotv-prod.data_engineering.de_archiver_other`
WHERE receivedat > '2021-06-03' and receivedat < '2021-06-10'
AND lower(properties_device_category) in ('roku')
AND event IN ('ui_interaction')
AND properties_event_sub_category IN ('ftp_game')
AND properties_data_event_context_page = 'player' -- player: in player exp; <>player: out of player exp
AND properties_data_event_context_section = 'fanview' -- fanview: for roku
AND properties_data_event_context_element = 'join_game'
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
WHERE receivedat > '2021-06-03' and receivedat < '2021-06-10'
AND lower(properties_device_category) in ('roku','iphone','ipad','android_phone','android_tablet')
AND event IN ('ui_interaction')
AND properties_event_sub_category IN ('ftp_game')
AND properties_data_event_context_component = 'ftp_game_question' .   ----- REMAINDER
# AND properties_data_event_context_page = 'player' -- player: in player exp; <>player: out of player exp
# AND properties_data_event_context_section = 'fanview' -- fanview: for roku
)

, roku_union_events_gameid AS ( 
SELECT *
FROM roku_agreed_to_play
UNION ALL 
SELECT *
FROM played_ftp WHERE device_category = 'roku'
--ORDER BY userid, timestamp --3421
-- 20193
)

, roku_get_correct_gameid AS (
SELECT a.event,
a.device_category,
a.timestamp,
a.userid,
a.profile_id,
a.tms_id,
a.playback_type,
b.match,
b.language,
a.watch_min,
a.ftp_exp_loc,
a.ftp_exp_loc_section,
a.ftp_game_id,
a.ftp_question_id,
a.ftp_question_pos,
FROM roku_union_events_gameid a
JOIN `fubotv-dev.product_analytics.ftp_gameid_mapping` b
ON a.ftp_game_id = b.game_id
AND a.tms_id = b.tms_id
)

, roku_event_sequence AS (
SELECT *
FROM roku_open_fanview
UNION ALL
SELECT *
FROM roku_get_correct_gameid
--ORDER BY userid, timestamp
)

, event_user_list AS (
SELECT DISTINCT event, userid, device_category,ftp_exp_loc, ftp_exp_loc_section
FROM roku_event_sequence
)

, all_things_roku AS (
SELECT device_category, event, ftp_exp_loc, ftp_exp_loc_section
, COUNT(DISTINCT userid) AS user_count
, ROUND(SUM(watch_min),1) AS watch_min
, COUNT(DISTINCT userid) AS user_watch_count
, ROUND(SUM(watch_min)/COUNT(DISTINCT userid),2) AS avg_watch_min
FROM view_match WHERE device_category = 'roku'
GROUP BY 1,2,3,4

UNION ALL

SELECT lower(a.device_category) as device_cateogry, a.event, a.ftp_exp_loc, a.ftp_exp_loc_section
, COUNT(DISTINCT a.userid) AS user_count
, ROUND(SUM(b.watch_min),1) AS watch_min
, COUNT(DISTINCT b.userid) AS user_watch_count
, ROUND(SUM(b.watch_min)/COUNT(DISTINCT a.userid),2) AS avg_watch_min
FROM event_user_list a
LEFT JOIN view_match b
ON a.userid = b.userid
AND lower(a.device_category) = lower(b.device_category)
GROUP BY 1,2,3,4
ORDER BY 1,2,3,4
)


-- ######################################################## --
-- ####### iOS & Android experience ####################### --
-- ####### note: Android had in-player & out-player exp ### --
-- ####### note: iOS only had 6/8/21 games ################ --
-- ######################################################## --

, view_match_2 AS (
SELECT
'1_watched_match_ftp_window' AS event,
device_category,
MIN(start_time) AS timestamp,
user_id AS userid,
profile_id,
v.tms_id,
v.playback_type,
m.match,
m.language,
SUM(v.duration)/60 AS watch_min,
'' AS ftp_exp_loc,
'' AS ftp_exp_loc_section,
'' AS ftp_game_id,
'' AS ftp_question_id,
'' AS ftp_question_pos
FROM `fubotv-prod.dmp.viewership_activities` v
JOIN `fubotv-dev.product_analytics.ftp_gameid_mapping` m
ON v.tms_id = m.tms_id
WHERE lower(v.device_category) IN ('android_phone','android_tablet')
AND v.playback_type not like '%dvr%' AND v.playback_type not like '%vod%' AND v.playback_type not like '%lookback%' -- include live & startover
AND (v.start_time between '2021-06-03' and '2021-06-10'
OR TIMESTAMP_ADD(v.start_time, INTERVAL v.duration SECOND) between '2021-06-03' and '2021-06-10')
GROUP BY 1,2,4,5,6,7,8,9,11,12,13,14

UNION ALL

SELECT
'1_watched_match_ftp_window' AS event,
device_category,
MIN(start_time) AS timestamp,
user_id AS userid,
profile_id,
v.tms_id,
v.playback_type,
m.match,
m.language,
SUM(v.duration)/60 AS watch_min,
'' AS ftp_exp_loc,
'' AS ftp_exp_loc_section,
'' AS ftp_game_id,
'' AS ftp_question_id,
'' AS ftp_question_pos
FROM `fubotv-prod.dmp.viewership_activities` v
JOIN `fubotv-dev.product_analytics.ftp_gameid_mapping` m
ON v.tms_id = m.tms_id
WHERE lower(v.device_category) IN ('iphone','ipad')
AND v.playback_type not like '%dvr%' AND v.playback_type not like '%vod%' AND v.playback_type not like '%lookback%' -- include live & startover
AND (v.start_time between '2021-06-08' and '2021-06-10'
OR TIMESTAMP_ADD(v.start_time, INTERVAL v.duration SECOND) between '2021-06-08' and '2021-06-10')
GROUP BY 1,2,4,5,6,7,8,9,11,12,13,14
)

, mobile_blue_banner_click AS (
SELECT distinct
'2_entered_to_play' as event,
properties_device_category AS device_category,
timestamp,
userid,
properties_profile_id AS profile_id,
COALESCE(IF(properties_data_event_context_program_id = 'TMS-ID-UNAVAILABLE', NULL, properties_data_event_context_program_id),SPLIT(properties_data_event_context_asset_id,"_")[OFFSET(0)]) AS tms_id,
'' AS playback_type,
NULL AS watch_min,
properties_data_event_context_page AS ftp_exp_loc,
properties_data_event_context_section AS ftp_exp_loc_section,
properties_data_event_context_ftp_game_id AS ftp_game_id,
'' AS ftp_question_id,
'' AS ftp_question_pos,
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
# properties_data_event_context_ftp_game_question_id,
# properties_data_event_context_ftp_game_question,
# properties_data_event_context_ftp_game_question_position,
# properties_data_event_context_ftp_game_question_state
FROM `fubotv-prod.data_engineering.de_archiver_other`
WHERE receivedat > '2021-06-03' and receivedat < '2021-06-10'
AND lower(properties_device_category) in ('android_phone','android_tablet')
AND event IN ('ui_interaction')
AND properties_event_sub_category IN ('ftp_game')
AND properties_data_event_context_component = 'ftp_banner'

UNION ALL 

SELECT distinct
'2_entered_to_play' as event,
properties_device_category AS device_category,
timestamp,
userid,
properties_profile_id AS profile_id,
COALESCE(IF(properties_data_event_context_program_id = 'TMS-ID-UNAVAILABLE', NULL, properties_data_event_context_program_id),SPLIT(properties_data_event_context_asset_id,"_")[OFFSET(0)]) AS tms_id,
'' AS playback_type,
NULL AS watch_min,
properties_data_event_context_page AS ftp_exp_loc,
properties_data_event_context_section AS ftp_exp_loc_section,
properties_data_event_context_ftp_game_id AS ftp_game_id,
'' AS ftp_question_id,
'' AS ftp_question_pos,
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
# properties_data_event_context_ftp_game_question_id,
# properties_data_event_context_ftp_game_question,
# properties_data_event_context_ftp_game_question_position,
# properties_data_event_context_ftp_game_question_state
FROM `fubotv-prod.data_engineering.de_archiver_other`
WHERE receivedat > '2021-06-08' and receivedat < '2021-06-10'
AND lower(properties_device_category) in ('iphone','ipad')
AND event IN ('ui_interaction')
AND properties_event_sub_category IN ('ftp_game')
AND properties_data_event_context_component = 'ftp_banner'
)

, android_ftp_carousel_click AS (
SELECT distinct
'2_entered_to_play' as event,
properties_device_category AS device_category,
timestamp,
userid,
properties_profile_id AS profile_id,
COALESCE(IF(properties_data_event_context_program_id = 'TMS-ID-UNAVAILABLE', NULL, properties_data_event_context_program_id),SPLIT(properties_data_event_context_asset_id,"_")[OFFSET(0)]) AS tms_id,
'' AS playback_type,
NULL AS watch_min,
properties_data_event_context_page AS ftp_exp_loc,
'carousel' AS ftp_exp_loc_section,
properties_data_event_context_ftp_game_id AS ftp_game_id,
'' AS ftp_question_id,
'' AS ftp_question_pos,
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
# properties_data_event_context_ftp_game_question_id,
# properties_data_event_context_ftp_game_question,
# properties_data_event_context_ftp_game_question_position,
# properties_data_event_context_ftp_game_question_state
FROM `fubotv-prod.data_engineering.de_archiver_other`
WHERE receivedat > '2021-06-03' and receivedat < '2021-06-10'
AND lower(properties_device_category) in ('android_phone','android_tablet')
AND event IN ('ui_interaction')
AND properties_event_sub_category IN ('ftp_game')
AND properties_data_event_context_page IN ('home_1','conmebol-2021')
AND properties_data_event_context_component = 'ftp_games_conmebol-2021'
AND properties_data_event_context_element = 'free_to_play_game'
)

, mobile_agreed_to_play AS (
SELECT distinct
'3_agreed_to_play' as event,
properties_device_category AS device_category,
timestamp,
userid,
properties_profile_id AS profile_id,
COALESCE(IF(properties_data_event_context_program_id = 'TMS-ID-UNAVAILABLE', NULL, properties_data_event_context_program_id),SPLIT(properties_data_event_context_asset_id,"_")[OFFSET(0)]) AS tms_id,
'' AS playback_type,
NULL AS watch_min,
CASE WHEN properties_data_event_context_page NOT IN ('player') THEN 'out-of-player' ELSE properties_data_event_context_page END AS ftp_exp_loc,
CASE WHEN properties_data_event_context_page NOT IN ('player') THEN 'carousel' ELSE properties_data_event_context_section END AS ftp_exp_loc_section,
properties_data_event_context_ftp_game_id AS ftp_game_id,
'' AS ftp_question_id,
'' AS ftp_question_pos,
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
# properties_data_event_context_ftp_game_question_id,
# properties_data_event_context_ftp_game_question,
# properties_data_event_context_ftp_game_question_position,
# properties_data_event_context_ftp_game_question_state
FROM `fubotv-prod.data_engineering.de_archiver_other`
WHERE receivedat > '2021-06-03' and receivedat < '2021-06-10'
AND lower(properties_device_category) in ('android_phone','android_tablet')
AND event IN ('ui_interaction')
AND properties_event_sub_category IN ('ftp_game')
AND properties_data_event_context_element = 'join_game'

UNION ALL 

SELECT distinct
'3_agreed_to_play' as event,
properties_device_category AS device_category,
timestamp,
userid,
properties_profile_id AS profile_id,
COALESCE(IF(properties_data_event_context_program_id = 'TMS-ID-UNAVAILABLE', NULL, properties_data_event_context_program_id),SPLIT(properties_data_event_context_asset_id,"_")[OFFSET(0)]) AS tms_id,
'' AS playback_type,
NULL AS watch_min,
'player' AS ftp_exp_loc,
properties_data_event_context_section AS ftp_exp_loc_section,
properties_data_event_context_ftp_game_id AS ftp_game_id,
'' AS ftp_question_id,
'' AS ftp_question_pos,
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
# properties_data_event_context_ftp_game_question_id,
# properties_data_event_context_ftp_game_question,
# properties_data_event_context_ftp_game_question_position,
# properties_data_event_context_ftp_game_question_state
FROM `fubotv-prod.data_engineering.de_archiver_other`
WHERE receivedat > '2021-06-08' and receivedat < '2021-06-10'
AND lower(properties_device_category) in ('iphone','ipad')
AND event IN ('ui_interaction')
AND properties_event_sub_category IN ('ftp_game')
AND properties_data_event_context_element = 'join_game'
)

, played_ftp_2 AS (
SELECT distinct
'4_played_ftp' as event,
properties_device_category AS device_category,
timestamp,
userid,
properties_profile_id AS profile_id,
COALESCE(IF(properties_data_event_context_program_id = 'TMS-ID-UNAVAILABLE', NULL, properties_data_event_context_program_id),SPLIT(properties_data_event_context_asset_id,"_")[OFFSET(0)]) AS tms_id,
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
WHERE receivedat > '2021-06-03' and receivedat < '2021-06-10'
AND lower(properties_device_category) in ('android_phone','android_tablet')
AND event IN ('ui_interaction')
AND properties_event_sub_category IN ('ftp_game')
AND properties_data_event_context_component = 'ftp_game_question'

UNION ALL 

SELECT distinct
'4_played_ftp' as event,
properties_device_category AS device_category,
timestamp,
userid,
properties_profile_id AS profile_id,
COALESCE(IF(properties_data_event_context_program_id = 'TMS-ID-UNAVAILABLE', NULL, properties_data_event_context_program_id),SPLIT(properties_data_event_context_asset_id,"_")[OFFSET(0)]) AS tms_id,
'' AS playback_type,
NULL AS watch_min,
'player' AS ftp_exp_loc,
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
FROM `fubotv-prod.data_engineering.de_archiver_other` a
WHERE receivedat > '2021-06-08' and receivedat < '2021-06-10'
AND lower(properties_device_category) in ('iphone','ipad')
AND event IN ('ui_interaction')
AND properties_event_sub_category IN ('ftp_game')
AND properties_data_event_context_component = 'ftp_game_question'
)

, mobile_union_events_gameid AS (
SELECT * FROM mobile_blue_banner_click
UNION ALL
SELECT * FROM android_ftp_carousel_click
UNION ALL
SELECT * FROM mobile_agreed_to_play
UNION ALL 
SELECT * FROM played_ftp_2
)

, mobile_get_correct_gameid_event_sequence AS (
SELECT a.event,
a.device_category,
a.timestamp,
a.userid,
a.profile_id,
a.tms_id,
a.playback_type,
b.match,
# b.language,
a.watch_min,
a.ftp_exp_loc,
a.ftp_exp_loc_section,
a.ftp_game_id,
a.ftp_question_id,
a.ftp_question_pos,
FROM mobile_union_events_gameid a
JOIN (SELECT DISTINCT match, game_id FROM `fubotv-dev.product_analytics.ftp_gameid_mapping`) b
ON a.ftp_game_id = b.game_id
# AND a.tms_id = b.tms_id --mobile doesn't send tmsi_id, hence, can't differentiate spanish nor english games
)

, event_user_list_2 AS (
SELECT DISTINCT event, userid, device_category,ftp_exp_loc, ftp_exp_loc_section
FROM mobile_get_correct_gameid_event_sequence
)

, all_things_others AS (
SELECT CASE WHEN device_category like '%android%' THEN 'andorid_mobile' ELSE 'iOS' END AS device_category
, event
, ftp_exp_loc
, ftp_exp_loc_section
, COUNT(DISTINCT userid) AS user_count
, ROUND(SUM(watch_min),1) AS watch_min
, COUNT(DISTINCT userid) AS user_watch_count
, ROUND(SUM(watch_min)/COUNT(DISTINCT userid),2) AS avg_watch_min
FROM view_match_2 
GROUP BY 1,2,3,4

UNION ALL

SELECT CASE WHEN lower(a.device_category) like '%android%' THEN 'andorid_mobile' ELSE 'iOS' END as device_cateogry
, a.event
, a.ftp_exp_loc
, a.ftp_exp_loc_section
, COUNT(DISTINCT a.userid) AS user_count
, ROUND(SUM(b.watch_min),1) AS watch_min
, COUNT(DISTINCT b.userid) AS user_watch_count
, ROUND(SUM(b.watch_min)/COUNT(DISTINCT a.userid),2) AS avg_watch_min
FROM event_user_list_2 a
LEFT JOIN view_match_2 b
ON a.userid = b.userid
AND lower(a.device_category) = lower(b.device_category)
GROUP BY 1,2,3,4
ORDER BY 1,2,3,4
)

SELECT DISTINCT * FROM all_things_roku
UNION ALL
SELECT DISTINCT * FROM all_things_others


################################################### ROKU ----- NEW EVENT ######################################################

SELECT distinct
'reminder_shown' as event, # Reminder Event Specific 
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
WHERE receivedat > '2021-09-01' and receivedat < '2021-09-02'
AND lower(properties_device_category) in ('roku','iphone','ipad','android_phone','android_tablet')
AND event IN ('ui_interaction')
AND properties_event_sub_category IN ('ftp_game')
AND properties_data_event_context_component = 'reminder'  ----- REMINDER