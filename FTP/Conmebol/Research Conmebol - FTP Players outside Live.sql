--- Research : FTP Players outside Live Dates
WITH view_match AS 
(
SELECT DISTINCT
user_id AS userid,
m.tms_id,
MIN(start_time) AS timestamp,
SUM(duration)/ 3600 as hours
FROM `fubotv-prod.dmp.viewership_activities` v
INNER JOIN `fubotv-dev.business_analytics.nov_conmebol_live_tmsid` m ON v.tms_id = m.tms_id
JOIN `fubotv-prod.data_insights.daily_status_static_update` t1 on v.user_id = t1.account_code and t1.day = DATE(v.start_time)
WHERE v.playback_type not like '%dvr%' AND v.playback_type not like '%vod%' AND v.playback_type not like '%lookback%' -- include live & startover
AND lower(v.device_category) IN ('roku','web','iphone','fire_tv','android_tv','smart_tv', 'android_phone','android_tablet', 'ipad')
AND (v.start_time between '2021-11-01' and '2021-11-18'
OR TIMESTAMP_ADD(v.start_time, INTERVAL v.duration SECOND) between '2021-11-01' and '2021-11-18')
GROUP BY 1,2
)

,played_ftp_outside AS (
SELECT DISTINCT userid , receivedat, properties_data_event_context_ftp_game_question_id, properties_data_event_context_ftp_game_id
  FROM `fubotv-prod.data_engineering.de_archiver_other`
  WHERE DATE(receivedat, 'America/New_York') >= '2021-11-01' 
  AND DATE(receivedat, 'America/New_York') <= '2021-11-30'
  AND DATE(receivedat,'America/New_York') NOT IN ('2021-11-11','2021-11-12', '2021-11-16')
  AND event IN ('ui_interaction')
  AND properties_event_sub_category IN ('ftp_game')
  AND properties_data_event_context_component = 'ftp_game_question'
  AND userid IS NOT NULL
)

SELECT DISTINCT t2.userid , receivedat, properties_data_event_context_ftp_game_id, properties_data_event_context_ftp_game_question_id,
FROM view_match t1
INNER JOIN played_ftp_outside t2 ON t1.userid = t2.userid
WHERE properties_data_event_context_ftp_game_id IN ('7ee63c60-d3bc-43db-bab0-dcb6780d3581', '4f1cfd3a-9e58-45ef-870e-f5776a723e52', '90739e6e-d31e-44fa-a141-b96281d78eff', '3d36478e-7132-4588-befe-01a79716d824', '1d1088bc-4039-4806-a202-648247748c9b',
# 16th context game ids
'ebe022e9-a41f-41fd-934e-143493bbec5b', '7e6b0c2b-1ed1-46e5-8852-986051bf9070', '11e1e0f3-6b96-40f0-be7a-9a5b71c30e79', 'e50a802e-b3da-44df-9e72-f25a1deeecb4', 'eb0c0bb9-7088-4ea1-b3ac-a6cef05fdde2')
ORDER BY 1,2


--- Research : FTP Players During Live Dates
WITH view_match AS 
(
SELECT DISTINCT
user_id AS userid,
m.tms_id,
MIN(start_time) AS timestamp,
SUM(duration)/ 3600 as hours
FROM `fubotv-prod.dmp.viewership_activities` v
INNER JOIN `fubotv-dev.business_analytics.nov_conmebol_live_tmsid` m ON v.tms_id = m.tms_id
JOIN `fubotv-prod.data_insights.daily_status_static_update` t1 on v.user_id = t1.account_code and t1.day = DATE(v.start_time)
WHERE v.playback_type not like '%dvr%' AND v.playback_type not like '%vod%' AND v.playback_type not like '%lookback%' -- include live & startover
AND lower(v.device_category) IN ('roku','web','iphone','fire_tv','android_tv','smart_tv', 'android_phone','android_tablet', 'ipad')
AND (v.start_time between '2021-11-01' and '2021-11-18'
OR TIMESTAMP_ADD(v.start_time, INTERVAL v.duration SECOND) between '2021-11-01' and '2021-11-18')
GROUP BY 1,2
)

,played_ftp AS (
SELECT DISTINCT userid , receivedat, properties_data_event_context_ftp_game_question_id, properties_data_event_context_ftp_game_id
  FROM `fubotv-prod.data_engineering.de_archiver_other`
  WHERE DATE(receivedat,'America/New_York') IN ('2021-11-11','2021-11-12', '2021-11-16')
  AND event IN ('ui_interaction')
  AND properties_event_sub_category IN ('ftp_game')
  AND properties_data_event_context_component = 'ftp_game_question'
  AND userid IS NOT NULL
)

SELECT DISTINCT t2.userid, receivedat, properties_data_event_context_ftp_game_id, properties_data_event_context_ftp_game_question_id,
FROM view_match t1
INNER JOIN played_ftp t2 ON t1.userid = t2.userid
WHERE properties_data_event_context_ftp_game_id NOT IN ('7ee63c60-d3bc-43db-bab0-dcb6780d3581', '4f1cfd3a-9e58-45ef-870e-f5776a723e52', '90739e6e-d31e-44fa-a141-b96281d78eff', '3d36478e-7132-4588-befe-01a79716d824', '1d1088bc-4039-4806-a202-648247748c9b',
# 16th context game ids
'ebe022e9-a41f-41fd-934e-143493bbec5b', '7e6b0c2b-1ed1-46e5-8852-986051bf9070', '11e1e0f3-6b96-40f0-be7a-9a5b71c30e79', 'e50a802e-b3da-44df-9e72-f25a1deeecb4', 'eb0c0bb9-7088-4ea1-b3ac-a6cef05fdde2')
ORDER BY 1,2