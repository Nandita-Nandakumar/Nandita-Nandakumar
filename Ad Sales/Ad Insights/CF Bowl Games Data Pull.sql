--Report Criteria:
--Dates : 2020, 2021 ESPN Bowl Games
--Playback_Type : Live

WITH viewership as (
SELECT user_id,DATE_TRUNC(DATE(v.start_time, 'America/New_York'), month) as month_est,date(start_time, 'America/New_York') as date_est, v.program_title, v.episode_title, 
CASE WHEN LOWER(v.program_title) LIKE '%sugar%' THEN 'Sugar Bowl'
WHEN LOWER(v.program_title) LIKE '%fiesta%' THEN 'Fiesta Bowl'
WHEN LOWER(v.program_title) LIKE '%cotton%' THEN 'Cotton Bowl'
WHEN LOWER(v.program_title) LIKE '%orange%' THEN 'Orange Bowl'
WHEN LOWER(v.program_title) LIKE '%peach%' THEN 'Peach Bowl'
WHEN LOWER(v.program_title) LIKE '%rose%' THEN 'Rose Bowl'
ELSE NULL
END AS Bowl_Type
,SUM(duration)/3600 as hours
FROM `fubotv-prod.data_insights.video_in_progress_via_va_view` v
JOIN `fubotv-prod.data_insights.tmsid_genre_mapping` m on v.tms_id =m.tms_id -- Genre Mapping Table
JOIN `fubotv-dev.business_analytics.station_mapping_table_51220` s on s.station_name =v.Channel -- Station Mapping Table
WHERE v.tms_id IS NOT NULL
AND UPPER(v.tms_id) NOT IN ("","00000000000000","TMS-ID-UNAVAILABLE")
AND DATE(v.start_time, 'America/New_York') >= '2020-12-01' 
AND DATE(v.start_time, 'America/New_York') <= '2021-01-31' -- adjust dates for program
AND ((lower(v.program_title)) LIKE '%bowl%' )
AND (lower(v.program_title)) NOT LIKE '%bowling%'
AND LOWER(playback_type) = 'live'
AND ( LOWER(station_mapping) LIKE 'espn%' )
GROUP BY 1,2,3,4,5,6
)
, user_related AS (
SELECT user_id, COUNT(DISTINCT Bowl_Type) as Games,SUM(hours) as Hours, COUNT(DISTINCT user_id) AS Viewers
FROM viewership
WHERE Bowl_Type IS NOT NULL
GROUP BY 1
ORDER BY 1
)

,users_2020 AS (
SELECT DISTINCT
/*
CASE WHEN Games = 1 THEN '1 Game only'
WHEN Games = 2 THEN '2 Games'
WHEN Games = 3 THEN '3 Games'
WHEN Games = 4 THEN '4 Games'
WHEN Games = 5 THEN '5 Games'
WHEN Games = 6 THEN 'All Bowl Games'
END AS Games_Watched,
SUM(Hours) AS Hours, COUNT(DISTINCT user_id) as Uniques
*/
user_id AS users_2020, '20' AS year
FROM user_related 
--GROUP BY 1
)

----- 2021 Users
,viewership2 as (
SELECT user_id,DATE_TRUNC(DATE(v.start_time, 'America/New_York'), month) as month_est,date(start_time, 'America/New_York') as date_est, v.program_title, v.episode_title, 
CASE WHEN LOWER(v.program_title) LIKE '%sugar%' THEN 'Sugar Bowl'
WHEN LOWER(v.program_title) LIKE '%fiesta%' THEN 'Fiesta Bowl'
WHEN LOWER(v.program_title) LIKE '%cotton%' THEN 'Cotton Bowl'
WHEN LOWER(v.program_title) LIKE '%orange%' THEN 'Orange Bowl'
WHEN LOWER(v.program_title) LIKE '%peach%' THEN 'Peach Bowl'
WHEN LOWER(v.program_title) LIKE '%rose%' THEN 'Rose Bowl'
ELSE NULL
END AS Bowl_Type
,SUM(duration)/3600 as hours
FROM `fubotv-prod.data_insights.video_in_progress_via_va_view` v
JOIN `fubotv-prod.data_insights.tmsid_genre_mapping` m on v.tms_id =m.tms_id -- Genre Mapping Table
JOIN `fubotv-dev.business_analytics.station_mapping_table_51220` s on s.station_name =v.Channel -- Station Mapping Table
WHERE v.tms_id IS NOT NULL
AND UPPER(v.tms_id) NOT IN ("","00000000000000","TMS-ID-UNAVAILABLE")
AND DATE(v.start_time, 'America/New_York') >= '2021-12-01' 
AND DATE(v.start_time, 'America/New_York') <= '2022-01-06' -- adjust dates for program
AND ((lower(v.program_title)) LIKE '%bowl%' )
AND (lower(v.program_title)) NOT LIKE '%bowling%'
AND LOWER(playback_type) = 'live'
AND ( LOWER(station_mapping) LIKE 'espn%' )
GROUP BY 1,2,3,4,5,6
)
, user_related2 AS (
SELECT user_id, COUNT(DISTINCT Bowl_Type) as Games,SUM(hours) as Hours, COUNT(DISTINCT user_id) AS Viewers
FROM viewership2 
WHERE Bowl_Type IS NOT NULL
GROUP BY 1
ORDER BY 1
)

,users_2021 AS (
SELECT DISTINCT
/*
CASE WHEN Games = 1 THEN '1 Game only'
WHEN Games = 2 THEN '2 Games'
WHEN Games = 3 THEN '3 Games'
WHEN Games = 4 THEN '4 Games'
WHEN Games = 5 THEN '5 Games'
WHEN Games = 6 THEN 'All Bowl Games'
END AS Games_Watched,
SUM(Hours) AS Hours, COUNT(DISTINCT user_id) as Uniques
*/
user_id AS users_2021, '21' AS year
FROM user_related2 
--GROUP BY 1
)

SELECT DISTINCT users_2021
FROM users_2020 AS t1 
JOIN users_2021 AS t2 ON t1.users_2020 = t2.users_2021

------------------------------------------------------------------------------ CFB BOWL + Championship ----------------------------------------------
WITH viewership as (
SELECT user_id,DATE_TRUNC(DATE(v.start_time, 'America/New_York'), month) as month_est,date(start_time, 'America/New_York') as date_est, v.program_title, v.episode_title, 
CASE WHEN LOWER(v.program_title) LIKE '%sugar%' THEN 'Sugar Bowl'
WHEN LOWER(v.program_title) LIKE '%fiesta%' THEN 'Fiesta Bowl'
WHEN LOWER(v.program_title) LIKE '%cotton%' THEN 'Cotton Bowl'
WHEN LOWER(v.program_title) LIKE '%orange%' THEN 'Orange Bowl'
WHEN LOWER(v.program_title) LIKE '%peach%' THEN 'Peach Bowl'
WHEN LOWER(v.program_title) LIKE '%rose%' THEN 'Rose Bowl'
WHEN LOWER(v.program_title) LIKE '%cfp national champion%' THEN 'CFP National Championship'
ELSE NULL
END AS Bowl_Type
,SUM(duration)/3600 as hours
FROM `fubotv-prod.data_insights.video_in_progress_via_va_view` v
JOIN `fubotv-prod.data_insights.tmsid_genre_mapping` m on v.tms_id =m.tms_id -- Genre Mapping Table
JOIN `fubotv-dev.business_analytics.station_mapping_table_51220` s on s.station_name =v.Channel -- Station Mapping Table
WHERE v.tms_id IS NOT NULL
AND UPPER(v.tms_id) NOT IN ("","00000000000000","TMS-ID-UNAVAILABLE")
AND DATE(v.start_time, 'America/New_York') >= '2020-12-01' -- adjust dates
AND DATE(v.start_time, 'America/New_York') <= '2021-01-31' -- adjust dates for program
AND LOWER(playback_type) = 'live'
AND ( LOWER(station_mapping) LIKE 'espn%' )
GROUP BY 1,2,3,4,5,6
)

SELECT SUM(hours) as Hours, COUNT(DISTINCT user_id) AS Viewers
FROM viewership
WHERE Bowl_Type IS NOT NULL
--GROUP BY 1
ORDER BY 1