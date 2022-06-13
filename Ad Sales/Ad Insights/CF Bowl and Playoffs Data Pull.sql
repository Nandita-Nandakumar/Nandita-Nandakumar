--Report Criteria:
--Dates : 2020-12-20 to 2021-01-31
--Channels : ESPN, ABC, CBS, NFL, ACC, SEC
--Playback_Type : Live

WITH viewership as (
SELECT DATE_TRUNC(DATE(v.start_time, 'America/New_York'), month) as month_est,date(start_time, 'America/New_York') as date_est, v.program_title, v.episode_title, SUM(duration)/3600 as hours
FROM `fubotv-prod.data_insights.video_in_progress_via_va_view` v
JOIN `fubotv-prod.data_insights.tmsid_genre_mapping` m on v.tms_id =m.tms_id -- Genre Mapping Table
JOIN `fubotv-dev.business_analytics.station_mapping_table_51220` s on s.station_name =v.Channel -- Station Mapping Table
WHERE v.tms_id IS NOT NULL
AND UPPER(v.tms_id) NOT IN ("","00000000000000","TMS-ID-UNAVAILABLE")
AND DATE(v.start_time, 'America/New_York') >= '2020-12-20' 
AND DATE(v.start_time, 'America/New_York') <= '2021-01-31' -- adjust dates for program
AND ((lower(v.program_title)) LIKE '%bowl%' OR (lower(v.program_title)) LIKE '%playoff%')
AND (lower(v.program_title)) NOT LIKE '%bowling%'
AND LOWER(playback_type) = 'live'
AND ( LOWER(station_mapping) LIKE 'espn%'OR LOWER(station_mapping) LIKE '%abc%'OR LOWER(station_mapping) LIKE '%cbs%'OR LOWER(station_mapping) LIKE  'nfl%' OR LOWER(station_mapping) LIKE  'acc%' OR LOWER(station_mapping) LIKE  'sec%' )
GROUP BY 1,2,3,4
)
SELECT  program_title, episode_title,
SUM(hours) as Hours
FROM viewership
GROUP BY 1,2
ORDER BY 1