----------- NBCLX Banner Flights

#Banners ran on Homepage On Jan 29 between 10AM - 1PM EST NBC Olympics ; 5PM - 12AM EST NBC FIgure Skating Champ
#Banners ran on Homepage on Jan 30 between 11AM - 2 PM EST NBC Olympics ; 5PM - 12AM EST NBC FIgure SKating Champ
--------------

WITH viewership as (
SELECT DATETIME(start_time, 'EST') as Timestamp_Est, DATE_TRUNC(DATE(v.start_time, 'America/New_York'), month) as month_est,date(start_time, 'America/New_York') as date_est, TIME(start_time, "EST") as time_est, user_id, station_mapping, SUM(duration)/3600 as hours
FROM `fubotv-prod.data_insights.video_in_progress_via_va_view` v
JOIN `fubotv-prod.data_insights.tmsid_genre_mapping` m on v.tms_id =m.tms_id -- Genre Mapping Table
JOIN `fubotv-dev.business_analytics.station_mapping_table_51220` s on s.station_name =v.channel -- Station Mapping Table
WHERE v.tms_id IS NOT NULL
AND UPPER(v.tms_id) NOT IN ("","00000000000000","TMS-ID-UNAVAILABLE")
AND (lower(station_mapping) LIKE ('nbc local x') ) ---- Modify channel name
GROUP BY 1,2,3,4,5,6
)

SELECT 
Date_est, -- Comment this for one number
SUM(hours) as Hours, COUNT(DISTINCT user_id) as uniques
FROM viewership
WHERE (EXTRACT(HOUR FROM time_est) IN (10,11,12,13,17,18,19,20,21,22,23) AND Date_Est IN ('2022-01-29')) --- For Saturdays
OR (EXTRACT(HOUR FROM time_est) IN (11,12,13,14,17,18,19,20,21,22,23) AND Date_Est IN ('2022-01-30')) --- For Sundays
GROUP BY 1 -- comment this for one number
ORDER BY 1