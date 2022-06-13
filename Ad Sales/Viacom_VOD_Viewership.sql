-- Monthly Viacom Vod users uniques and duration by channel and device type

with viewership as (
select v.playback_type, s.station_name, v.device_category, v.user_id, date(v.start_time) as day, SUM(v.duration)/3600 AS Total_Duration
from `fubotv-prod.data_insights.video_in_progress_via_va_view` v
inner join `fubotv-dev.business_analytics.station_mapping_table_51220` s on s.station_name =v.channel
WHERE lower(s.network_owner) = 'viacom'
AND lower(v.playback_type) = 'vod'
AND date(v.start_time) >= '2019-09-01' 
AND date(v.start_time) < '2020-09-01'
GROUP BY 1,2,3,4,5
)
Select DATE_TRUNC(day, Month) AS Month, station_name, device_category, COUNT(DISTINCT user_id) AS uniques, SUM(Total_Duration) AS Duration
From viewership
GROUP BY 1,2,3