-------------------------------------- Summer Olympics - Current subs who watched 5+ minutes of the Summer Olympics --------------------------------------

WITH viewership as (
Select user_id, SUM(duration) as secs
from `fubotv-prod.data_insights.video_in_progress_via_va_view` t1
inner join `fubotv-prod.data_insights.tmsid_genre_mapping` t3 on t1.tms_id=t3.tms_id   --Genre Mapping Table
inner join `fubotv-dev.business_analytics.station_mapping_table_51220` t5 on t1.channel=t5.station_name -- Station Mapping Table
WHERE t3.tms_id IS NOT NULL
AND UPPER(t3.tms_id) NOT IN ("","00000000000000","TMS-ID-UNAVAILABLE")
AND LOWER(t3.primary_genre) LIKE ('%olympics%')
and lower(playback_type) = 'live'
AND DATE(start_time,"EST") >= '2021-07-20' -- Dates of olympics
AND DATE(start_time,"EST") <= '2021-08-10'
group by 1
)
, users AS 
(
select  user_id, secs
from viewership
WHERE secs >= 300
AND user_id IS NOT NULL
)

SELECT DISTINCT user_id
FROM users t1
INNER JOIN `fubotv-prod.data_insights.daily_status_static_update` t2 ON t1.user_id = t2.account_code
WHERE day = '2022-01-20'
AND LOWER(final_status_restated) = 'paid'
order by 1 asc