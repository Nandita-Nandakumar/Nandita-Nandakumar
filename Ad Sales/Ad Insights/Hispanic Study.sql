---------------------------------------------------- HISPANIC STUDY INCLUDING SPORTS ----------------------------------------------
with users as
(
Select distinct t2.user_id,
from `fubotv-prod.data_insights.daily_status_static_update` t1
inner join `fubotv-dev.business_analytics.account_code_mapping` t2 on t1.account_code=t2.account_code
where final_status_v2_sql like ('%paid%')
and lower(plan_code) not like '%cana%'
and lower(plan_code) not like '%spain%'
and day = '2022-02-22'
),

viewership as (
SELECT DISTINCT user_id, device_category, hours
FROM (
SELECT DISTINCT t1.user_id, device_category, sum(duration)/3600 as hours
from `fubotv-prod.data_insights.video_in_progress_via_va_view` t1
inner join users t2 on t1.user_id=t2.user_id
inner join `fubotv-prod.data_insights.tmsid_genre_mapping` t3 on t1.tms_id=t3.tms_id   --Genre Mapping Table
inner join `fubotv-dev.business_analytics.station_mapping_table_51220` t5 on t1.channel=t5.station_name -- Station Mapping Table
WHERE t3.tms_id IS NOT NULL
AND UPPER(t3.tms_id) NOT IN ("","00000000000000","TMS-ID-UNAVAILABLE")
AND language_is_es = True
AND DATE(start_time, "EST") >= '2021-01-01'
AND DATE(start_time, "EST") <= '2021-12-31'
AND duration >= 18000
--AND primary_genre_group = 'Sports'
-- and lower(playback_type) = 'live'
group by 1 ,2 --,3,4,5
)

)
select device_category, SUM(Hours) AS Spanish_Lang_Hours,  COUNT(DISTINCT user_id) Spanish_Lang_Watching_User,
from viewership
GROUP BY 1
ORDER BY 2 DESC

