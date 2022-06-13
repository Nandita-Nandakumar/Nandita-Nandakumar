-------------- Spanish Language Hours -----------------
WITH users as
(
Select distinct day, t2.user_id,
from `fubotv-prod.data_insights.daily_status_static_update` t1
inner join `fubotv-dev.business_analytics.account_code_mapping` t2 on t1.account_code=t2.account_code
where final_status_v2_sql like ('paid%')
and lower(plan_code) not like '%cana%'
and lower(plan_code) not like '%spain%'
),

viewership as (
SELECT DISTINCT DATE(start_time) AS Date_Utc, t1.user_id, sum(duration)/3600 as hours
from `fubotv-prod.data_insights.video_in_progress_via_va_view` t1
inner join users t2 on t1.user_id=t2.user_id AND t2.day = DATE(t1.start_time)
inner join `fubotv-prod.data_insights.tmsid_genre_mapping` t3 on t1.tms_id=t3.tms_id   --Genre Mapping Table
inner join `fubotv-dev.business_analytics.station_mapping_table_51220` t5 on t1.channel=t5.station_name -- Station Mapping Table
WHERE t3.tms_id IS NOT NULL
AND UPPER(t3.tms_id) NOT IN ("","00000000000000","TMS-ID-UNAVAILABLE")
AND language_is_es = True -- Spanish Language Program
AND DATE(start_time, "EST") >= '2022-01-01'
group by 1 ,2
)

SELECT DISTINCT
--Date_Utc, --- for Date
SUM(Hours) AS Spanish_Lang_Hours,  
COUNT(DISTINCT user_id) Spanish_Lang_Watching_User,
from viewership
--GROUP BY 1 -- uncomment this line if you need line 16