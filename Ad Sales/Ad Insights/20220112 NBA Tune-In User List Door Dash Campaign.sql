-------------------------------------- For Door Dash Campaign - NBA Viewers in 2021 +6Hours --------------------------------------

WITH viewership as (
Select user_id, sum(duration)/3600 as Hours
from `fubotv-prod.data_insights.video_in_progress_via_va_view` t1
inner join `fubotv-prod.data_insights.tmsid_genre_mapping` t3 on t1.tms_id=t3.tms_id   --Genre Mapping Table
--inner join `fubotv-dev.business_analytics.AdInsertable_Networks` t4 on t1.channel=t4.channel -- Ad Insertable Table
inner join `fubotv-dev.business_analytics.station_mapping_table_51220` t5 on t1.channel=t5.station_name -- Station Mapping Table
WHERE t3.tms_id IS NOT NULL
AND UPPER(t3.tms_id) NOT IN ("","00000000000000","TMS-ID-UNAVAILABLE")
and primary_genre = 'Basketball' 
and (t1.league_names like '%NBA%' OR t1.program_title IN ('NBA Basketball'))
and lower(playback_type) = 'live'
AND DATE_TRUNC(start_time,year) = '2021-01-01' -- 2021 viewers 
group by 1
)
, users AS 
(
select  user_id, Hours
from viewership
WHERE Hours >= 6
AND user_id IS NOT NULL
)

SELECT DISTINCT user_id
FROM users t1
INNER JOIN `fubotv-prod.data_insights.daily_status_static_update` t2 ON t1.user_id = t2.account_code
WHERE day = '2022-01-11'
AND LOWER(final_status_restated) = 'paid'
order by 1 asc