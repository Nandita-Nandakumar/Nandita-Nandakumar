/* NN Ad Sales X Viwership Dash 
 Viewership Part of query */

with users as (
Select distinct t1.day,t1.account_code,t2.user_id
from `fubotv-prod.data_insights.daily_status_static_update` t1
inner join `fubotv-dev.business_analytics.account_code_mapping` t2 on t1.account_code=t2.account_code
where final_status_v2_sql not in ('expired','paused','past due from trials and scheduled pause','failed and scheduled pause')
and lower(plan_code) not like '%canadian%'
and lower(plan_code) not like '%spain%'
)

Select
t2.day as Date_range, t5.station_mapping as Channel_Name, LOWER(t4.ad_insertable) AS ad_insertable_network, SUM(duration)/3600 as Hours, COUNT(DISTINCT(t1.user_id)) AS Uniques, COUNT(distinct(t1.start_time)) as Streams
from `fubotv-prod.data_insights.video_in_progress_via_va_view` t1
inner join users t2 on t1.user_id=t2.user_id and date(t1.start_time)=t2.day
inner join `fubotv-prod.data_insights.tmsid_genre_mapping` t3 on t1.tms_id=t3.tms_id
inner join `fubotv-dev.business_analytics.AdInsertable_Networks` t4 on t1.channel=t4.channel
inner join `fubotv-dev.business_analytics.station_mapping_table_51220` t5 ON t5.station_name = t1.channel
WHERE t3.tms_id IS NOT NULL
AND UPPER(t3.tms_id) NOT IN ("","00000000000000","TMS-ID-UNAVAILABLE")
and date(start_time) >= '2021-01-01'
and date(start_time) <= '2021-04-13'
group by 1,2,3
order by 1 DESC