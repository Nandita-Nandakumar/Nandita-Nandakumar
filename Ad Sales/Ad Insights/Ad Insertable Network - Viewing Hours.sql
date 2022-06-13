with users as (
Select distinct t1.day,t1.account_code,t2.user_id,
case when t1.final_status_v2_sql IN ('in trial', 'past due from trials') then 'Trial User'
else 'Paying User' end as user_type
from `fubotv-prod.data_insights.daily_status_static_update` t1
inner join `fubotv-dev.business_analytics.account_code_mapping` t2 on t1.account_code=t2.account_code
where final_status_v2_sql not in ('expired','paused','past due from trials and scheduled pause','failed and scheduled pause')
and lower(plan_code) not like '%canadian%'
and lower(plan_code) not like '%spain%'
and day >= '2021-04-29' ----adjust dates here
and day <= '2021-05-10'
),
viewership as (
Select t2.day as streaming_day,station_mapping,lower(t4.ad_insertable) as ad_insertable, t4.channel, user_type, sum(duration)/3600 as hours, count(distinct t1.user_id) as uniques
from `fubotv-prod.data_insights.video_in_progress_via_va_view` t1
inner join users t2 on t1.user_id=t2.user_id and date(t1.start_time)=t2.day
inner join `fubotv-prod.data_insights.tmsid_genre_mapping` t3 on t1.tms_id=t3.tms_id   --Genre Mapping Table
inner join `fubotv-dev.business_analytics.AdInsertable_Networks` t4 on t1.channel=t4.channel -- Ad Insertable Table
inner join `fubotv-dev.business_analytics.station_mapping_table_51220` t5 on t1.channel=t5.station_name -- Station Mapping Table
WHERE t3.tms_id IS NOT NULL
AND LOWER(ad_insertable) = 'yes'
AND UPPER(t3.tms_id) NOT IN ("","00000000000000","TMS-ID-UNAVAILABLE")
group by 1,2,3,4,5
)
select streaming_day, --channel, 
sum(hours) as hours
from viewership
group by 1
order by 1 asc