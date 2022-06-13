With spain_users as
(
Select distinct account_code,day
from `fubotv-prod.data_insights.daily_status_static_update` t1
where lower(plan_code) like '%spain%'
and final_status_v2_sql in ('paid','paid but canceled','past due','open','paid and scheduled pause', 'in trial')
and day >= '2019-12-01'
and day <= '2020-12-31'
),
viewership as 
(
select v.playback_type, v.device_category, v.channel, t2.primary_genre, v.user_id, date(v.start_time) as day, v.duration
from `fubotv-prod.data_insights.video_in_progress_via_va_view` v
inner join `fubotv-prod.data_insights.tmsid_genre_mapping` t2 on v.tms_id =t2.tms_id 
inner join `fubotv-dev.business_analytics.spain_channels` t3 on v.channel =t3.channel 
where v.tms_id IS NOT NULL 
AND UPPER(v.tms_id) NOT IN ("","00000000000000","TMS-ID-UNAVAILABLE")
AND date(v.start_time) >= '2019-12-01' 
AND date(v.start_time) <= '2020-12-31'
),
viewership_combined as
(
Select DATE_TRUNC(v.day, Month) AS Month, channel, primary_genre, playback_type,device_category, 
COUNT(DISTINCT user_id) AS Uniques, SUM(v.duration)/3600 AS Hours
From viewership v
inner join spain_users s on s.account_code = v.user_id and s.day = v.day
GROUP BY 1,2,3,4,5
ORDER BY 1
),
Spain_users1 as
(
Select date_trunc(day, Year) as Year, count(distinct(account_code)) as users1
from spain_users
group by 1
order by 1
)

--viewership details
Select * 
from viewership_combined 

/*
select * from spain_users1 
*/
