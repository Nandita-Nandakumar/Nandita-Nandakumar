-- content team ask on whats the viewership KPIs for fubo cycling only who doesn't watch any of the ISP

with active_users as
(
Select distinct day,account_code,plan_code, plan_or_add_on_code , e.email
from `fubotv-prod.data_insights.daily_status_add_ons_static_update`
inner join `fubotv-dev.business_analytics.email_mapping` e using(account_code)
where 1=1
and day >= '2019-10-01'
and day < '2020-10-01'
and final_status_v2_sql in ('paid','paid but canceled','past due','open','paid and scheduled pause')
and plan_or_add_on_code = 'cycling'
),
isp_users as
(
Select distinct day,account_code, plan_code, plan_or_add_on_code , e.email
from `fubotv-prod.data_insights.daily_status_add_ons_static_update`
inner join `fubotv-dev.business_analytics.email_mapping` e using(account_code)
where 1=1
and day >= '2019-10-01'
and day < '2020-10-01'
and final_status_v2_sql in ('paid','paid but canceled','past due','open','paid and scheduled pause')
and plan_or_add_on_code = 'intl-sports-plus'
),
cycling_only_users as
(
Select distinct a.day,a.account_code,a.plan_code, a.plan_or_add_on_code , a.email
FROM active_users a
LEFT OUTER JOIN isp_users i using (account_code)
WHERE i.account_code is null
),
viewership as (
select DATE_TRUNC(DATE(v.start_time, 'America/New_York'), month) as month, s.station_mapping, v.user_id, v.duration, v.start_time
from `fubotv-prod.data_insights.video_in_progress_via_va_view` v
inner join `fubotv-dev.business_analytics.station_mapping_table_51220` s on s.station_name =v.channel
where v.tms_id IS NOT NULL
AND UPPER(v.tms_id) NOT IN ("","00000000000000","TMS-ID-UNAVAILABLE")
and DATE(v.start_time, 'America/New_York') >= '2019-10-01'
and DATE(v.start_time, 'America/New_York') < '2020-10-01'
and station_mapping in ('FOX Soccer Plus', 'GolTV (English)','GolTV (Spanish)','TyC Sports')
)
Select t3.month, station_mapping,t1.account_code, t1.plan_or_add_on_code , sum(duration)/3600 as Hours
from cycling_only_users t1
inner join `fubotv-dev.business_analytics.account_code_mapping` t2 on t1.account_code=t2.user_id
inner join viewership t3 on t2.user_id=t3.user_id and t1.day = date(t3.start_time)
group by 1,2,3,4