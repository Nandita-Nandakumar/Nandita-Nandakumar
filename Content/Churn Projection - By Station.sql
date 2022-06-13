# Churn Projection By Network / Station
# Edit 4 places
with days as (
select
date("2022-06-01", 'America/New_York') as EOP, --Edit
date("2021-05-31", 'America/New_York')as BOP --Edit
),
daily_paying_users as
(
Select distinct account_code,coupon_code
from `fubotv-prod.data_insights.daily_status_static_update` 
where 1=1
and day = (select EOP from days)
and final_status_v2_sql like ('paid%')
and (plan_code in ('fubotv-basic','fubo-extra')
or plan_code like '%Bundle%' or plan_code like '%quarter%' or plan_code like '%qrtr%')
and lower(plan_code) not like '%latino%'
and lower(plan_code) not like '%canadian%'
and lower(plan_code) not like '%spain%'
and lower(plan_code_invoice) in ('fubotv-basic','fubo-extra','fubotv-basic-qrtr')
),
daily_paying_users_2 as
(
Select distinct day,account_code,coupon_code
from `fubotv-prod.data_insights.daily_status_static_update` 
where 1=1
and day >= (select BOP from days)
and day <= (select EOP from days)
and final_status_v2_sql like ('paid%')
and (plan_code in ('fubotv-basic','fubo-extra')
or plan_code like '%Bundle%' or plan_code like '%quarter%' or plan_code like '%qrtr%')
and lower(plan_code) not like '%latino%'
and lower(plan_code) not like '%canadian%'
and lower(plan_code) not like '%spain%'
and lower(plan_code_invoice) in ('fubotv-basic','fubo-extra','fubotv-basic-qrtr')
),
net_hours as
(
Select t1.account_code, sum(duration)/3600 as net_hours
from `fubotv-prod.data_insights.video_in_progress_via_va_view` t2 
inner join daily_paying_users t1 on t1.account_code = t2.user_id
where 1=1
and lower(channel) IN ('vsin', 'tvg', 'tvg2') -- edit
and date(start_time) >= (select BOP from days)
and date(start_time) <= (select EOP from days)
group by 1
),
all_streaming_hours as
(
Select t1.account_code, sum(duration)/3600 as fubo_hours
from `fubotv-prod.data_insights.video_in_progress_via_va_view` t2
inner join daily_paying_users t1 on t1.account_code = t2.user_id
where 1=1
and date(start_time) >= (select BOP from days)
and date(start_time) <= (select EOP from days)
group by 1
),
days_viewing as
(
Select date(start_time) as days_watched_all,t1.account_code, sum(duration)/3600 as net_hours
from `fubotv-prod.data_insights.video_in_progress_via_va_view` t2 
inner join daily_paying_users_2 t1 on t1.account_code = t2.user_id and date(t2.start_time) = t1.day
where 1=1
and date(start_time) >= (select BOP from days)
and date(start_time) <= (select EOP from days)
group by 1,2
),
days_viewing_2 as 
( select account_code, count(distinct days_watched_all) as days_watched_all_ttm
from days_viewing
GROUP BY 1
),
days_station as
(
Select date(start_time) as days_watched,t1.account_code, sum(duration)/3600 as net_hours
from `fubotv-prod.data_insights.video_in_progress_via_va_view` t2 
inner join daily_paying_users_2 t1 on t1.account_code = t2.user_id and date(t2.start_time) = t1.day
where 1=1
and lower(channel) IN ('vsin', 'tvg', 'tvg2') --- Edit
and date(start_time) >= (select BOP from days)
and date(start_time) <= (select EOP from days)
group by 1,2
),
days_station_2 as 
( select account_code, count(distinct days_watched) as days_watched_station_ttm
from days_station
GROUP BY 1
),
total_paying_days as 
( select account_code, count(distinct day) as total_paying_days
from daily_paying_users_2
GROUP BY 1
),
combined as 
( select t1.account_code, days_watched_station_ttm,days_watched_all_ttm, total_paying_days, days_watched_station_ttm/total_paying_days as perc_of_paying_days_watched, days_watched_station_ttm/days_watched_all_ttm as perc_of_viewing_days_watched,
from days_station_2 t1
inner join total_paying_days t2 on t2.account_code = t1.account_code
inner join days_viewing_2 t3 on t3.account_code = t1.account_code
),
final as (
Select distinct t1.account_code as users, net_hours, fubo_hours, net_hours/fubo_hours*100 as share_net, days_watched_station_ttm,days_watched_all_ttm, total_paying_days,perc_of_paying_days_watched,perc_of_viewing_days_watched
from all_streaming_hours t1
left outer join net_hours t2 on t1.account_code=t2.account_code
left outer join combined t3 on t3.account_code = t1.account_code
where net_hours is not null
)
select *, 
case 
  when round(net_hours,0) <=1 then "Remove" 
    else "Keep" 
  end as R_K, 
case 
  when round(net_hours,0) = 0 then "'0-1" 
  when round(net_hours,0) <= 5 then "'1-5" 
  when round(net_hours,0) <= 10 then "'5-10"
  when round(net_hours,0) <= 15 then "'10-15" 
  when round(net_hours,0) <= 20 then "'15-20" 
  when round(net_hours,0) <= 30 then "'20-30" 
  when round(net_hours,0) <= 40 then "'30-40" 
    else "'40+" 
  end as bucket,
case 
  when round(share_net,0) = 0 then "'0%" 
  when round(share_net,0) <= 5 then "'1-5%" 
  when round(share_net,0) <= 10 then "'5-10%" 
  when round(share_net,0) <= 25 then "'10-25%" 
  when round(share_net,0) <= 50 then "'25-50%" 
  when round(share_net,0) <= 75 then "'50-75%" 
  when round(share_net,0) <= 85 then "'75-85%"
  when round(share_net,0) <= 95 then "'85-95%"
    else "'95-100%" 
  end as share_bucket
from final