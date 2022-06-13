
-- Active English users paid in the last 30 days

with days as (
select
date("2020-9-1", 'America/New_York') as EOP,
date("2019-9-1", 'America/New_York')as BOP
),
active_users as
(
Select distinct day,account_code, (select EOP from days) as EOP
from `fubotv-prod.data_insights.daily_status_static_update` 
where 1=1
and day = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
and final_status_restated in ('paid','paid but canceled','paid and scheduled pause')--'open','past due')
and (plan_code in ('fubo-extra','fubotv-basic') or lower(plan_code) like '%bundle%')
and lower(plan_code) not like '%latino%'
and lower(plan_code_invoice) in ('fubotv-basic','fubo-extra')
),
coupon_users as (
Select distinct account_code,coupon_code
from `fubotv-prod.data_insights.daily_status_static_update`
where (lower(coupon_code) like '%csteam%'
  or lower(coupon_code) like '%aff201%'
  or lower(coupon_code) like '%voxmedia%'
  or lower(coupon_code) like '%bengrad%'
  or lower(coupon_code) like '%bd%'
  or lower(coupon_code) like '%nctcpartner%'
  or lower(coupon_code) like '%mincoupon%'
  or lower(coupon_code) like '%1yearofffubo%'
  or lower(coupon_code) like '%fubotvemployee%'
  or lower(coupon_code) like '%nbcdemoaccounts%'
  or lower(coupon_code) like '%testing100off%'
  or lower(coupon_code) like '%fan100%'
  or lower(coupon_code) like '%partnergift%'
  or lower(coupon_code) like '%fansided1year%'
  or lower(coupon_code) like '%qaaccounts%'
  or lower(coupon_code) like '%fubotvemployee%'
  or lower(coupon_code) like '%pressaccount%'
  or lower(coupon_code) like '%bbpartners%'
  or lower(coupon_code) like '%pass4revolt%'
  or lower(coupon_code) like '%internalandtesting%'
  or lower(coupon_code) like '%fubo100%'  
  or lower(coupon_code) like '%complimentary%'
  or lower(coupon_code) like '%compedspain%'
  or lower(coupon_code) like '%alabrad%'
  or lower(coupon_code) like '%retention%'
  or lower(coupon_code) like '%winback%'
  or lower(coupon_code) like '%ret%')
),
active_users_exclude_coupons as (
Select distinct t1.account_code
from active_users t1
left outer join coupon_users t2 on t1.account_code=t2.account_code
where t2.account_code is NULL
),
paying_days as (
select  account_code,count(distinct day) as billing_days 
from `fubotv-prod.data_insights.daily_status_static_update` 
where 1=1
and day >= (select BOP from days)
and day < (select EOP from days)
and final_status_restated in ('paid','paid but canceled','paid and scheduled pause')--'open','past due')
and (plan_code in ('fubo-extra','fubotv-basic') or lower(plan_code) like '%bundle%')
and lower(plan_code) not like '%latino%'
and lower(plan_code_invoice) in ('fubotv-basic','fubo-extra')
group by 1
),
paying_days_2 as (
select  distinct account_code, day
from `fubotv-prod.data_insights.daily_status_static_update` 
where 1=1
and day >= (select BOP from days)
and day < (select EOP from days)
and final_status_restated in ('paid','paid but canceled','paid and scheduled pause')--'open','past due')
and (plan_code in ('fubo-extra','fubotv-basic') or lower(plan_code) like '%bundle%')
and lower(plan_code) not like '%latino%'
and lower(plan_code_invoice) in ('fubotv-basic','fubo-extra')
),
asset_table as (
select asset_id, duration, ROW_NUMBER() OVER (PARTITION BY asset_id ORDER BY kg_modified_timestamp DESC) as rank
from `fubotv-prod.dmp.content_asset_details`
),
viewership as (
select v.playback_type, v.network_owner, s.station_mapping, v.program_title, v.tms_id, v.series_title, v.episode_title, m.primary_genre, m.primary_genre_group, v.device_category, v.user_id, v.start_time, a.asset_id, case when v.duration > cast(a.duration as INT64)*60 then cast(a.duration as INT64)*60 else v.duration end as duration, v.league_names, date(v.start_time) as day, (select EOP from days) as EOP 
from `fubotv-prod.data_insights.video_in_progress_via_va_view` v
inner join paying_days_2 t2 on v.user_id = t2.account_code AND date(v.start_time) = t2.day
inner join `fubotv-prod.data_insights.tmsid_genre_mapping` m on v.tms_id =m.tms_id 
inner join `fubotv-dev.business_analytics.station_mapping_table_51220` s on s.station_name =v.channel 
inner join asset_table a on v.asset_id = a.asset_id
where v.start_time >=TIMESTAMP((select BOP from days))
AND v.start_time < TIMESTAMP((select EOP from days))
AND a.rank = 1
AND lower(playback_type) NOT LIKE ('%preview%')
),
-- hours & uniques
hours_uniques as 
(select t1.EOP, user_id, sum(duration)/3600 as hours
from viewership t1
inner join active_users_exclude_coupons t2 on t1.user_id = t2.account_code
group by 1,2
),
hours_uniques_2 as 
(select t1.EOP, user_id, t3.billing_days, hours
from hours_uniques t1
left outer join paying_days t3 on t1.user_id = t3.account_code
),
final as
(
Select t1.*, email from hours_uniques_2 t1
inner join `fubotv-dev.business_analytics.email_mapping` e on t1.user_id = e.account_code
order by 3 desc
)

select * from final 
where email not like '%@fubo.tv'
and email not like '%@apple.com'
and email not like '%@roku.com'
and email <> ""
