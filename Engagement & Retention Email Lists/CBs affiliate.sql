
-- Active english users 

with days as (
select
date("2020-9-1", 'America/New_York') as EOP
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
viewership as (
select v.playback_type, v.network_owner, s.station_mapping, v.program_title, v.tms_id, v.series_title, v.episode_title, m.primary_genre, m.primary_genre_group, v.device_category, v.user_id, v.start_time, v.league_names, date(v.start_time) as day, (select EOP from days) as EOP
from `fubotv-prod.data_insights.video_in_progress_via_va_view` v
inner join active_users_exclude_coupons t1 on t1.account_code = v.user_id
inner join `fubotv-prod.data_insights.tmsid_genre_mapping` m on v.tms_id =m.tms_id
inner join `fubotv-dev.business_analytics.station_mapping_table_51220` s on s.station_name =v.channel
where v.channel like ('%KRQEDT%') OR v.channel like ('%WIAT%') OR v.channel like ('%WIVB%') OR v.channel like ('%WCIA%') OR v.channel like ('%WOWK%') OR v.channel like ('%WHBF%') OR v.channel like ('%KGPE%') OR v.channel like ('%WANE%') OR v.channel like ('%WFRV%') OR v.channel like ('%WNCT%') OR v.channel like ('%WSPA%') OR v.channel like ('%KGBT%')
OR v.channel like ('%WJTV%') OR v.channel like ('%WTAJ%') OR v.channel like ('%KLFY%') OR v.channel like ('%WLNS%') OR v.channel like ('%KLAS%')
OR v.channel like ('%WKRG%') OR v.channel like ('%WBTW%') OR v.channel like ('%WMBD%') OR v.channel like ('%KOIN%') OR v.channel like ('%WPRI%')
OR v.channel like ('%WNCN%') OR v.channel like ('%WROC%') OR v.channel like ('%KELO%') OR v.channel like ('%KOLR%') OR v.channel like ('%WJHL%') OR v.channel like ('%WYOU%') OR v.channel like ('%WKBN%')
),
final as (
Select t1.user_id, email
from viewership t1
inner join `fubotv-dev.business_analytics.email_mapping` e on t1.user_id = e.account_code
)
select distinct * from final
where email not like '%@fubo.tv'
and email not like '%@apple.com'
and email not like '%@roku.com'
and email <> ""