with users as (
Select distinct account_code,plan_code,plan_name,plan_code_invoice,coupon_code,final_status_restated
from `fubotv-prod.data_insights.daily_status_static_update`
where day = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
and final_status_restated in ('paid but canceled','paid','paid and scheduled pause')
),
coupon_users as (
Select distinct account_code
from `fubotv-prod.data_insights.daily_status_static_update`
where day = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
and (lower(coupon_code) like '%csteam%'
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
  or lower(coupon_code) like '%valencia1month%')
),
final_users_temp as (
Select distinct t1.account_code,plan_code,plan_name,plan_code_invoice,coupon_code,final_status_restated
from users t1
left outer join coupon_users t2 on t1.account_code=t2.account_code
where t2.account_code is NULL
),
final_users as (
Select distinct t1.account_code,t2.email,t1.plan_code,t1.plan_name,t1.plan_code_invoice,t1.coupon_code,t1.final_status_restated
from final_users_temp t1
left outer join `fubotv-dev.business_analytics.email_mapping` t2 on t1.account_code=t2.account_code
where email not like '%@fubo.tv%'
),
entry_order as (
Select t1.account_code,t1.email,t1.plan_code as Plan_Code,t1.plan_name as Plan_Name,t1.plan_code_invoice,t1.coupon_code,t1.final_status_restated,t2.*,
row_number() over (partition by account_code order by current_period_started_at desc) as entry_row
from final_users t1
inner join `fubotv-dev.data_insights.subscription_addon_prices_wide_temp` t2 on t1.account_code=t2.recurly_account_id
),
recent_entry as (
Select * from entry_order
where entry_row = 1
)
Select * from recent_entry