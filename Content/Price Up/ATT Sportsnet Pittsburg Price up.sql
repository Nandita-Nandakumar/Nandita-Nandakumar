--ATT Sportsnet Pittsburg Price up for Zone 1 & 2

WITH active_users as
(
Select distinct day,account_code
from `fubotv-prod.data_insights.daily_status_static_update`
where 1=1
and day = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
and final_status_v2_sql NOT IN ('expired', 'paused')
and (plan_code in ('fubo-extra','fubotv-basic') or lower(plan_code) like '%bundle%')
and lower(plan_code) not like '%latino%'
and lower(plan_code_invoice) in ('fubotv-basic','fubo-extra')
and lower(collection_method) = 'automatic'
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
active_users_exclude_coupons as 
(
Select distinct t1.account_code
from active_users t1
left outer join coupon_users t2 on t1.account_code=t2.account_code
where t2.account_code is NULL
),
final as (
Select t1.account_code, email
from active_users_exclude_coupons t1
inner join `fubotv-dev.business_analytics.email_mapping` e on t1.account_code = e.account_code
),
users as 
(
Select distinct * from final
where email not like '%@fubo.tv'
and email not like '%@apple.com'
and email not like '%@roku.com'
and email <> ""
),
--- zip codes
home_zip as (
Select id,email,home_postal,row_number() over (partition by id order by updated_at desc) as row_number 
from `fubotv-prod.dmp.de_sync_users` 
where home_postal is not NULL
and home_postal <> ''
),
current_home_zip as (
Select distinct id,email,home_postal
from home_zip
where row_number = 1
),
-----final home zip table with account code
latest_recurly_id as (
Select distinct t1.id,t2.recurly_account_id,t1.email,home_postal
from current_home_zip t1
inner join `fubotv-prod.dmp.de_sync_subscription` t2 on t1.id=t2.user_id
),
billing_zip as (
Select account_code,'unknown' as email,billing_postal_code,row_number() over (partition by account_code order by updated_at desc) as row_number
from `fubotv-prod.dmp.recurly_billing_infos` 
where billing_postal_code is not NULL
and billing_postal_code <> ''
),
latest_billing_zip as (
Select distinct account_code,email,billing_postal_code
from billing_zip
where row_number = 1
),
zip_code_file1 as (
Select distinct t1.recurly_account_id as user_id,t1.email,t1.home_postal as home_zip,billing_postal_code as billing_zip
from latest_recurly_id t1
left outer join latest_billing_zip t2 on t1.recurly_account_id=t2.account_code
),
addl_users as (
Select distinct t1.account_code,t1.email,t1.billing_postal_code
from latest_billing_zip t1
left outer join zip_code_file1 t2 on t1.account_code=t2.user_id
where t2.user_id is NULL
),
zip_code_file2 as (
Select * from zip_code_file1
union all
Select distinct account_code as user_id,email,'' as home_zip,billing_postal_code as billing_zip
from addl_users
),
final_zip_codes as
(
Select distinct user_id,email,case when home_zip is NULL then billing_zip
when home_zip = '' then billing_zip else home_zip end as home_zip,
billing_zip
from zip_code_file2
),
zip_user_match as (
Select distinct t1.account_code,t2.home_zip
from users t1
left outer join final_zip_codes t2 on t1.account_code=t2.user_id
),
pitts_match as (
Select distinct t1.account_code, t1.email
from users t1
inner join zip_user_match t2 on t1.account_code=t2.account_code
inner join `fubotv-dev.business_analytics.ATTSN Pittsburg Zip` t3 on t2.home_zip=t3.zipcode
)

Select *
from pitts_match