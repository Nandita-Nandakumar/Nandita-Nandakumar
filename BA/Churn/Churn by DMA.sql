with bom_subs as (
Select distinct account_code 
from `fubotv-prod.data_insights.daily_status_static_update`
where final_status_v2_sql like '%paid%'
and day = '2020-08-31' -- for churn from sept, the bom subs will be last day pf prev month #s
and (plan_code in ('fubo-extra','fubotv-basic') or lower(plan_code) like '%bundle%')
and lower(plan_code) not like '%latino%'
and lower(plan_code_invoice) in ('fubotv-basic','fubo-extra')
),
-- make these 3 comments when running to totals BOM #s
expires as (
Select distinct t1.account_code,day as expired_day 
from `fubotv-prod.data_insights.daily_status_static_update` t1
inner join bom_subs t2 on t1.account_code=t2.account_code
where final_status_v2_sql = 'expired'
and expired_from_sql = 'subs'
and day >= '2020-09-01'
),
paused as (
SELECT day as paused_day,t1.account_code
FROM `fubotv-prod.data_insights.daily_status_static_update` t1
inner join bom_subs t2 on t1.account_code=t2.account_code
where final_status_restated IN ('paused')
and day = DATE(paused_at)
and day >= '2020-09-01'
),
expired_and_paused as (
Select distinct account_code
from expires
union all
select distinct account_code
from paused
),

-- Comment until here for totals

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
Select distinct account_code,home_zip
from expired_and_paused t1 -- make this bom_sept when counting totals
left outer join final_zip_codes t2 on t1.account_code=t2.user_id
)
-- , dma zip as (
Select distinct dma_name,account_code as users
from zip_user_match t1
left outer join `fubotv-dev.business_analytics.DMA_by_ZIP` t2 on t1.home_zip=t2.zip