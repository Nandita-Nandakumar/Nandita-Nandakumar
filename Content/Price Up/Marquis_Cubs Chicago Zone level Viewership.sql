-- Chicago Cubs Chicago zone level Viewership in 2019

with users as (
Select distinct date_trunc(day,MONTH) as monthx,day,account_code 
from `fubotv-prod.data_insights.daily_status_static_update`
where (plan_code in ('fubo-extra','fubotv-basic') or lower(plan_code) like '%bundle%')
and lower(plan_code) not like '%latino%'
and lower(plan_code_invoice) in ('fubotv-basic','fubo-extra')
and day >= '2019-04-01'
and day < '2019-10-01'
and final_status_restated like '%paid%'
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
Select distinct account_code,home_zip
from users t1
left outer join final_zip_codes t2 on t1.account_code=t2.user_id
),
chicago_match as (
Select distinct t1.monthx,t1.day,t3.zone,t1.account_code,t2.home_zip
from users t1
inner join zip_user_match t2 on t1.account_code=t2.account_code
inner join `fubotv-dev.business_analytics.chicago_zips_by_zone` t3 on t2.home_zip=t3.chicago_zip
),
-- avg monthly user
chicago_match_daily as
(
Select day, monthx ,zone, count(distinct(account_code)) as users1
from chicago_match
group by 1,2,3
),
viewership as (
Select monthx,zone,sum(duration)/3600 as viewing_hours,count(distinct t1.user_id) as viewing_uniques
from `fubotv-prod.data_insights.video_in_progress_via_va_view` t1
inner join chicago_match t2 on t1.user_id=t2.account_code and date(t1.start_time)=t2.day
where channel in ('NBC Sports Chicago','NBC Sports Chicago Plus')
and playback_type = 'live'
and (lower(program_title) like '%cubs%' or lower(episode_title) like '%cubs%') 
group by 1,2
)

-- for avg subs per month by zone
Select monthx, zone, avg(users1) as cvg_subs
from chicago_match_daily
Group by 1,2
order by 1 asc

/* 
-- for uniques, hours and other viewership related pull
Select * 
from viewership
order by monthx asc
*/