-- Churned Users by Sub start month

with expired as (
Select distinct account_code, day as expired_day 
from `fubotv-prod.data_insights.daily_status_static_update`
where final_status_v2_sql = 'expired'
and expired_from_sql = 'subs'
and day >= '2021-03-01' -- always beginning of month
),

paused as (
SELECT day as paused_day, account_code
FROM `fubotv-prod.data_insights.daily_status_static_update`
where final_status_v2_sql = 'paused'
and day = DATE(paused_at)
and day >= '2021-03-01' -- always beginning of month
),

expired_and_paused as (
Select distinct account_code
from expired
union all
select distinct account_code
from paused
),

sub_first_month as (
Select distinct t1.account_code,date_trunc(sub_first_dt,MONTH) as start_month
from expired_and_paused t1
inner join `fubotv-prod.data_insights.daily_status_static_update` t2 on t1.account_code=t2.account_code
)

Select start_month,count(distinct account_code) as churned_users
from sub_first_month
group by 1
order by 1 asc

-- if need to compare with users in BOM


with users1 as (
Select distinct account_code,date_trunc(sub_first_dt,MONTH) as start_month
from `fubotv-prod.data_insights.daily_status_static_update`
where day = '2021-03-24'
and final_status_restated like '%paid%'
)
Select start_month,count(distinct account_code) as users
from users1
group by 1
order by 1 asc
