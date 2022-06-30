----- Monthly Churn Projection
with start_month1 as (
Select distinct account_code,date_trunc(sub_first_dt,MONTH) as start_month
from `fubotv-prod.data_insights.daily_status_static_update`
where day = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
and final_status_restated like '%paid%'
and LOWER(plan_code) NOT LIKE '%spain%'
)
Select start_month,count(distinct account_code) as users
from start_month1
group by 1
order by 1 asc
