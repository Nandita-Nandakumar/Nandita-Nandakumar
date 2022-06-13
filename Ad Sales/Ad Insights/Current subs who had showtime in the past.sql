---------------------------------------------------- CURRENT SUBS WITH SHOWTIME ADD-ON IN THE PAST ----------------------------------------------
WITH current_subs AS (
Select distinct t2.user_id
from `fubotv-prod.data_insights.daily_status_static_update` t1
inner join `fubotv-dev.business_analytics.account_code_mapping` t2 on t1.account_code=t2.account_code
where final_status_v2_sql like ('%paid%')
and lower(plan_code) not like '%cana%'
and lower(plan_code) not like '%spain%'
and lower(plan_code) NOT LIKE ('%latino%')
and lower(add_ons) not like ('%showtime%')
and day = '2022-03-30'
)

---- had or has showtime
, showtime_subs AS 
(
Select distinct user_id
from `fubotv-prod.data_insights.daily_status_static_update` t1
inner join `fubotv-dev.business_analytics.account_code_mapping` t2 on t1.account_code=t2.account_code
where final_status_v2_sql like ('%paid%')
and lower(plan_code) not like '%cana%'
and lower(plan_code) not like '%spain%'
and lower(add_ons) like ('%showtime%')
and day >= '2021-01-01'
and day <= '2022-03-30'
)

SELECT DISTINCT t1.user_id
FROM current_subs t1
JOIN showtime_subs t2 ON t1.user_id = t2.user_id