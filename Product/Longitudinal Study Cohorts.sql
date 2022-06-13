/* Longitudinal Study of Cohorts - for Kent Mark 

/* anyone started trial on the 1st nov 2020 who are still in trial on 2nd */

SELECT s.account_code, e.email
FROM `fubotv-prod.data_insights.daily_status_static_update` s
Inner Join `fubotv-dev.business_analytics.email_mapping` e on t1.account_code=e.account_code
Where e.email not like '%@fubo.tv'
AND e.email not like '%@apple.com'
AND e.email not like '%@roku.com'
AND e.email is not null
AND e.email <> ""
AND date(trial_started_at) = '2020-10-28' -- change to 2020-11-01
AND day = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
AND final_status_v2_sql IN ('in trial')
AND (plan_code in ('fubo-extra','fubotv-basic') OR lower(plan_code) like '%bundle%')
AND lower(plan_code) not like '%latino%'
AND lower(plan_code_invoice) in ('fubotv-basic','fubo-extra')


/* 6 months users cohort */

SELECT account_code
FROM `fubotv-prod.data_insights.daily_status_static_update`
WHERE day = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
AND final_status_v2_sql IN ('paid')
AND (plan_code in ('fubo-extra','fubotv-basic') OR lower(plan_code) like '%bundle%')
AND LOWER(plan_code) not like '%latino%'
AND DATE_TRUNC(sub_first_dt,MONTH) IN ('2020-02-01', '2020-03-01', '2020-04-01')


/* End of M1 and Beginning of M2 */

with users
AS 
(
    Select distinct t1.day,t1.account_code
    FROM `fubotv-prod.data_insights.daily_status_static_update` t1
    WHERE day = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
    AND sub_nth_month_by_firstlast_plan_sql=1
    AND date_diff(date(current_period_ends_at), current_date(), DAY) <= 5
    AND final_status_v2_sql = ('paid')
    AND (plan_code IN ('fubo-extra','fubotv-basic') OR lower(plan_code) LIKE '%bundle%')
    AND lower(plan_code) NOT LIKE '%latino%'
    AND lower(plan_code_invoice) IN ('fubotv-basic','fubo-extra')
    
    UNION ALL

    SELECT distinct t1.day,t1.account_code
    FROM `fubotv-prod.data_insights.daily_status_static_update` t1
    WHERE day = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
    AND sub_nth_month_by_firstlast_plan_sql=2
AND date_diff(current_date(),date(current_period_started_at), DAY) <= 5
AND final_status_v2_sql = ('paid')
AND (plan_code IN ('fubo-extra','fubotv-basic') OR lower(plan_code) LIKE '%bundle%')
AND lower(plan_code) NOT LIKE '%latino%'
AND lower(plan_code_invoice) IN ('fubotv-basic','fubo-extra'))

Select t1.account_code, e.email
from users t1
Inner Join `fubotv-dev.business_analytics.email_mapping` e on t1.account_code=e.account_code
Where e.email not like '%@fubo.tv'
AND e.email not like '%@apple.com'
AND e.email not like '%@roku.com'
AND e.email is not null
AND e.email <> ""
ORDER BY RAND()
LIMIT 35000
