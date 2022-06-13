-- Dunning period users churn count by Day

WITH Churn_users AS
  (
  SELECT DATE(current_period_started_at) AS Period_start_day, 
  DATE_ADD (DATE(current_period_started_at), INTERVAL 28 DAY) AS expired_day, 
  account_code, final_status_v2_sql
  FROM `fubotv-prod.data_insights.daily_status_static_update`   
  WHERE day = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
  AND final_status_v2_sql = 'past due'
  )
SELECT DISTINCT t1.expired_day, COUNT (t1.account_code)
FROM Churn_users t1
GROUP BY 1
ORDER BY 1;


+

-- Paid users who are churning - paid but scheduled pause, paid and cancelled

WITH Churn_users AS
(
  SELECT DATE_TRUNC(Date(current_period_ends_at),Day) AS Period_ends_day, account_code, final_status_v2_sql
  FROM `fubotv-prod.data_insights.daily_status_static_update`   
  WHERE day = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
  AND final_status_v2_sql LIKE ('%paid%')
  AND final_status_v2_sql NOT LIKE ('paid')
  AND (plan_code IN ('fubo-extra','fubotv-basic') OR LOWER(plan_code) LIKE '%bundle%')
  AND LOWER(plan_code) NOT LIKE '%latino%'
  AND LOWER(plan_code_invoice) IN ('fubotv-basic','fubo-extra')
)

Select DISTINCT t1.Period_ends_day, COUNT (t1.account_code) churn_count
FROM Churn_users t1
GROUP BY 1
ORDER BY 1