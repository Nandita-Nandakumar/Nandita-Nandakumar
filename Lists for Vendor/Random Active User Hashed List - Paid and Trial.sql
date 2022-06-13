-- Active Trial User - Random List

WITH active_trial_users AS 
  (
  SELECT DISTINCT day,account_code
  FROM `fubotv-prod.data_insights.daily_status_static_update` 
  WHERE day = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
  AND final_status_v2_sql = ('in trial')
  AND (plan_code IN ('fubo-extra','fubotv-basic') OR LOWER(plan_code) LIKE '%bundle%')
  AND LOWER(plan_code) NOT LIKE '%latino%'
  AND LOWER(plan_code_invoice) IN ('fubotv-basic','fubo-extra')
  ORDER BY RAND()
  LIMIT 20000
  )
SELECT TO_HEX(MD5(LOWER(t2.email)))
FROM active_trial_users t1
INNER JOIN `fubotv-dev.business_analytics.email_mapping` t2 
ON t1.account_code = t2.account_code;


-- Active Paid User - Random List

WITH active_paid_users AS 
  (
  SELECT DISTINCT day,account_code
  FROM `fubotv-prod.data_insights.daily_status_static_update` 
  WHERE day = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
  AND final_status_v2_sql LIKE ('%paid%')
  AND (plan_code IN ('fubo-extra','fubotv-basic') OR LOWER(plan_code) LIKE '%bundle%')
  AND LOWER(plan_code) NOT LIKE '%latino%'
  AND LOWER(plan_code_invoice) IN ('fubotv-basic','fubo-extra')
  ORDER BY RAND()
  LIMIT 20000
  )
SELECT TO_HEX(MD5(LOWER(t2.email)))
FROM active_paid_users t1
INNER JOIN `fubotv-dev.business_analytics.email_mapping` t2 
ON t1.account_code = t2.account_code; 