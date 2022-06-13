SELECT DISTINCT t1.account_code, t2.email
FROM `fubotv-prod.data_insights.daily_status_static_update` t1
INNER JOIN `fubotv-dev.business_analytics.email_mapping` t2 on t1.account_code = t2.account_code
WHERE 1=1
AND user_type = 'subs'
AND LOWER(plan_code) LIKE ('%elite%')
AND LOWER(plan_code) LIKE ('%bundle%')
AND sub_first_dt >= '2021-03-13'
AND sub_first_dt <= '2021-03-31'
and activated_type LIKE '%new%'