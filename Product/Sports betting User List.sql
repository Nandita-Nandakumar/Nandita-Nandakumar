-- Product team query : Random list of 20,000 English and Latino Paid users who watched sports in the last 30 days. For Sports betting. Ran this on 11/03

WITH paid_users AS 
  (
  SELECT DISTINCT s.account_code, e.email
  FROM `fubotv-prod.data_insights.daily_status_static_update` s
  INNER JOIN  `fubotv-dev.business_analytics.email_mapping` e 
  ON s.account_code = e.account_code
  WHERE day = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
  AND final_status_v2_sql IN ('paid')
  AND (plan_code IN ('fubo-extra','fubotv-basic') OR lower(plan_code) LIKE '%bundle%'
  OR lower(plan_code) LIKE '%latino%')
  AND e.email NOT LIKE '%@fubo.tv'
  AND e.email NOT LIKE '%@apple.com'
  AND e.email NOT LIKE '%@roku.com'
  AND e.email IS NOT NULL
  AND e.email <> ""
  ),
 sports_viewers AS
  (
  SELECT DISTINCT t2.user_id, t1.email, sum(duration)/3600 AS hours
  FROM paid_users t1
  INNER JOIN `fubotv-prod.data_insights.video_in_progress_via_va_view` t2 ON t2.user_id = t1.account_code
  INNER JOIN `fubotv-prod.data_insights.tmsid_genre_mapping`t3 ON t3.tms_id = t2.tms_id
  WHERE LOWER(primary_genre_group) IN ('sports')
  AND date(t2.start_time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
  GROUP BY 1,2
  )

SELECT DISTINCT t1.user_id, t1.email
FROM sports_viewers t1
LEFT OUTER JOIN fubotv-dev.business_analytics.longstudy_6Mcohort t2 ON t1.user_id = t2.account_code
LEFT OUTER JOIN fubotv-dev.business_analytics.longstudy_M1cohort t3 ON t1.user_id = t3.account_code
WHERE t2.account_code IS NULL
AND t3.account_code IS NULL
AND hours >=2
ORDER BY RAND()
LIMIT 35000