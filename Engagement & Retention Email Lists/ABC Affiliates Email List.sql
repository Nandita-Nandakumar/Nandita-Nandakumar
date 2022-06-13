WITH active_english_viewership AS 
  (
  SELECT DISTINCT s.account_code, e.email
  FROM `fubotv-prod.data_insights.daily_status_static_update` s
  INNER JOIN  `fubotv-dev.business_analytics.email_mapping` e 
  ON s.account_code = e.account_code
  WHERE day = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
  AND final_status_v2_sql NOT IN ('paused', 'expired')
  AND (plan_code IN ('fubo-extra','fubotv-basic') OR LOWER(plan_code) LIKE '%bundle%')
  AND LOWER(plan_code) NOT LIKE '%latino%'
  AND LOWER(plan_code_invoice) IN ('fubotv-basic','fubo-extra')
  AND e.email not like '%@fubo.tv'
  AND e.email not like '%@apple.com'
  AND e.email not like '%@roku.com'
  AND e.email is not null
  AND e.email <> ""
  ),
  abc_viewers AS
  (
  SELECT DISTINCT a.account_code, a.email, 'abc' as indicator
  FROM active_english_viewership a
  INNER JOIN `fubotv-prod.data_insights.video_in_progress_via_va_view` v 
  ON v.user_id = a.account_code
  WHERE LOWER(channel) LIKE ('abc (%')
  ),
  not_abc_viewers AS
  (
  SELECT DISTINCT a.account_code, a.email, 'not abc' as indicator
  FROM active_english_viewership a
  LEFT JOIN abc_viewers v ON a.account_code = v.account_code
  WHERE v.account_code IS NULL
  ),
  viewer_group AS
  (
  SELECT DISTINCT a.account_code, a.email, a.indicator
  FROM abc_viewers a
  UNION ALL
  SELECT DISTINCT b.account_code, b.email, b.indicator
  FROM not_abc_viewers b
  )
  
SELECT CASE WHEN LOWER(indicator) LIKE ('abc') 
        THEN 'Watched ABC'
        ELSE 'Never watched ABC'
        END AS viewers_bucket, email
FROM viewer_group ;