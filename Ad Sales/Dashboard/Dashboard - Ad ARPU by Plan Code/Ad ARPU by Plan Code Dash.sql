# Subscriber numbers

SELECT Sub_Date,
user_type,
plan_code,
Sub_Count
FROM
(
SELECT t1.day as Sub_Date,
case when t1.final_status_restated IN ('in trial', 'past due from trials') then "Trial User"
WHEN LOWER(final_status_restated) IN ('paid', 'paid and scheduled pause','paid but canceled') then "Paying User" end as user_type,
plan_code,
count(DISTINCT(t1.account_code)) AS Sub_Count
FROM `fubotv-prod.data_insights.daily_status_static_update` t1
WHERE day >= '2021-01-01' -- adjust according to the dates looking for
GROUP BY 1,2,3
ORDER BY 1 DESC
)
WHERE user_type IS NOT NULL


# FW Logs
SELECT DISTINCT *
FROM `fubotv-dev.business_analytics.NN_FW_Impressions_Station_PlanCode`
WHERE DATE_TRUNC(event_date,month) < DATE_TRUNC(current_date,month)