----------------------------------------------- Sportsbook X FTP ----------------------------------------------------------------

WITH sb_users AS (
Select distinct t1.user_id as userid, t2.final_status_v2_sql
from `fubotv-dev.business_analytics.cross_over_users` t1
JOIN (SELECT t1.account_code, final_status_v2_sql from `fubotv-prod.data_insights.daily_status_static_update` t1 JOIN (select account_code, max(day) max_day from fubotv-prod.data_insights.daily_status_static_update group by 1) t2
on t1.account_code=t2.account_code and t1.day=t2.max_day) t2 on t1.user_id=t2.account_code
where type='TV_First'
)

, ftp_users AS (
SELECT DISTINCT userid,
tms_id,
ftp_question_id,
ftp_device_category
FROM `fubotv-dev.business_analytics.NN_FTP_Data` 
)

SELECT DISTINCT 
t1.userid as sb_user, 
final_status_v2_sql, 
CASE WHEN t1.userid = t2.userid THEN "yes" ELSE "no" END AS played_ftp,
ftp_device_category,
COUNT(DISTINCT tms_id) as match_count,
COUNT (DISTINCT tms_id) as question_count
FROM sb_users t1
LEFT JOIN ftp_users  t2 USING (userid)
GROUP BY 1,2,3,4
ORDER BY 3 DESC