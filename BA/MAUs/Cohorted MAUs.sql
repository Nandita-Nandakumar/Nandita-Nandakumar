--------------------------------------------------- COHORTED MAUs ----------------------------------------------

with users as 
(
Select distinct t1.day,t1.account_code,t2.user_id,
CASE WHEN DATE_TRUNC(sub_first_dt, month) < '2021-09-01' THEN 'Pre Sep'
WHEN DATE_TRUNC(sub_first_dt, month) = '2021-09-01' THEN 'Sep'
WHEN DATE_TRUNC(sub_first_dt, month) = '2021-10-01' THEN 'Oct'
WHEN DATE_TRUNC(sub_first_dt, month) = '2021-11-01' THEN 'Nov'
WHEN DATE_TRUNC(sub_first_dt, month) = '2021-12-01' THEN 'Dec'
END AS cohort,
from `fubotv-prod.data_insights.daily_status_static_update` t1
inner join `fubotv-dev.business_analytics.account_code_mapping` t2 on t1.account_code=t2.account_code
where day >= '2021-12-01'
and day <  '2022-01-01'
and final_status_v2_sql not in ('expired','paused','past due from trials','in trial')
),

viewership as
(
Select t1.user_id, cohort, sum(duration)/3600 as viewing_hours
from `fubotv-prod.data_insights.video_in_progress_via_va_view` t1
inner join users t2 on t1.user_id=t2.user_id and date(t1.start_time) = t2.day
inner join `fubotv-prod.data_insights.tmsid_genre_mapping` t3 on t1.tms_id =t3.tms_id
where t1.tms_id IS NOT NULL
AND UPPER(t1.tms_id) NOT IN ("","00000000000000","TMS-ID-UNAVAILABLE")
group by 1,2
),

semifinal as 
(
Select cohort, count(distinct user_id) as maus, sum(viewing_hours) as monthly_hours
from viewership
GROUP BY 1
)

Select cohort AS Dec_2021, monthly_hours/maus as MAU
from semifinal
WHERE cohort IS NOT NULL
ORDER BY 1

INNER JOIN `fubotv-dev.business_analytics.AdInsertable_Networks` t4 ON t1.channel = t4.channel