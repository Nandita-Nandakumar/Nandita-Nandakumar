/* OLD WAY
----------------------------------------------------------------- Hours Per MAU ---------------------------------------------------------------------
with users as (
Select distinct t1.day,t1.account_code,t2.user_id
from `fubotv-prod.data_insights.daily_status_static_update` t1
inner join `fubotv-dev.business_analytics.account_code_mapping` t2 on t1.account_code=t2.account_code
where day >= '2022-04-01'
and day <=  '2022-04-04'
and final_status_v2_sql not in ('expired','paused','past due from trials','in trial')
---and sub_first_dt < '2021-11-01'
----and sub_first_dt >= '2021-11-01'
),
viewership as (
Select t1.user_id,sum(duration)/3600 as viewing_hours
from `fubotv-prod.data_insights.video_in_progress_via_va_view` t1
inner join users t2 on t1.user_id=t2.user_id and date(t1.start_time) = t2.day
inner join `fubotv-prod.data_insights.tmsid_genre_mapping` t3 on t1.tms_id =t3.tms_id
where t1.tms_id IS NOT NULL
AND UPPER(t1.tms_id) NOT IN ("","00000000000000","TMS-ID-UNAVAILABLE")
---and network_owner = 'AE'
---and primary_genre_group = 'Entertainment'
group by 1
),
semifinal as (
Select count(distinct user_id) as maus, sum(viewing_hours) as monthly_hours
from viewership
)
Select monthly_hours/maus
from semifinal

*/

/* NEW WAY - TESTING 06/02/2022*/
----------------------------------------------------------------- Hours Per MAU ---------------------------------------------------------------------
---- Current Year -------
with params AS (
  
SELECT 
 DATE_TRUNC(current_date()-1, month) AS BOM,
 DATE_TRUNC(current_date()-1, day) AS COM
),
  
users as (
Select distinct t1.day as day,t1.account_code as account_code,t2.user_id as user_id
from `fubotv-prod.data_insights.daily_status_static_update` t1
inner join `fubotv-dev.business_analytics.account_code_mapping` t2 on t1.account_code=t2.account_code
where day >= (SELECT BOM from params)
and day <=  (SELECT COM from params)
and final_status_v2_sql not in ('expired','paused','past due from trials','in trial')
),
viewership as (
Select t1.user_id,sum(duration)/3600 as viewing_hours
from `fubotv-prod.data_insights.video_in_progress_via_va_view` t1
inner join users t2 on t1.user_id=t2.user_id and date(t1.start_time) = t2.day
inner join `fubotv-prod.data_insights.tmsid_genre_mapping` t3 on t1.tms_id =t3.tms_id
where t1.tms_id IS NOT NULL
AND UPPER(t1.tms_id) NOT IN ("","00000000000000","TMS-ID-UNAVAILABLE")
group by 1
),
semifinal as (
Select count(distinct user_id) as maus, sum(viewing_hours) as monthly_hours
from viewership
)
, current_hours_per_mau AS (
Select(SELECT COM FROM params) as m_date, monthly_hours/maus as Hours_per_MAU
from semifinal
)

---------------- 2021

, params2 AS (
  
SELECT 
 DATE_TRUNC(current_date()-1, month)-365 AS BOM,
 DATE_TRUNC(current_date()-1, day)-365 AS COM
),
  
users2 as (
Select distinct t1.day,t1.account_code,t2.user_id
from `fubotv-prod.data_insights.daily_status_static_update` t1
inner join `fubotv-dev.business_analytics.account_code_mapping` t2 on t1.account_code=t2.account_code
where day >= (SELECT BOM from params2)
and day <=  (SELECT COM from params2)
and final_status_v2_sql not in ('expired','paused','past due from trials','in trial')

),
viewership2 as (
Select t1.user_id,sum(duration)/3600 as viewing_hours
from `fubotv-prod.data_insights.video_in_progress_via_va_view` t1
inner join users2 t2 on t1.user_id=t2.user_id and date(t1.start_time) = t2.day
inner join `fubotv-prod.data_insights.tmsid_genre_mapping` t3 on t1.tms_id =t3.tms_id
where t1.tms_id IS NOT NULL
AND UPPER(t1.tms_id) NOT IN ("","00000000000000","TMS-ID-UNAVAILABLE")
group by 1
),
semifinal2 as (
Select count(distinct user_id) as maus, sum(viewing_hours) as monthly_hours
from viewership2
)
, YAG_hours_per_mau AS (
Select (SELECT COM FROM params2) as m_date, monthly_hours/maus AS Hours_per_MAU
from semifinal2
)

SELECT DISTINCT t1.*
FROM current_hours_per_mau as t1
UNION ALL
SELECT DISTINCT t2.*
FROM YAG_hours_per_mau as t2