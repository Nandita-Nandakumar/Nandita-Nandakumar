
--reactivation
WITH daily_status as (
select *
      , CASE WHEN current_period_started_at = trial_started_at AND current_period_ends_at = trial_ends_at THEN NULL
             WHEN trial_started_at IS NULL THEN DATE_DIFF(day, DATE(activated_at), DAY)+1
             WHEN trial_ends_at IS NOT NULL THEN DATE_DIFF(day, DATE(trial_ends_at), DAY)+1
             ELSE NULL
        END AS sub_nth_day_by_activated
from `fubotv-prod.data_insights.daily_status_static_update` 
WHERE day < CURRENT_DATE('US/Eastern')
AND day >= '2017-06-01'
)

, react_prepay AS (
select day, plan_code, activated_type, prepay_promo, count (distinct account_code) as freq, 'restated' AS status_shown
from daily_status
where 1=1
and final_status_restated in ('paid','paid but canceled','open','paid and scheduled pause')
and trial_started_at is null
and (sub_nth_day_by_activated = 1 OR resume_start_flag = 'Y')
group by 1, 2, 3, 4
)

, reacts_accounts AS (
select day as reactivated_day, plan_code as react_plan, activated_type, prepay_promo, collection_method as new_collection_method, account_code, 'restated' AS status_shown
from daily_status
where 1=1
and final_status_restated in ('paid','paid but canceled','open','paid and scheduled pause')
and trial_started_at is null
and (sub_nth_day_by_activated = 1 OR resume_start_flag = 'Y')
and activated_type='reactivation'
and date_trunc(day,month)='2021-06-01' -- reacts month to be adjusted per the month
) -- react account count

-- reactivations that have expired prior
,expired_temp AS (
SELECT t1.account_code, MAX(t2.day) as expired_day,
FROM reacts_accounts t1
JOIN  `fubotv-prod.data_insights.daily_status_static_update`  t2 ON t1.account_code = t2.account_code
where t2.final_status_v2_sql = 'expired'
and t1.reactivated_day > t2.day
--and t1.account_code = '5bf9b98a3242cc000710b936'
GROUP BY 1
) 
,expired_2 AS (
Select DISTINCT t1.account_code, expired_day, t2.plan_code, t2.activated_at, t2.collection_method as old_collection_method
from expired_temp t1
JOIN  `fubotv-prod.data_insights.daily_status_static_update`  t2 ON t1.account_code = t2.account_code 
and t2.final_status_v2_sql = 'expired'
and t1.expired_day = t2.day
ORDER BY 1
)

,expired AS (
select t1.account_code, t1.plan_code, expired_day, old_collection_method , count(*) as days_in_plan_code
from expired_2  t1 
join `fubotv-prod.data_insights.daily_status_static_update` t2 on t1.account_code = t2.account_code
WHERE t2.day <= t1.expired_day 
and t2.activated_at = t1.activated_at
group by 1,2,3,4
ORDER BY 1
)

,reacts_expired AS (
SELECT t1.account_code, (DATE_SUB(t1.expired_day , INTERVAL t1.days_in_plan_code  day))+1 as first_date_active_prior_to_expiration,
t1.expired_day as last_expired_date, t1.plan_code as old_plan_code, t1.days_in_plan_code, t2.reactivated_day as reactivated_date
, t2.react_plan as new_plan_code,date_diff(reactivated_day, expired_day, month) months_before_reactivation, t2.new_collection_method, t1.old_collection_method 
from expired t1
inner join reacts_accounts t2 on t1.account_code = t2.account_code
)

--viewership after reacts

,viewership_data_after_reacts AS (
SELECT t1.account_code, t2.start_time , t1.reactivated_date, t2.tms_id, t2.program_title,t2.playback_type, t2.device_category, SUM(duration)/3600 AS hours_consumed, t3.primary_genre_group, 1 as days_engaged
from reacts_expired t1
left join  `fubotv-prod.data_insights.video_in_progress_via_va_view` t2 on t1.account_code = t2.user_id
inner join `fubotv-prod.data_insights.tmsid_genre_mapping` t3 on t3.tms_id = t2.tms_id
WHERE date(start_time) >= reactivated_date -- react_day, modify this for a different match
--AND account_code = '54332ec6646a740200c57301'
GROUP BY 1,2,3,4,5,6,7,9,10
ORDER BY 1,2
)

--first & Longest view after reactivation

,engagement_after_reacts AS (
SELECT DISTINCT  account_code , start_time, date(start_time) as viewing_date, tms_id, program_title, primary_genre_group
,SUM(hours_consumed) AS hours_consumed
,SUM(days_engaged) as period_after_reacts_days_engaged,
row_number() OVER (PARTITION BY  account_code ORDER BY start_time ASC ) as FV,
row_number() OVER (PARTITION BY  account_code ORDER BY SUM(hours_consumed) DESC ) as LV,
from viewership_data_after_reacts 
group by 1,2,3,4,5,6
order by 1,2,9
)

, FV AS (
select DISTINCT account_code, viewing_date as FV_date, tms_id as FV_tms_id, program_title as FV_program_title, primary_genre_group AS FV_genre, hours_consumed AS FV_hours
from  engagement_after_reacts 
WHERE FV=1
)

, LV AS (
select DISTINCT account_code, viewing_date as LV_date, tms_id as LV_tms_id, program_title as LV_program_title, primary_genre_group AS LV_genre, hours_consumed AS LV_hours
from  engagement_after_reacts 
WHERE LV=1
)

select DISTINCT t1.* , FV_date, FV_tms_id, FV_program_title, FV_genre, FV_hours, LV_date, LV_tms_id, LV_program_title, LV_genre, LV_hours
from reacts_expired t1
JOIN FV t2 USING (account_code)
JOIN LV t3 using (account_code)
