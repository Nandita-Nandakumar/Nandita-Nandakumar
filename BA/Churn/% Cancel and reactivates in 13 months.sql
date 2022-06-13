#4/15
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
and date_trunc(day,month)='2021-03-01'
)

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

,viewership_data_before_reacts AS (
SELECT t1.account_code, date(t2.start_time) as day, SUM(duration)/3600 AS hours_consumed, t3.primary_genre_group, 1 as days_engaged
from reacts_expired t1
left join  `fubotv-prod.data_insights.video_in_progress_via_va_view` t2 on t1.account_code = t2.user_id
inner join `fubotv-prod.data_insights.tmsid_genre_mapping` t3 on t3.tms_id = t2.tms_id
WHERE date(start_time) >= first_date_active_prior_to_expiration
AND date(start_time) <= last_expired_date
GROUP BY 1,2,4,5
)

,viewership_data_after_reacts AS (
SELECT t1.account_code, date(t2.start_time) as day, t1.reactivated_date,  SUM(duration)/3600 AS hours_consumed, t3.primary_genre_group, 1 as days_engaged
from reacts_expired t1
left join  `fubotv-prod.data_insights.video_in_progress_via_va_view` t2 on t1.account_code = t2.user_id
inner join `fubotv-prod.data_insights.tmsid_genre_mapping` t3 on t3.tms_id = t2.tms_id
WHERE date(start_time) >= reactivated_date
GROUP BY 1,2,3,5,6
)

,engagement_prior_to_reacts AS (
SELECT DISTINCT  account_code , primary_genre_group
,SUM(hours_consumed) AS period_prior_to_expiration_hours_engaged,SUM(days_engaged) as period_prior_to_expiration_days_engaged,
row_number() OVER (PARTITION BY  account_code ORDER BY SUm(hours_consumed) DESC ) as r
from viewership_data_before_reacts 
group by 1,2
order by 1
) 

,engagement_after_reacts AS (
SELECT DISTINCT  account_code , primary_genre_group
,SUM(hours_consumed) AS period_after_reacts_hours_engaged,SUM(days_engaged) as period_after_reacts_days_engaged,
row_number() OVER (PARTITION BY  account_code ORDER BY SUm(hours_consumed) DESC ) as r
from viewership_data_after_reacts 
group by 1,2
order by 1
)

,users as (
select DISTINCT t1.*, t2.primary_genre_group as old_primary_genre_group, t2.period_prior_to_expiration_days_engaged, 
t2.period_prior_to_expiration_hours_engaged , t3.primary_genre_group AS new_primary_genre_group
from reacts_expired t1
JOIN engagement_prior_to_reacts  t2 on t1.account_code = t2.account_code
JOIN engagement_after_reacts t3 on t1.account_code = t3.account_code
WHERE t2.r=1
and t3.r=1
)

, coupon_users as (
Select distinct account_code,coupon_code
from `fubotv-prod.data_insights.daily_status_static_update`
where (lower(coupon_code) like '%csteam%'
  or lower(coupon_code) like '%aff201%'
  or lower(coupon_code) like '%voxmedia%'
  or lower(coupon_code) like '%bengrad%'
  or lower(coupon_code) like '%bd%'
  or lower(coupon_code) like '%nctcpartner%'
  or lower(coupon_code) like '%mincoupon%'
  or lower(coupon_code) like '%1yearofffubo%'
  or lower(coupon_code) like '%fubotvemployee%'
  or lower(coupon_code) like '%nbcdemoaccounts%'
  or lower(coupon_code) like '%testing100off%'
  or lower(coupon_code) like '%fan100%'
  or lower(coupon_code) like '%partnergift%'
  or lower(coupon_code) like '%fansided1year%'
  or lower(coupon_code) like '%qaaccounts%'
  or lower(coupon_code) like '%fubotvemployee%'
  or lower(coupon_code) like '%pressaccount%'
  or lower(coupon_code) like '%bbpartners%'
  or lower(coupon_code) like '%pass4revolt%'
  or lower(coupon_code) like '%internalandtesting%'
  or lower(coupon_code) like '%fubo100%'
  or lower(coupon_code) like '%complimentary%'
  or lower(coupon_code) like '%compedspain%'
  or lower(coupon_code) like '%alabrad%'
  or lower(coupon_code) like '%retention%'
  or lower(coupon_code) like '%winback%'
  or lower(coupon_code) like '%ret%')
  AND date_trunc(day,month) = '2021-03-01'
)
, users_exclude_coupons as (
Select distinct t1.account_code
from users t1
left outer join coupon_users t2 on t1.account_code=t2.account_code
where t2.account_code is NULL
)

, final as (
select t2.*
from users_exclude_coupons t1
inner join users t2 on t1.account_code = t2.account_code
)

, viewership_activity_react_day AS (
SELECT t1.account_code, date(start_time) as day, t1.reactivated_date, t2.tms_id, t2.program_title, t2.playback_type, t2.device_category, SUM(duration)/3600 AS hours_consumed
FROM final t1
JOIN `fubotv-prod.data_insights.video_in_progress_via_va_view` t2 on t1.account_code = t2.user_id
WHERE date(start_time) = reactivated_date
GROUP BY 1,2,3,4,5,6,7
ORDER BY 1
)

--longest tms_id 
,engagement_activity AS (
SELECT DISTINCT account_code, tms_id, program_title, playback_type, device_category, hours_consumed, 
row_number() OVER (PARTITION BY  account_code ORDER BY SUM(hours_consumed) DESC ) as r
from viewership_activity_react_day 
group by 1,2,3,4,5,6
order by 1
)

--,users_react_activity AS (
SELECT DISTINCT t1.*, t2.tms_id, t2.program_title, t2.playback_type, t2.device_category, hours_consumed
from  reacts_expired t1
join engagement_activity  t2 on t1.account_code = t2.account_code
where r=1


-- all reacts activity - longest tms_id


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
and date_trunc(day,month)='2021-03-01'
)

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

,viewership_data_before_reacts AS (
SELECT t1.account_code, date(t2.start_time) as day, SUM(duration)/3600 AS hours_consumed, t3.primary_genre_group, 1 as days_engaged
from reacts_expired t1
left join  `fubotv-prod.data_insights.video_in_progress_via_va_view` t2 on t1.account_code = t2.user_id
inner join `fubotv-prod.data_insights.tmsid_genre_mapping` t3 on t3.tms_id = t2.tms_id
WHERE date(start_time) >= first_date_active_prior_to_expiration
AND date(start_time) <= last_expired_date
GROUP BY 1,2,4,5
)

,viewership_data_after_reacts AS (
SELECT t1.account_code, date(t2.start_time) as day, t1.reactivated_date, t2.tms_id, t2.program_title,t2.playback_type, t2.device_category, SUM(duration)/3600 AS hours_consumed, t3.primary_genre_group, 1 as days_engaged
from reacts_expired t1
left join  `fubotv-prod.data_insights.video_in_progress_via_va_view` t2 on t1.account_code = t2.user_id
inner join `fubotv-prod.data_insights.tmsid_genre_mapping` t3 on t3.tms_id = t2.tms_id
WHERE date(start_time) = reactivated_date
GROUP BY 1,2,3,4,5,6,7,9,10
)

,engagement_prior_to_reacts AS (
SELECT DISTINCT  account_code , primary_genre_group
,SUM(hours_consumed) AS period_prior_to_expiration_hours_engaged,SUM(days_engaged) as period_prior_to_expiration_days_engaged,
row_number() OVER (PARTITION BY  account_code ORDER BY SUm(hours_consumed) DESC ) as r
from viewership_data_before_reacts 
group by 1,2
order by 1
) 

,engagement_after_reacts AS (
SELECT DISTINCT  account_code , tms_id, program_title, playback_type, device_category, primary_genre_group
,SUM(hours_consumed) AS period_after_reacts_hours_engaged,SUM(days_engaged) as period_after_reacts_days_engaged,
row_number() OVER (PARTITION BY  account_code ORDER BY SUm(hours_consumed) DESC ) as r,
row_number() OVER (PARTITION BY  account_code ORDER BY SUM(hours_consumed) DESC ) as r2
from viewership_data_after_reacts 
group by 1,2,3,4,5,6
order by 1
)

select DISTINCT t1.* , t3.tms_id, t3.program_title,t3.playback_type,t3.device_category, t3.period_after_reacts_hours_engaged 
from reacts_expired t1
--JOIN engagement_prior_to_reacts  t2 on t1.account_code = t2.account_code
JOIN engagement_after_reacts t3 on t1.account_code = t3.account_code
WHERE t3.r=1
and t3.r2=1


-- CURRENT RUN

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
and date_trunc(day,month)='2021-03-01' -- reacts month to be adjusted per the month
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
SELECT t1.account_code, date(t2.start_time) as day, t1.reactivated_date, t2.tms_id, t2.program_title,t2.playback_type, t2.device_category, SUM(duration)/3600 AS hours_consumed, t3.primary_genre_group, 1 as days_engaged
from reacts_expired t1
left join  `fubotv-prod.data_insights.video_in_progress_via_va_view` t2 on t1.account_code = t2.user_id
inner join `fubotv-prod.data_insights.tmsid_genre_mapping` t3 on t3.tms_id = t2.tms_id
WHERE date(start_time) = reactivated_date -- react_day, modify this for a different match
GROUP BY 1,2,3,4,5,6,7,9,10
)
-- longest tms_id per user since reactivation
,engagement_after_reacts AS (
SELECT DISTINCT  account_code , tms_id, program_title, playback_type, device_category, primary_genre_group
,SUM(hours_consumed) AS period_after_reacts_hours_engaged,SUM(days_engaged) as period_after_reacts_days_engaged,
row_number() OVER (PARTITION BY  account_code ORDER BY SUm(hours_consumed) DESC ) as r,
from viewership_data_after_reacts 
group by 1,2,3,4,5,6
order by 1
)

select DISTINCT t1.* , t3.tms_id, t3.program_title,t3.playback_type,t3.device_category, t3.period_after_reacts_hours_engaged 
from reacts_expired t1
JOIN engagement_after_reacts t3 on t1.account_code = t3.account_code
WHERE t3.r=1