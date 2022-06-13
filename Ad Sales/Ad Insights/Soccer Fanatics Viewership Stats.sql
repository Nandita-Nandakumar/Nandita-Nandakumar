----------------------------------------------------------- 2020 SOCCER FANATICS -------------------------------------------------------
---- THE criteria we went for is 4 hours per month / 48 hours per year to be considered a fan.
---- excluding la liga from this group because its not comparable.

with old_cohort as
(
Select distinct day,t1.account_code
from `fubotv-prod.data_insights.daily_status_static_update` t1
where 1=1
and final_status_restated like ('paid%')
and lower(plan_code) not like '%cana%'
and lower(plan_code) not like '%spain%'
and day >= '2020-01-01'
and day <= '2020-12-31'
),

current_cohort as (
Select distinct t1.account_code
from `fubotv-prod.data_insights.daily_status_static_update` t1
where 1=1
and final_status_restated like ('paid%')
and lower(plan_code) not like '%cana%'
and lower(plan_code) not like '%spain%'
and day = '2022-02-14'
),

days_viewing as
(
Select 
user_id, 
playback_type,
date(start_time, "EST") as days_watched, 
t2.league_names, 
t2.program_title, 
CASE WHEN playback_type = 'live_preview' THEN 'live_preview'
WHEN playback_type = 'live'
 OR playback_type = 'stream ' 
 OR playback_type = 'lite' 
 OR playback_type LIKE 'liv%'THEN 'live'
WHEN playback_type = 'vod' THEN 'vod'
WHEN playback_type = 'dvr' THEN 'dvr'
WHEN playback_type = 'lookback' THEN 'lookback'
WHEN playback_type = 'dvr_startover' THEN 'dvr_startover'
WHEN playback_type = 'start_over' 
 OR playback_type = 'startover' THEN 'startover'
ELSE 'others'
END AS playback_type_group, 
device_category,
t2.tms_id,
sum(duration)/3600 as total_hours
from `fubotv-prod.data_insights.video_in_progress_via_va_view` t2
inner join `fubotv-prod.data_insights.tmsid_genre_mapping` m on t2.tms_id =m.tms_id
where 1=1
AND t2.tms_id IS NOT NULL
AND UPPER(t2.tms_id) NOT IN ("","00000000000000","TMS-ID-UNAVAILABLE")
AND t2.program_title IS NOT NULL 
AND t2.league_names IS NOT NULL
and date(start_time,"EST") >= '2020-01-01'
and date(start_time,"EST") <= '2020-12-31'
AND LOWER(playback_type) NOT IN ('live_preview')
AND LOWER(m.primary_genre_group) LIKE '%sport%'
AND LOWER(primary_genre) = 'soccer'
and t2.tms_id like ('EP%')
and m.program_type = 'match'
and LOWER(t2.league_names) NOT IN ('la liga') -- excluding la liga
group by 1,2,3,4,5,6,7,8
order by 3,4
),

aggregate as (
Select distinct 
t1.account_code,
CASE WHEN t3.account_code IS NULL then 'Churned' else 'Active' end as still_active,
ifNULL(sum(total_hours),0) as total_hours,
ifNULL(count(distinct days_watched),0) as days_watched,
ifNULL(count(distinct league_names),0) as league_watched,
ifNULL(count(distinct tms_id),0) as matches_watched
from old_cohort t1  
inner join days_viewing t2 on t1.account_code=t2.user_id and t1.day = t2.days_watched
left outer join current_cohort t3 on t3.account_code = t1.account_code
GROUP BY 1,2
),

all_info as (
Select distinct 2020 as year, account_code,still_active,total_hours,days_watched,league_watched,matches_watched
from aggregate
--WHERE still_active = 'Active'
WHERE total_hours >= 1
ORDER BY 1,2
)

SELECT *
FROM all_info
--FROM percent
ORDER BY 1,2


----------------------------------------------------------- 2021 SOCCER FANATICS -------------------------------------------------------
with old_cohort as
(
Select distinct day,t1.account_code
from `fubotv-prod.data_insights.daily_status_static_update` t1
where 1=1
and final_status_restated like ('paid%')
and lower(plan_code) not like '%cana%'
and lower(plan_code) not like '%spain%'
and day >= '2021-01-01'
and day <= '2021-12-31'
),

current_cohort as (
Select distinct t1.account_code
from `fubotv-prod.data_insights.daily_status_static_update` t1
where 1=1
and final_status_restated like ('paid%')
and lower(plan_code) not like '%cana%'
and lower(plan_code) not like '%spain%'
and day = '2022-02-14'
),

days_viewing as
(
Select 
user_id, 
playback_type,
date(start_time, "EST") as days_watched, 
t2.league_names, 
t2.program_title, 
CASE WHEN playback_type = 'live_preview' THEN 'live_preview'
WHEN playback_type = 'live'
 OR playback_type = 'stream ' 
 OR playback_type = 'lite' 
 OR playback_type LIKE 'liv%'THEN 'live'
WHEN playback_type = 'vod' THEN 'vod'
WHEN playback_type = 'dvr' THEN 'dvr'
WHEN playback_type = 'lookback' THEN 'lookback'
WHEN playback_type = 'dvr_startover' THEN 'dvr_startover'
WHEN playback_type = 'start_over' 
 OR playback_type = 'startover' THEN 'startover'
ELSE 'others'
END AS playback_type_group, 
device_category,
t2.tms_id,
sum(duration)/3600 as total_hours
from `fubotv-prod.data_insights.video_in_progress_via_va_view` t2
inner join `fubotv-prod.data_insights.tmsid_genre_mapping` m on t2.tms_id =m.tms_id
where 1=1
AND t2.tms_id IS NOT NULL
AND UPPER(t2.tms_id) NOT IN ("","00000000000000","TMS-ID-UNAVAILABLE")
AND t2.program_title IS NOT NULL 
AND t2.league_names IS NOT NULL
and date(start_time,"EST") >= '2021-01-01'
and date(start_time,"EST") <= '2021-12-31'
AND LOWER(playback_type) NOT IN ('live_preview')
AND LOWER(m.primary_genre_group) LIKE '%sport%'
AND LOWER(primary_genre) = 'soccer'
and t2.tms_id like ('EP%')
and LOWER(t2.league_names) NOT IN ('la liga') -- excluding la liga
order by 3,4
),

aggregate as (
Select distinct 
t1.account_code,
CASE WHEN t3.account_code IS NULL then 'Churned' else 'Active' end as still_active,
ifNULL(sum(total_hours),0) as total_hours,
ifNULL(count(distinct days_watched),0) as days_watched,
ifNULL(count(distinct league_names),0) as league_watched,
ifNULL(count(distinct tms_id),0) as matches_watched
from old_cohort t1  
inner join days_viewing t2 on t1.account_code=t2.user_id and t1.day = t2.days_watched
left outer join current_cohort t3 on t3.account_code = t1.account_code
GROUP BY 1,2
),

all_info as (
Select distinct 2021 as year, account_code,still_active,total_hours,days_watched,league_watched,matches_watched
from aggregate
--WHERE still_active = 'Active'
WHERE total_hours >= 1
ORDER BY 1,2
)

SELECT *
FROM all_info
--FROM percent
ORDER BY 1,2

################################################################################################################################################
------ TO identify what % of subs are watching some soccer ----
----------------------------------------------------------- 2020 UNIQUE SOCCER VIEWERS  -------------------------------------------------------
------ Repeat the same for 2021
with old_cohort as
(
Select distinct day,t1.account_code
from `fubotv-prod.data_insights.daily_status_static_update` t1
where 1=1
and final_status_restated like ('paid%')
and lower(plan_code) not like '%cana%'
and lower(plan_code) not like '%spain%'
and day >= '2020-01-01'
and day <= '2020-12-31'
),

current_cohort as (
Select distinct t1.account_code
from `fubotv-prod.data_insights.daily_status_static_update` t1
where 1=1
and final_status_restated like ('paid%')
and lower(plan_code) not like '%cana%'
and lower(plan_code) not like '%spain%'
and day = '2022-02-14'
),

days_viewing as
(
Select 
DATE(start_time, "EST") as start_date,
user_id, 
playback_type,
date(start_time, "EST") as days_watched, 
t2.league_names, 
t2.program_title, 
CASE WHEN playback_type = 'live_preview' THEN 'live_preview'
WHEN playback_type = 'live'
 OR playback_type = 'stream ' 
 OR playback_type = 'lite' 
 OR playback_type LIKE 'liv%'THEN 'live'
WHEN playback_type = 'vod' THEN 'vod'
WHEN playback_type = 'dvr' THEN 'dvr'
WHEN playback_type = 'lookback' THEN 'lookback'
WHEN playback_type = 'dvr_startover' THEN 'dvr_startover'
WHEN playback_type = 'start_over' 
 OR playback_type = 'startover' THEN 'startover'
ELSE 'others'
END AS playback_type_group, 
device_category,
t2.tms_id,
sum(duration)/3600 as total_hours
from `fubotv-prod.data_insights.video_in_progress_via_va_view` t2
inner join `fubotv-prod.data_insights.tmsid_genre_mapping` m on t2.tms_id =m.tms_id
where 1=1
AND t2.tms_id IS NOT NULL
AND UPPER(t2.tms_id) NOT IN ("","00000000000000","TMS-ID-UNAVAILABLE")
AND t2.program_title IS NOT NULL 
AND t2.league_names IS NOT NULL
and date(start_time,"EST") >= '2020-01-01'
and date(start_time,"EST") <= '2020-12-31'
AND LOWER(playback_type) NOT IN ('live_preview')
AND LOWER(m.primary_genre_group) LIKE '%sport%'
AND LOWER(primary_genre) = 'soccer'
and t2.tms_id like ('EP%')
and m.program_type = 'match'
and LOWER(t2.league_names) NOT IN ('la liga') -- excluding la liga
group by 1,2,3,4,5,6,7,8,9
order by 3,4
),

aggregate as (
Select distinct 
DATE_TRUNC(start_date, month) as mon,
t1.account_code,
-- playback_type_group, --
-- device_category, -- 
CASE WHEN t3.account_code IS NULL then 'Churned' else 'Active' end as still_active,
ifNULL(sum(total_hours),0) as total_hours,
ifNULL(count(distinct days_watched),0) as days_watched,
ifNULL(count(distinct league_names),0) as league_watched,
ifNULL(count(distinct tms_id),0) as matches_watched
from old_cohort t1  
inner join days_viewing t2 on t1.account_code=t2.user_id and t1.day = t2.days_watched
left outer join current_cohort t3 on t3.account_code = t1.account_code
GROUP BY 1,2,3
)


, all_info as (
Select distinct 
--2020 as year, 
mon,
account_code, 
--device_category,
--playback_type_group, 
SUM(total_hours) as hours
--distinct 2020 as year, account_code,still_active,total_hours,days_watched,league_watched,matches_watched ---- first round
from aggregate
--WHERE total_hours >= 48
GROUP BY 1,2
ORDER BY 1,2
)

SELECT mon, COUNT(DISTINCT account_code) as watched_soccer
FROM all_info 
GROUP BY 1
--FROM percent
ORDER BY 1,2

----------------------------------------- SUBS BY MONTH -----------------------------------------------
SELECT 
DATE_TRUNC(t1.day, month) AS Sub_month,
count(DISTINCT(t1.account_code)) AS Sub_Count
FROM `fubotv-prod.data_insights.daily_status_static_update` t1
WHERE day >= '2020-01-01' -- adjust according to the dates looking for
and final_status_restated like ('paid%')
and lower(plan_code) not like '%cana%'
and lower(plan_code) not like '%spain%'
GROUP BY 1
ORDER BY 1