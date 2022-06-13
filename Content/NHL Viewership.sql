with active_users as
(
Select distinct day, date_trunc(day,MONTH) as month, account_code, plan_code
from `fubotv-prod.data_insights.daily_status_static_update` 
where 1=1
and day >= '2019-01-01'
and day <= '2020-12-31'
and final_status_v2_sql in ('paid','paid but canceled','past due','open','paid and scheduled pause')
--Use lines 7 -10 for basic & extra users only
and (plan_code in ('fubo-extra','fubotv-basic') or lower(plan_code) like '%bundle%')
and lower(plan_code) not like '%latino%'
and lower(plan_code_invoice) in ('fubotv-basic','fubo-extra')
/*--Use lines 12-13 for latino users only
and lower(plan_code) like '%latino%'
and lower(plan_code_invoice) like ('%latino%')*/
),
viewership as (
select DATE_TRUNC(DATE(v.start_time, 'America/New_York'), month) as month,date(start_time), m.primary_genre_group,v.asset_id, v.playback_type,v.tms_id, v.program_title,v.episode_title, v.league_names,v.series_title,s.station_mapping, v.user_id, v.duration, v.start_time
from `fubotv-prod.data_insights.video_in_progress_via_va_view` v
inner join `fubotv-prod.data_insights.tmsid_genre_mapping` m on v.tms_id =m.tms_id 
inner join `fubotv-dev.business_analytics.station_mapping_table_51220` s on s.station_name =v.channel 
where v.tms_id IS NOT NULL 
AND UPPER(v.tms_id) NOT IN ("","00000000000000","TMS-ID-UNAVAILABLE")
and DATE(v.start_time, 'America/New_York') >= '2019-01-01' 
and DATE(v.start_time, 'America/New_York') <= '2020-12-31' 
and lower(playback_type) != 'live-preview'
and lower(playback_type) = 'live'
and lower(primary_genre_group) like '%sport%'
and v.tms_id NOT LIKE ('%SH%')
and lower(primary_genre) like ('%hockey%')
and (v.league_names = 'NHL' OR v.league_names IS NULL)
and ((v.program_title = 'NHL Hockey' OR lower(v.program_title) like '%stanley cup%')  AND v.episode_title IS NOT NULL)
--and (lower(v.program_title) like ('uefa') OR lower(v.program_title) like ('champion') OR lower(v.episode_title) like ('uefa') OR lower(v.episode_title) like ('champion') OR 
--lower(v.league_names) like ('uefa') OR lower(v.league_names) like ('champion'))
--OR v.league_names = 'College Lacrosse' OR v.league_names like '%Women%s  College Lacrosse%')
--and lower(playback_type) = 'vod'
--and lower(v.tms_id) like '%mv%'
--and (v.program_title IN ('Premier Lacrosse League','College Lacrosse','MLL Lacrosse','NLL Lacrosse','Lacrosse') OR v.program_title like ('%Women%s College Lacrosse%')
--OR v.league_names = 'College Lacrosse' OR v.league_names like '%Women%s  College Lacrosse%')
--and lower(s.network_owner) like '%turner%'
--and station_mapping in ('Univision Deportes Network','Univision','The Fight Network')
),
combined as (
Select t3.month,tms_id,program_title,episode_title,league_names,station_mapping,t1.account_code,sum(duration)/3600 as Hours
from active_users t1
inner join `fubotv-dev.business_analytics.account_code_mapping` t2 on t1.account_code=t2.user_id 
inner join viewership t3 on t1.account_code=t3.user_id and t1.day = date(t3.start_time)
group by 1,2,3,4,5,6,7
),

-- first view
trial_users as
(
Select t1.account_code,date_trunc(sub_first_dt,MONTH) as first_paying_month
from `fubotv-prod.data_insights.daily_status_static_update` t1
inner join active_users t2 ON t1.account_code = t2.account_code
where 1=1
and sub_first_dt >= '2019-01-01'  ---- change as needed
and sub_first_dt < '2020-12-01'  ----- change as needed
and final_status_restated in ('paid','paid but canceled','paid and scheduled pause')
and (t1.plan_code in ('fubo-extra','fubotv-basic') or lower(t1.plan_code) like '%bundle%')
and lower(t1.plan_code) not like '%latino%'
and lower(t1.plan_code_invoice) in ('fubotv-basic','fubo-extra')
AND sub_current_nth_day_sql >= 5
and sub_nth_month_by_firstlast_plan_sql = 1
),

first_view_raw as
(
select user_id, start_time, tms_id, row_number() over (partition by user_id order by start_time asc) as row
from `fubotv-prod.data_insights.video_in_progress_via_va_view` t1
inner join `fubotv-dev.business_analytics.station_mapping_table_51220` s on s.station_name =t1.channel
where 1=1
and duration >= 300
and UPPER(tms_id) NOT IN ("","00000000000000","TMS-ID-UNAVAILABLE")
and tms_id is not null
and lower(playback_type) not like '%preview%'

),
first_view as
(
select user_id,start_time, tms_id
from first_view_raw
where 1=1
and row = 1
)


/*
-- Viewership details
select month,tms_id,program_title,episode_title,league_names,station_mapping, sum(hours) as Hours,count(distinct account_code) as viewers,
from combined
GROUP BY 1,2,3,4,5,6

*/

-- For First view Pull for NHL
select t1.first_paying_month, tms_id, count(distinct account_code) as count_of_first_views
from trial_users t1
inner join first_view t2 on t1.account_code=t2.user_id
where t2.tms_id IS NOT NULL
group by 1,2