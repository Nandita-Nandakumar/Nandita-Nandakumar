-- Churned Users by Sub start month

with expired as (
Select distinct account_code, day as expired_day 
from `fubotv-prod.data_insights.daily_status_static_update`
where final_status_v2_sql = 'expired'
and expired_from_sql = 'subs'
and LOWER(plan_code) NOT LIKE '%spain%'
and day >= '2022-05-01' -- always beginning of month
),

paused as (
SELECT day as paused_day, account_code
FROM `fubotv-prod.data_insights.daily_status_static_update`
where final_status_v2_sql = 'paused'
and day = DATE(paused_at)
and LOWER(plan_code) NOT LIKE '%spain%'
and day >= '2022-05-01' -- always beginning of month
),

expired_and_paused as (
Select distinct account_code
from expired
union all
select distinct account_code
from paused
),

sub_first_month as (
Select distinct t1.account_code,date_trunc(sub_first_dt,MONTH) as start_month
from expired_and_paused t1
inner join `fubotv-prod.data_insights.daily_status_static_update` t2 on t1.account_code=t2.account_code
)

Select start_month,count(distinct account_code) as churned_users
from sub_first_month
group by 1
order by 1 asc

-- if need to compare with users in BOM


with users1 as (
Select distinct account_code,date_trunc(sub_first_dt,MONTH) as start_month
from `fubotv-prod.data_insights.daily_status_static_update`
where day = '2022-05-01' --also bom
and final_status_restated like '%paid%'
and LOWER(plan_code) NOT LIKE '%spain%'
)
Select start_month,count(distinct account_code) as users
from users1
group by 1
order by 1 asc



----------------------------------- Churned Users Behavior --------------------------
with expired as (
Select distinct account_code, day as expired_day 
from `fubotv-prod.data_insights.daily_status_static_update`
where final_status_v2_sql = 'expired'
and expired_from_sql = 'subs'
and day >= '2021-11-01' -- always beginning of month
),

paused as (
SELECT day as paused_day, account_code
FROM `fubotv-prod.data_insights.daily_status_static_update`
where final_status_v2_sql = 'paused'
and day = DATE(paused_at)
and day >= '2021-11-01' -- always beginning of month
),

expired_and_paused as (
Select distinct account_code
from expired
union all
select distinct account_code
from paused
),

sub_first_month as (
Select distinct t1.account_code,date_trunc(sub_first_dt,MONTH) as start_month
from expired_and_paused t1
inner join `fubotv-prod.data_insights.daily_status_static_update` t2 on t1.account_code=t2.account_code
)

, churned_users AS (
Select start_month,account_code as churned_user
from sub_first_month
order by 1 asc
)

, static_update AS
(
    SELECT DISTINCT c_month, account_code, plan_code 
    FROM 
    (
    SELECT DISTINCT day, DATE_TRUNC(day,Month) as c_month,account_code, plan_code, row_number() OVER (PARTITION BY  account_code ORDER BY day DESC ) AS r 
    FROM `fubotv-prod.data_insights.daily_status_static_update` 
    ORDER BY 5
    )
    WHERE r=1
)
,churned_user_list AS 
(
SELECT DISTINCT start_month, churned_user, plan_code
FROM  churned_users c 
INNER JOIN static_update t1 ON t1.account_code= c.churned_user
ORDER BY 2
)

, viewership AS
(
SELECT DATE(start_time,'EST') as date_est, DATE_TRUNC(DATE(start_time), Month) AS month_s, user_id, primary_genre_group, primary_genre, t2.program_title, t2.episode_title, SUM(duration)/3600 AS Hours
FROM `fubotv-prod.data_insights.video_in_progress_via_va_view` t1
INNER JOIN `fubotv-prod.data_insights.tmsid_genre_mapping` t2 ON t1.tms_id = t2.tms_id
GROUP BY 1,2,3,4,5,6,7
)

,combined AS 
(
SELECT DISTINCT primary_genre_group, COUNT (DISTINCT churned_user) as uniques , SUM(Hours) as Hours
FROM churned_user_list t1
INNER JOIN viewership t2 ON t1.churned_user = t2.user_id AND t1.start_month <= t2.month_s
GROUp BY 1
)

SELECT uniques 
FROM combined

-------------------------------------- By Team ----------------------------------------

with active_users as
(
Select distinct day,account_code, plan_code
from `fubotv-prod.data_insights.daily_status_static_update`
where 1=1
and day >= '2020-09-01'
and final_status_v2_sql like 'paid%'
),
viewership as (
select DATE_TRUNC(DATE(v.start_time, 'America/New_York'), month) as month,date(start_time,'America/New_York'), m.primary_genre_group,v.asset_id, v.playback_type,v.tms_id, v.program_title,v.episode_title, v.league_names,v.series_title,s.station_mapping, v.user_id, v.duration, v.start_time, m.team_ids,m.team_names
from `fubotv-prod.data_insights.video_in_progress_via_va_view` v
inner join `fubotv-prod.data_insights.tmsid_genre_mapping` m on v.tms_id =m.tms_id
inner join `fubotv-dev.business_analytics.station_mapping_table_51220` s on s.station_name =v.channel
where v.tms_id IS NOT NULL
AND UPPER(v.tms_id) NOT IN ("","00000000000000","TMS-ID-UNAVAILABLE")
and DATE(v.start_time, 'America/New_York') >= '2020-09-01'
and DATE(v.start_time, 'America/New_York') < current_date()
and lower(playback_type) != 'live-preview'
and lower(playback_type) = 'live'
and lower(primary_genre_group) like '%sport%'
and v.tms_id NOT LIKE ('%SH%')
and lower(primary_genre) in ('football')
and m.program_type = 'match'
),
combined as (
Select t3.month,tms_id,program_title,episode_title,league_names,station_mapping,t1.account_code,plan_code,team_ids, team_names, sum(duration)/3600 as Hours
from active_users t1
inner join `fubotv-dev.business_analytics.account_code_mapping` t2 on t1.account_code=t2.user_id
inner join viewership t3 on t1.account_code=t3.user_id and t1.day = date(t3.start_time)
group by 1,2,3,4,5,6,7,8,9,10
),
final as (
select month, station_mapping, case when lower(league_names) = 'nfl' then 'NFL'
else '' end as program_title, league_names, episode_title, account_code, team_ids, case when lower(plan_code) like '%latino%' then 'Latino' else 'English' end as plan, sum(hours) as hours
from combined
group by 1,2,3,4,5,6,7,8),
final_2 as (
select month, plan ,account_code, program_title, split(team_ids, ',')[safe_offset(0)] as Home, split(team_ids, ',')[safe_offset(1)] as Away, sum(Hours) as Hours
from final
where program_title <> ''
GROUP BY 1,2,3,4,5,6
),
final_3 as (
select month, plan, account_code, program_title,trim(Home,'(') as Home, trim(Away, ')') as Away, sum(Hours) as Hours
from final_2
GROUP BY 1,2,3,4,5,6
),
home_games as (
select month, plan, program_title, account_code,
case when home = '31' then 'Arizona Cardinals'
when home = '32' then 'Atlanta Falcons'
when home = '33' then 'Baltimore Ravens'
when home = '34' then 'Buffalo Bills'
when home = '35' then 'Carolina Panthers'
when home = '36' then 'Chicago Bears'
when home = '37' then 'Cincinnati Bengals'
when home = '38' then 'Cleveland Browns'
when home = '39' then 'Dallas Cowboys'
when home = '40' then 'Denver Broncos'
when home = '41' then 'Detroit Lions'
when home = '42' then 'Green Bay Packers'
when home = '43' then 'Houston Texans'
when home = '44' then 'Indianapolis Colts'
when home = '45' then 'Jacksonville Jaguars'
when home = '46' then 'Kansas City Chiefs'
when home = '20783' then 'Las Vegas Raiders'
when home = '14215' then 'Los Angeles Chargers'
when home = '9690' then 'Los Angeles Rams'
when home = '47' then 'Miami Dolphins'
when home = '48' then 'Minnesota Vikings'
when home = '49' then 'New England Patriots'
when home = '50' then 'New Orleans Saints'
when home = '51' then 'New York Giants'
when home = '52' then 'New York Jets'
when home = '54' then 'Philadelphia Eagles'
when home = '55' then 'Pittsburgh Steelers'
when home = '57' then 'San Francisco 49ers'
when home = '58' then 'Seattle Seahawks'
when home = '60' then 'Tampa Bay Buccaneers'
when home = '61' then 'Tennessee Titans'
when home = '21208' then 'Washington Football Team'
else 'Show' end as Team, sum(hours) as hours
from final_3
GROUP BY 1,2,3,4,5
),
away_games as (
select month, plan, program_title, account_code,
case when away = '31' then 'Arizona Cardinals'
when away = '32' then 'Atlanta Falcons'
when away = '33' then 'Baltimore Ravens'
when away = '34' then 'Buffalo Bills'
when away = '35' then 'Carolina Panthers'
when away = '36' then 'Chicago Bears'
when away = '37' then 'Cincinnati Bengals'
when away = '38' then 'Cleveland Browns'
when away = '39' then 'Dallas Cowboys'
when away = '40' then 'Denver Broncos'
when away = '41' then 'Detroit Lions'
when away = '42' then 'Green Bay Packers'
when away = '43' then 'Houston Texans'
when away = '44' then 'Indianapolis Colts'
when away = '45' then 'Jacksonville Jaguars'
when away = '46' then 'Kansas City Chiefs'
when away = '20783' then 'Las Vegas Raiders'
when away = '14215' then 'Los Angeles Chargers'
when away = '9690' then 'Los Angeles Rams'
when away = '47' then 'Miami Dolphins'
when away = '48' then 'Minnesota Vikings'
when away = '49' then 'New England Patriots'
when away = '50' then 'New Orleans Saints'
when away = '51' then 'New York Giants'
when away = '52' then 'New York Jets'
when away = '54' then 'Philadelphia Eagles'
when away = '55' then 'Pittsburgh Steelers'
when away = '57' then 'San Francisco 49ers'
when away = '58' then 'Seattle Seahawks'
when away = '60' then 'Tampa Bay Buccaneers'
when away = '61' then 'Tennessee Titans'
when away = '21208' then 'Washington Football Team'
else 'Show' end as Team, sum(hours) as hours, 
from final_3
GROUP BY 1,2,3,4,5
)

,join_teams as (
select * from home_games
UNION ALL
select * from away_games
),
----------------------------------- Churned Users by Sub start month AND what plan they were on
expired as (
Select distinct account_code, day as expired_day
from `fubotv-prod.data_insights.daily_status_static_update`
where final_status_v2_sql = 'expired'
and expired_from_sql = 'subs'
and day >= '2021-11-01' -- always beginning of month
),
paused as (
SELECT day as paused_day, account_code
FROM `fubotv-prod.data_insights.daily_status_static_update`
where final_status_v2_sql = 'paused'
and day = DATE(paused_at)
and day >= '2021-11-01' -- always beginning of month
),
expired_and_paused as (
Select distinct account_code
from expired
union all
select distinct account_code
from paused
),
sub_first_month as (
Select distinct t1.account_code,date_trunc(sub_first_dt,MONTH) as start_month
from expired_and_paused t1
inner join `fubotv-prod.data_insights.daily_status_static_update` t2 on t1.account_code=t2.account_code
)
, churned_users AS (
Select start_month,account_code as churned_user
from sub_first_month
order by 1 asc
)
, static_update AS (
    SELECT DISTINCT c_day, c_month, account_code, plan_code
    FROM
    (
    SELECT DISTINCT day as c_day, DATE_TRUNC(day,Month) as c_month,account_code, plan_code, row_number() OVER (PARTITION BY  account_code ORDER BY day DESC ) AS r
    FROM `fubotv-prod.data_insights.daily_status_static_update`
    ORDER BY 5
    )
    WHERE r=1
)
,churned_user_list AS (
SELECT DISTINCT start_month, c_day as churned_day, churned_user, plan_code
FROM  churned_users c
INNER JOIN static_update t1 ON t1.account_code= c.churned_user
ORDER BY 3
)
select --account_code, churned_day , 
Team, sum(hours) as hours, count(distinct account_code) as viewers
from join_teams t
--inner join churned_user_list c on t.account_code = c.churned_user
where program_title <> ''
AND LOWER(program_title) NOT LIKE 'show'
group by 1--,2,3
order by 1
-------------------------------- with team_ID --------------------- By Team
with active_users as
(
Select distinct day,account_code, plan_code
from `fubotv-prod.data_insights.daily_status_static_update`
where 1=1
and day >= '2020-09-01'
and final_status_v2_sql like 'paid%'
),
viewership as (
select DATE_TRUNC(DATE(v.start_time, 'America/New_York'), month) as month,date(start_time,'America/New_York'), m.primary_genre_group,v.asset_id, v.playback_type,v.tms_id, v.program_title,v.episode_title, v.league_names,v.series_title,s.station_mapping, v.user_id, v.duration, v.start_time, m.team_ids,m.team_names
from `fubotv-prod.data_insights.video_in_progress_via_va_view` v
inner join `fubotv-prod.data_insights.tmsid_genre_mapping` m on v.tms_id =m.tms_id
inner join `fubotv-dev.business_analytics.station_mapping_table_51220` s on s.station_name =v.channel
where v.tms_id IS NOT NULL
AND UPPER(v.tms_id) NOT IN ("","00000000000000","TMS-ID-UNAVAILABLE")
and DATE(v.start_time, 'America/New_York') >= '2020-09-01'
and DATE(v.start_time, 'America/New_York') < current_date()
and lower(playback_type) != 'live-preview'
and lower(playback_type) = 'live'
and lower(primary_genre_group) like '%sport%'
and v.tms_id NOT LIKE ('%SH%')
and lower(primary_genre) in ('football')
and m.program_type = 'match'
),
combined as (
Select t3.month,tms_id,program_title,episode_title,league_names,station_mapping,t1.account_code,plan_code,team_ids, team_names, sum(duration)/3600 as Hours
from active_users t1
inner join `fubotv-dev.business_analytics.account_code_mapping` t2 on t1.account_code=t2.user_id
inner join viewership t3 on t1.account_code=t3.user_id and t1.day = date(t3.start_time)
group by 1,2,3,4,5,6,7,8,9,10
),
final as (
select month, station_mapping, case when lower(league_names) = 'nfl' then 'NFL'
else '' end as program_title, league_names, episode_title, account_code, team_ids, case when lower(plan_code) like '%latino%' then 'Latino' else 'English' end as plan, sum(hours) as hours
from combined
group by 1,2,3,4,5,6,7,8),
final_2 as (
select month, plan ,account_code, program_title, episode_title, split(team_ids, ',')[safe_offset(0)] as Home, split(team_ids, ',')[safe_offset(1)] as Away, sum(Hours) as Hours
from final
where program_title <> ''
GROUP BY 1,2,3,4,5,6,7
),
final_3 as (
select month, plan, account_code, program_title, episode_title,trim(Home,'(') as Home, trim(Away, ')') as Away, sum(Hours) as Hours
from final_2
GROUP BY 1,2,3,4,5,6,7
),
home_games as (
select month, plan, program_title, episode_title, account_code, 1 as num_matches,
case when home = '31' then 'Arizona Cardinals'
when home = '32' then 'Atlanta Falcons'
when home = '33' then 'Baltimore Ravens'
when home = '34' then 'Buffalo Bills'
when home = '35' then 'Carolina Panthers'
when home = '36' then 'Chicago Bears'
when home = '37' then 'Cincinnati Bengals'
when home = '38' then 'Cleveland Browns'
when home = '39' then 'Dallas Cowboys'
when home = '40' then 'Denver Broncos'
when home = '41' then 'Detroit Lions'
when home = '42' then 'Green Bay Packers'
when home = '43' then 'Houston Texans'
when home = '44' then 'Indianapolis Colts'
when home = '45' then 'Jacksonville Jaguars'
when home = '46' then 'Kansas City Chiefs'
when home = '20783' then 'Las Vegas Raiders'
when home = '14215' then 'Los Angeles Chargers'
when home = '9690' then 'Los Angeles Rams'
when home = '47' then 'Miami Dolphins'
when home = '48' then 'Minnesota Vikings'
when home = '49' then 'New England Patriots'
when home = '50' then 'New Orleans Saints'
when home = '51' then 'New York Giants'
when home = '52' then 'New York Jets'
when home = '54' then 'Philadelphia Eagles'
when home = '55' then 'Pittsburgh Steelers'
when home = '57' then 'San Francisco 49ers'
when home = '58' then 'Seattle Seahawks'
when home = '60' then 'Tampa Bay Buccaneers'
when home = '61' then 'Tennessee Titans'
when home = '21208' then 'Washington Football Team'
else 'Show' end as Team, sum(hours) as hours
from final_3
GROUP BY 1,2,3,4,5,6,7
),
away_games as (
select month, plan, program_title, episode_title, account_code, 1 as num_matches,
case when away = '31' then 'Arizona Cardinals'
when away = '32' then 'Atlanta Falcons'
when away = '33' then 'Baltimore Ravens'
when away = '34' then 'Buffalo Bills'
when away = '35' then 'Carolina Panthers'
when away = '36' then 'Chicago Bears'
when away = '37' then 'Cincinnati Bengals'
when away = '38' then 'Cleveland Browns'
when away = '39' then 'Dallas Cowboys'
when away = '40' then 'Denver Broncos'
when away = '41' then 'Detroit Lions'
when away = '42' then 'Green Bay Packers'
when away = '43' then 'Houston Texans'
when away = '44' then 'Indianapolis Colts'
when away = '45' then 'Jacksonville Jaguars'
when away = '46' then 'Kansas City Chiefs'
when away = '20783' then 'Las Vegas Raiders'
when away = '14215' then 'Los Angeles Chargers'
when away = '9690' then 'Los Angeles Rams'
when away = '47' then 'Miami Dolphins'
when away = '48' then 'Minnesota Vikings'
when away = '49' then 'New England Patriots'
when away = '50' then 'New Orleans Saints'
when away = '51' then 'New York Giants'
when away = '52' then 'New York Jets'
when away = '54' then 'Philadelphia Eagles'
when away = '55' then 'Pittsburgh Steelers'
when away = '57' then 'San Francisco 49ers'
when away = '58' then 'Seattle Seahawks'
when away = '60' then 'Tampa Bay Buccaneers'
when away = '61' then 'Tennessee Titans'
when away = '21208' then 'Washington Football Team'
else 'Show' end as Team, sum(hours) as hours, 
from final_3
GROUP BY 1,2,3,4,5,6,7
)

,join_teams as (
select * from home_games
UNION ALL
select * from away_games
where Team <> ''
AND LOWER(Team) NOT LIKE '%show%'
)

,churned_viewership AS (
select account_code, Team, sum(hours) as hours, SUM(num_matches)as num_matches, 
row_number() OVER (PARTITION BY account_code ORDER BY SUM(hours) DESC ) AS r
from join_teams t
inner join fubotv-dev.business_analytics.NN_churned_user_list_nov c on t.account_code = c.churned_user
--AND account_code = '54f8b3befe7eae030013c7e7'
group by 1,2
order by 1, 5 DESC
)

SELECT account_code, Team, sum(hours) as hours, SUM(num_matches)as num_matches
FROM churned_viewership 
WHERE r=1
GROUP BY 1,2

------------------------ For Primary Genre Group ----------------------

with viewership AS
(
SELECT DATE(start_time,'EST') as date_est, 
DATE_TRUNC(DATE(start_time), Month) AS month_s, 
user_id, 
primary_genre_group, 
primary_genre, 
t2.program_title, 
t2.episode_title, 
SUM(duration)/3600 AS Hours
FROM `fubotv-prod.data_insights.video_in_progress_via_va_view` t1
INNER JOIN `fubotv-prod.data_insights.tmsid_genre_mapping` t2 ON t1.tms_id = t2.tms_id
WHERE date(start_time, 'EST') >= '2020-09-01'
GROUP BY 1,2,3,4,5,6,7
)

,churn_viewership AS 
(
SELECT DISTINCT primary_genre_group, user_id, SUM(Hours) AS Hours
FROM `fubotv-dev.business_analytics.NN_churned_user_list_nov` t1
INNER JOIN viewership t2 ON t1.churned_user = t2.user_id
GROUp BY 1,2
)

,final AS (
SELECT DISTINCT user_id, primary_genre_group, Hours
FROM 
(
SELECT 
DISTINCT user_id, primary_genre_group, SUM(Hours) as Hours, row_number() OVER (PARTITION BY user_id ORDER BY SUM(Hours) DESC ) AS r
FROM churn_viewership 
GROUP BY 1,2
)
WHERE r=1
)

SELECT user_id, primary_genre_group, SUM(Hours) AS hours
FROM final 
GROUP BY 1,2