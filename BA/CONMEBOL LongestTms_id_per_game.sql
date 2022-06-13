--First View query:
-- Viewership based on First View
with active_users as
(
Select distinct day,account_code, plan_code
from `fubotv-prod.data_insights.daily_status_static_update`
where 1=1
and day >= '2021-06-03'
--and day <= '2021-12-31'
and final_status_v2_sql in ('paid','paid but canceled','past due','open','paid and scheduled pause')
--Use lines 7 -10 for basic & extra users only
--and (plan_code in ('fubo-extra','fubotv-basic') or lower(plan_code) like '%bundle%')
and lower(plan_code) not like '%canadian%' OR lower(plan_code) not like '%spain%'
--and lower(plan_code_invoice) in ('fubotv-basic','fubo-extra')
/*--Use lines 12-13 for latino users only
and lower(plan_code) like '%latino%'
and lower(plan_code_invoice) like ('%latino%')*/
),
first_view_raw as
(
select user_id, start_time, station_mapping, s.network_owner,primary_genre_group,primary_genre,t1.program_title,t1.league_names, row_number() over (partition by user_id order by start_time asc) as row
from `fubotv-prod.data_insights.video_in_progress_via_va_view` t1
inner join `fubotv-prod.data_insights.tmsid_genre_mapping` m on t1.tms_id =m.tms_id
inner join `fubotv-dev.business_analytics.station_mapping_table_51220` s on s.station_name =t1.channel
where 1=1
and duration >= 300
and UPPER(t1.tms_id) NOT IN ("","00000000000000","TMS-ID-UNAVAILABLE")
and t1.tms_id is not null
and lower(playback_type) = 'live'
),
first_view as
(
select user_id,start_time, network_owner,station_mapping,primary_genre_group,primary_genre,program_title,league_names
from first_view_raw
where 1=1
and row = 1
),
final as (
select t1.account_code, primary_genre_group, primary_genre, league_names
from active_users t1
inner join first_view t2 on t1.account_code=t2.user_id
where 1=1
and primary_genre_group = 'Sports'
),
semi_final as (
select distinct primary_genre_group, League_names, account_code
from final
),
daily_paying_users as
(
Select distinct day,t2.account_code,primary_genre_group, League_names
from `fubotv-prod.data_insights.daily_status_static_update` t1
inner join semi_final t2 on t1.account_code = t2.account_code
where 1=1
and day >= '2021-06-03'
--and day < '2021-12-31'
and final_status_v2_sql like ('paid%')
)
, viewership as (
SELECT DATE_TRUNC(DATE(v.start_time, 'America/New_York'), month) as month,date(start_time) as event_date, m.primary_genre_group,v.asset_id, v.playback_type,v.tms_id, v.program_title,v.episode_title, v.league_names,v.series_title,s.station_mapping, v.user_id, v.duration, v.start_time
from `fubotv-prod.data_insights.video_in_progress_via_va_view` v
inner join `fubotv-prod.data_insights.tmsid_genre_mapping` m on v.tms_id =m.tms_id
inner join `fubotv-dev.business_analytics.station_mapping_table_51220` s on s.station_name =v.channel
where v.tms_id IS NOT NULL
AND UPPER(v.tms_id) NOT IN ("","00000000000000","TMS-ID-UNAVAILABLE")
and DATE(v.start_time, 'America/New_York') >= '2021-06-03'
--and DATE(v.start_time, 'America/New_York') <= '2021-12-31'
--and lower(playback_type) != 'live-preview'
and lower(playback_type) = 'live'
/*and lower(primary_genre_group) like '%sport%'
and lower(primary_genre) = 'soccer'
and v.tms_id NOT LIKE ('%SH%')
and m.program_type = 'match'*/
and v.tms_id IN ('EP033149890044','FUBO20210603185306', 'FUBOCOM0000001','FUBO20210601192345','FUBO20210528202830','FUBO20210601192444',
 'FUBO20210528204447','FUBO20210601192536','FUBO20210601193949','FUBO20210601192633')
)
,combined as (
SELECT event_date,tms_id,program_title,episode_title,account_code,SUM(Hours) as hours_watched,
row_number() OVER (PARTITION BY  account_code ORDER BY SUM(Hours) DESC ) as r
FROM 
(
Select t3.event_date,t3.month,tms_id,program_title,episode_title,t3.league_names,station_mapping,t1.account_code,sum(duration)/3600 as Hours, 
from daily_paying_users t1
inner join `fubotv-dev.business_analytics.account_code_mapping` t2 on t1.account_code=t2.user_id
inner join viewership t3 on t1.account_code=t3.user_id and t1.day = date(t3.start_time)
group by 1,2,3,4,5,6,7,8
)
GROUP BY 1,2,3,4,5
ORDER BY 5
)

SELECT event_date, tms_id, program_title, SUM(hours_watched) Total_Viewed_hours, COUNT(DISTINCT account_code) Users
FROM 
(
select DISTINCT event_date, tms_id, program_title,episode_title, hours_watched, account_code,
from combined
WHERE r=1
ORDER BY 6
)
GROUP BY 1,2,3
ORDER BY 1,3