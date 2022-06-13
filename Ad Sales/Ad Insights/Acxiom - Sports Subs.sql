--- Current SUbs who watched atleast 10 mins and 2 matches of each of the 7 sport groups
with active_users as
(
Select distinct day,account_code, plan_code, e.email
from `fubotv-prod.data_insights.daily_status_static_update` t1
inner join `fubotv-dev.business_analytics.email_mapping` e using(account_code)
where 1=1
and day = current_date() - 1 ---- April 6th 2022
and final_status_v2_sql like ('paid%')
and lower(plan_code) not like '%can%' and lower(plan_code) not like '%spain%'
and lower(plan_code) not like '%latino%' -- Excludes Latino Plan -- comment this for soccer
),
viewership as (
select DATE_TRUNC(DATE(v.start_time, 'America/New_York'), month) as month,date(start_time, 'America/New_York') as date, m.primary_genre_group,primary_genre,second_genre,v.asset_id, v.playback_type,v.tms_id, v.program_title,v.episode_title, v.league_names,v.series_title,s.station_mapping,s.network_owner, v.user_id, v.duration, v.start_time,extract(dayofweek from date(v.start_time, 'America/New_York')) as dow,
from `fubotv-prod.data_insights.video_in_progress_via_va_view` v
inner join `fubotv-prod.data_insights.tmsid_genre_mapping` m on v.tms_id =m.tms_id 
inner join `fubotv-dev.business_analytics.station_mapping_table_51220` s on s.station_name =v.channel 
where v.tms_id IS NOT NULL 
AND UPPER(v.tms_id) NOT IN ("","00000000000000","TMS-ID-UNAVAILABLE")
and DATE(v.start_time, 'America/New_York') >= '2021-01-01'
and DATE(v.start_time, 'America/New_York') <= '2022-12-31'
and lower(playback_type) != 'live-preview'
and lower(playback_type) = 'live'
and lower(primary_genre_group) like '%sport%'
and v.tms_id not like ('%SH%')
and primary_genre IN ('Hockey','Football','Soccer','Basketball','Baseball')
and m.program_type = 'match'
and duration >= 600 --- 10 minutes minimum
),
combined as (
Select t3.month,tms_id,primary_genre,second_genre,program_title,episode_title,league_names,network_owner,station_mapping,t1.account_code,t1.plan_code,t1.email,sum(duration)/3600 as Hours
from active_users t1
inner join `fubotv-dev.business_analytics.account_code_mapping` t2 on t1.account_code=t2.user_id 
inner join viewership t3 on t1.account_code=t3.user_id
group by 1,2,3,4,5,6,7,8,9,10,11,12
)

, final AS (
SELECT month, account_code, UPPER(to_hex(md5(lower(email)))) as md5_hashed_email,
case when primary_genre = 'Football' and (league_names like '%NFL%' OR program_title IN ('NFL Football','NFL Football in 4k')) and month >= '2021-09-09' and month <= '2022-02-13'then 'NFL'
WHEN primary_genre = 'Football' and (league_names like '%College Football%' OR program_title IN ('College Football','College Football in 4K'))  and month >= '2021-08-28' and month <= '2022-01-10' then 'College Football'
WHEN primary_genre = 'Soccer' and month >= '2021-01-01' and month <= '2021-12-31'  THEN 'Soccer'
when primary_genre = 'Baseball' and (league_names like '%MLB%' OR program_title IN ('MLB Baseball','MLB Baseball in 4K')) and month >= '2021-04-01' and month <= '2022-04-30'then 'MLB'
when primary_genre = 'Hockey' and (league_names like '%NHL%' OR program_title IN ('NHL Hockey')) and month >= '2021-10-12' and month <= '2022-06-30' then 'NHL'  
when primary_genre = 'Basketball' and (league_names like '%NBA%' OR program_title IN ('NBA Basketball'))  and month >= '2021-10-19' and month <= '2022-06-19' then 'NBA'
when primary_genre = 'Basketball' and (league_names like '%College Basketball%' OR league_names like '%2019 NCAA Basketball Tournament%' OR program_title IN ('College Basketball'))  and month >= '2021-11-09' and month <= '2022-04-22'then 'College Basketball'
else 'Other' end as league, 
COUNT(DISTINCT tms_id) as matches
FROM combined
GROUP BY 1,2,3,4
)

SELECT DISTINCT account_code as fubo_account_code, md5_hashed_email
FROM final
WHERE matches >= 2
AND league = 'MLB'
