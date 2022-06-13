# PARAMOUNT YELLOWSTONE VIEWERS
WITH active_users as
(
Select distinct t1.day,t1.account_code,t2.user_id,
case when t1.final_status_v2_sql IN ('in trial', 'past due from trials') then 'Trial User'
else 'Paying User' end as user_type
from `fubotv-prod.data_insights.daily_status_static_update` t1
inner join `fubotv-dev.business_analytics.account_code_mapping` t2 on t1.account_code=t2.account_code
where final_status_v2_sql not in ('expired','paused','past due from trials and scheduled pause','failed and scheduled pause')
and lower(plan_code) not like '%canadian%'
and lower(plan_code) not like '%spain%'
and day >= '2020-06-01' ----adjust dates here for a user 
)

,viewership as (
select DATE_TRUNC(DATE(v.start_time, 'America/New_York'), month) as month,date(start_time) as event_date, m.primary_genre_group,v.channel, v.playback_type,primary_genre,station_mapping, s.network_owner, v.program_title,v.episode_title, v.user_id, v.duration, v.start_time,
case when v.episode_title is null then v.program_title else v.episode_title end as event, lower(t4.ad_insertable) as ad_insertable
from `fubotv-prod.data_insights.video_in_progress_via_va_view` v
inner join `fubotv-prod.data_insights.tmsid_genre_mapping` m on v.tms_id =m.tms_id -- Genre Mapping Table
inner join `fubotv-dev.business_analytics.station_mapping_table_51220` s on s.station_name =v.channel -- Station Mapping Table
inner join `fubotv-dev.business_analytics.AdInsertable_Networks` t4 on v.channel=t4.channel  -- Ad Insertable Table
where v.tms_id IS NOT NULL
AND UPPER(v.tms_id) NOT IN ("","00000000000000","TMS-ID-UNAVAILABLE")
AND LOWER(playback_type) NOT IN ('live_preview')
and DATE(v.start_time, 'America/New_York') >= '2020-06-01'

--------- YELLOWSTONE SHOW -----------------
and LOWER(v.program_title) LIKE ('%yellowstone%') 
and station_mapping in ('Paramount Network')
AND duration >= 300

----------- PARAMOUNT NETWORK ---------------
and station_mapping in ('Paramount Network')
AND duration >= 60 

----------- YELLOWSTONE PROXY SHOWS -----------
and ( (LOWER(v.program_title) LIKE ('%queen of the south%') and station_mapping in ('USA Network')) OR
      (LOWER(v.program_title) LIKE ('%animal kingdom%') and station_mapping in ('TNT')) OR  
      (LOWER(v.program_title) LIKE ('nos4a2%') and station_mapping in ('AMC Network')) OR
      (LOWER(v.program_title) LIKE ('%snowpiercer%') and station_mapping in ('TNT')) OR
      (LOWER(v.program_title) LIKE ('%the sinner%') and station_mapping in ('USA Network')) OR 
      (LOWER(v.program_title) LIKE ('%counting cars%') and station_mapping in ('History Channel')) OR
      (LOWER(v.program_title) LIKE ('%deadliest catch') and station_mapping in ('Discovery Channel')) OR 
      (LOWER(v.program_title) LIKE ('%street outlaws%') and station_mapping in ('Discovery Channel')) OR 
      (LOWER(v.program_title) LIKE ('walker') and station_mapping in ('The CW')) OR 
      (LOWER(v.program_title) LIKE ('nascar%') and station_mapping in ('FOX', 'FOX Sports 1')) OR 
      (LOWER(v.program_title) LIKE ('%academy of country music award%') and station_mapping in ('CBS')) OR
      (LOWER(v.program_title) LIKE ('billions%') and s.network_owner in ('Showtime')) OR 
      (LOWER(v.program_title) LIKE ('no man\'s land'))
    ) 
and duration >= 60
ORDER BY v.program_title, channel

)

, combined as (
Select t3.month, event_date,
t3.primary_genre_group,primary_genre,t3.program_title,episode_title,t3.channel,station_mapping, network_owner, ad_insertable, playback_type, t1.account_code, t1.user_type, sum(duration)/3600 as Hours
from active_users t1
inner join `fubotv-dev.business_analytics.account_code_mapping` t2 on t1.account_code=t2.user_id
inner join viewership t3 on t2.user_id=t3.user_id and t1.day = date(t3.start_time)
group by 1,2,3,4,5,6,7,8,9,10,11,12,13
)

select count(distinct account_code) as uniques, sum(hours) as Hours
from combined