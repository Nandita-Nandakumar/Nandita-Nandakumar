------------------------------------------------------------------------------------ Movie Viewership ------------------------------------------------------------------------------------
with active_users as
(
Select distinct day,account_code, plan_code
from `fubotv-prod.data_insights.daily_status_static_update` 
where 1=1
and day >= '2021-01-01'
--and final_status_restated like ('paid%')
and lower(plan_code) not like '%spain%'
)

, viewership as (
select DATE_TRUNC(DATE(v.start_time, 'America/New_York'), month) as month,date(start_time, 'America/New_York') as date, m.primary_genre_group,primary_genre,second_genre,v.asset_id, v.playback_type,v.tms_id, v.program_title,v.episode_title, v.league_names,v.series_title,s.station_mapping, v.user_id, v.duration, v.start_time,t4.ad_insertable,extract(dayofweek from date(v.start_time, 'America/New_York')) as dow
from `fubotv-prod.data_insights.video_in_progress_via_va_view` v
inner join `fubotv-prod.data_insights.tmsid_genre_mapping` m on v.tms_id =m.tms_id 
inner join `fubotv-dev.business_analytics.station_mapping_table_51220` s on s.station_name =v.channel 
inner join `fubotv-dev.business_analytics.AdInsertable_Networks` t4 on v.channel=t4.channel
where v.tms_id IS NOT NULL 
AND UPPER(v.tms_id) NOT IN ("","00000000000000","TMS-ID-UNAVAILABLE")
and DATE(v.start_time, 'America/New_York') >= '2021-01-01'
and lower(playback_type) != 'live-preview'
and lower(playback_type) = 'live'
AND v.tms_id LIKE 'MV%'
)

SELECT DISTINCT tms_id
FROM viewership
LIMIT 100