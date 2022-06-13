#other one
# GENERIC VIEWERSHIP PULL BY PROGRAM
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
and day >= '2018-10-01' ----adjust dates here for a user 
),
viewership as (
select DATE_TRUNC(DATE(v.start_time, 'America/New_York'), month) as month,date(start_time) as event_date, m.primary_genre_group,v.channel, v.playback_type,primary_genre,station_mapping, s.network_owner, v.program_title,v.episode_title, v.user_id, v.duration, v.start_time,
case when v.episode_title is null then v.program_title else v.episode_title end as event, lower(t4.ad_insertable) as ad_insertable
from `fubotv-prod.data_insights.video_in_progress_via_va_view` v
inner join `fubotv-prod.data_insights.tmsid_genre_mapping` m on v.tms_id =m.tms_id -- Genre Mapping Table
inner join `fubotv-dev.business_analytics.station_mapping_table_51220` s on s.station_name =v.channel -- Station Mapping Table
inner join `fubotv-dev.business_analytics.AdInsertable_Networks` t4 on v.channel=t4.channel  -- Ad Insertable Table
where v.tms_id IS NOT NULL
AND UPPER(v.tms_id) NOT IN ("","00000000000000","TMS-ID-UNAVAILABLE")
and DATE(v.start_time, 'America/New_York') >= '2021-01-01' -- adjust dates for program
--and DATE(v.start_time, 'America/New_York') < '2021-01-01' -- adjust dates for program
--adjust the following parameters accordingly
--and playback_type = 'live'
--and lower(primary_genre) like '%cyc%'
--and lower(primary_genre) not like '%motor%'
--and v.program_title not in ('inCycle')
--and channel in ('Fubo Cycling Canada')
--and v.episode_title like ('%Giro%Italia%')
--and station_mapping in ('fubo Cycling','fubo Sports Network','NBCSN','Olympic Channel','Rai Italia','FOX Sports 2','NBC')
--and lower(v.program_title) like 'conmebol%copa%2019'
--and v.program_title not in ('MLS en 60')
--and station_mapping in ('Univision','FOX Soccer Plus','Univision Deportes Network','FOX Deportes','FOX Sports 1','FOX Sports 2','UniMas','ESPN','My Network TV','Galavision')
)
, combined as (
Select t3.month, event_date,
t3.primary_genre_group,primary_genre,t3.program_title,episode_title,t3.channel,station_mapping, network_owner, ad_insertable, t1.account_code, t1.user_type, sum(duration)/3600 as Hours
from active_users t1
inner join `fubotv-dev.business_analytics.account_code_mapping` t2 on t1.account_code=t2.user_id
inner join viewership t3 on t2.user_id=t3.user_id and t1.day = date(t3.start_time)
where event_date >= '2021-06-10'
group by 1,2,3,4,5,6,7,8,9,10,11,12
)
select event_date, ad_insertable, count(distinct account_code) as uniques, sum(hours) as Hours
from combined
GROUP BY 1,2
ORDER BY 1,3


#currently in Dash

WITH  days as (
select
current_date()  as EOP,
date("2019-1-1", 'America/New_York')as BOP
)

,active_users as
(
Select distinct 
t1.day,
t1.account_code, 
(select EOP from days) as EOP,
case when t1.final_status_v2_sql IN ('in trial', 'past due from trials') then 'Trial User'
     else 'Paying User' end as user_type
from `fubotv-prod.data_insights.daily_status_static_update` t1
inner join `fubotv-dev.business_analytics.account_code_mapping` t2 on t1.account_code=t2.account_code
where final_status_v2_sql not in ('expired','paused','past due from trials and scheduled pause','failed and scheduled pause')
and lower(plan_code) not like '%canadian%'
and lower(plan_code) not like '%spain%'
)

#From Net Val by Station
, viewership as (
select
  v.playback_type,
  s.network_owner, 
  s.station_mapping, 
  v.program_title, 
  v.tms_id, 
  v.series_title, 
  v.episode_title, 
  m.primary_genre, 
  m.primary_genre_group, 
  v.device_category, 
  ad_insertable,
  user_type,
  case when date(v.start_time) >= '2020-07-01' AND s.network_owner = 'Turner Networks' then NULL 
         when (date(v.start_time) >= '2020-01-01' AND date(v.start_time) <= '2020-07-01') AND s.network_owner = 'Disney' AND (s.station_mapping IN ('FX','FXX','FXM','National Geographic','National Geographic Wild')) then NULL 
         when (date(v.start_time) >= '2020-01-01') AND s.network_owner = 'FOX' AND (s.station_mapping IN ('Fox SportsTime Ohio','BabyTV','The CW','YES Network','FOX Sports Arizona','FOX Sports Carolinas','FOX Sports Cincinnati',
        'FOX Sports Detroit','FOX Sports Florida','FOX Sports Indiana','FOX Sports Kansas City','FOX Sports Midwest','FOX Sports New Orleans','FOX Sports North','FOX Sports Ohio','FOX Sports Oklahoma','FOX Sports Prime Ticket',
        'FOX Sports San Diego','FOX Sports South','FOX Sports Southeast','FOX Sports Southwest','FOX Sports Sun','FOX Sports Tennessee','FOX Sports West','FOX Sports Wisconsin')) then NULL
         when date(v.start_time) >= '2020-05-01' AND s.network_owner = 'XITE' then NULL
         when date(v.start_time) >= '2019-01-01' AND s.station_mapping = 'TeleXitos' then NULL
         else v.user_id end as user_id, 
  case when date(v.start_time) >= '2020-07-01' AND s.network_owner = 'Turner Networks' then NULL 
         when (date(v.start_time) >= '2020-01-01' AND date(v.start_time) <= '2020-08-01') AND s.network_owner = 'Disney' AND (s.station_mapping IN ('FX','FXX','FXM','National Geographic','National Geographic Wild')) then NULL 
         when (date(v.start_time) >= '2020-01-01') AND s.network_owner = 'FOX' AND (s.station_mapping IN ('Fox SportsTime Ohio','BabyTV','The CW','YES Network','FOX Sports Arizona','FOX Sports Carolinas','FOX Sports Cincinnati',
        'FOX Sports Detroit','FOX Sports Florida','FOX Sports Indiana','FOX Sports Kansas City','FOX Sports Midwest','FOX Sports New Orleans','FOX Sports North','FOX Sports Ohio','FOX Sports Oklahoma','FOX Sports Prime Ticket',
        'FOX Sports San Diego','FOX Sports South','FOX Sports Southeast','FOX Sports Southwest','FOX Sports Sun','FOX Sports Tennessee','FOX Sports West','FOX Sports Wisconsin')) then NULL
         when date(v.start_time) >= '2020-05-01' AND s.network_owner = 'XITE' then NULL
         when date(v.start_time) >= '2019-01-01' AND s.station_mapping = 'TeleXitos' then NULL
         else v.duration end as duration,
v.start_time, 
v.league_names, 
date(v.start_time) as day, 
(select EOP from days) as EOP, 
DATE_TRUNC(DATE(v.start_time),month) month
from `fubotv-prod.data_insights.video_in_progress_via_va_view` v
inner join active_users t2 on v.user_id = t2.account_code and t2.day = date(v.start_time)
inner join `fubotv-prod.data_insights.tmsid_genre_mapping` m on v.tms_id =m.tms_id 
inner join `fubotv-dev.business_analytics.station_mapping_table_51220` s on s.station_name =v.channel 
inner join `fubotv-dev.business_analytics.AdInsertable_Networks` t4 on v.channel=t4.channel  -- Ad Insertable Table
where v.start_time >=TIMESTAMP((select BOP from days))
AND start_time < TIMESTAMP((select EOP from days))

),
/* hours & uniques*/
hours_uniques as 
(
    select 
    day,
    ad_insertable,
    user_type,
    case when month <= '2019-12-01' and network_owner = 'Disney' then 'FOX'
         when month > '2019-12-01' and network_owner = 'FOX' AND (station_mapping IN ('Fox SportsTime Ohio','BabyTV','The CW','YES Network','FOX Sports Arizona','FOX Sports Carolinas','FOX Sports Cincinnati',
        'FOX Sports Detroit','FOX Sports Florida','FOX Sports Indiana','FOX Sports Kansas City','FOX Sports Midwest','FOX Sports New Orleans','FOX Sports North','FOX Sports Ohio','FOX Sports Oklahoma','FOX Sports Prime Ticket',
        'FOX Sports San Diego','FOX Sports South','FOX Sports Southeast','FOX Sports Southwest','FOX Sports Sun','FOX Sports Tennessee','FOX Sports West','FOX Sports Wisconsin')) then NULL
        when month > '2020-06-01' and network_owner = 'Turner Networks' then NULL
        when month >= '2020-01-01' and month <= '2020-07-01' and network_owner = 'Disney' then NULL
        else network_owner end as network_owner, 
    case when month <= '2019-12-01' and network_owner = 'Disney' then 'FOX'
         when month > '2019-12-01' and network_owner = 'FOX' AND (station_mapping IN ('Fox SportsTime Ohio','BabyTV','The CW','YES Network','FOX Sports Arizona','FOX Sports Carolinas','FOX Sports Cincinnati',
        'FOX Sports Detroit','FOX Sports Florida','FOX Sports Indiana','FOX Sports Kansas City','FOX Sports Midwest','FOX Sports New Orleans','FOX Sports North','FOX Sports Ohio','FOX Sports Oklahoma','FOX Sports Prime Ticket',
        'FOX Sports San Diego','FOX Sports South','FOX Sports Southeast','FOX Sports Southwest','FOX Sports Sun','FOX Sports Tennessee','FOX Sports West','FOX Sports Wisconsin')) then NULL
        when month > '2020-06-01' and network_owner = 'Turner Networks' then NULL
        when month >= '2020-01-01' and month <= '2020-07-01' and network_owner = 'Disney' then NULL
        else network_owner end as station_mapping,
    sum(duration)/3600 as hours, 
    count(distinct user_id) as uniques
    from viewership
    group by 1,2,3,4,5
    order by 1,2
)

SELECT *
FROM hours_uniques
WHERE day >='2021-06-01'