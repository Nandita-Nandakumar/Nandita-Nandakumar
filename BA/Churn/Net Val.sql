with days as (
select
current_date()  as EOP,
date("2020-01-01", 'America/New_York')as BOP
),
daily_paying_users as
(
Select distinct day,account_code, (select EOP from days) as EOP
from `fubotv-prod.data_insights.daily_status_static_update` 
where 1=1
and day >= (select BOP from days)
and day < (select EOP from days)
and final_status_v2_sql like ('paid%')
and (plan_code in ('fubotv-basic','fubo-extra')
or plan_code like '%Bundle%' or plan_code like '%quarter%' or plan_code like '%qrtr%')
and lower(plan_code) not like '%latino%'
and lower(plan_code_invoice) in ('fubotv-basic','fubo-extra','fubotv-basic-qrtr')
and (lower(coupon_code) not like '%csteam%'
   and lower(coupon_code) not like '%aff201%'
   and lower(coupon_code) not like '%voxmedia%'
   and lower(coupon_code) not like '%bengrad%'
   and lower(coupon_code) not like '%bd%'
   and lower(coupon_code) not like '%nctcpartner%'
   and lower(coupon_code) not like '%mincoupon%'
   and lower(coupon_code) not like '%1yearofffubo%'
   and lower(coupon_code) not like '%fubotvemployee%'
   and lower(coupon_code) not like '%nbcdemoaccounts%'
   and lower(coupon_code) not like '%testing100off%'
   and lower(coupon_code) not like '%fan100%'
   and lower(coupon_code) not like '%partnergift%'
   and lower(coupon_code) not like '%fansided1year%'
   and lower(coupon_code) not like '%qaaccounts%'
   and lower(coupon_code) not like '%fubotvemployee%'
   and lower(coupon_code) not like '%pressaccount%'
   and lower(coupon_code) not like '%bbpartners%'
   and lower(coupon_code) not like '%bizdev%'
   and lower(coupon_code) not like '%partnergift%'
    or coupon_code is NULL)
),  
paying_days as (
select EOP, date_trunc(day,month) month, account_code,count(distinct day) as billing_days 
from daily_paying_users
group by 1,2,3
),
viewership as (
select v.playback_type, s.network_owner, s.station_mapping, v.program_title, v.tms_id, v.series_title, v.episode_title, m.primary_genre, m.primary_genre_group, v.device_category, 
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
v.start_time, v.league_names, date(v.start_time) as day, (select EOP from days) as EOP, DATE_TRUNC(DATE(v.start_time),month) month
from `fubotv-prod.data_insights.video_in_progress_via_va_view` v
inner join daily_paying_users t2 on v.user_id = t2.account_code and t2.day = date(v.start_time) /*AND DATE_TRUNC(t2.day,month)=DATE_TRUNC(DATE(v.start_time),month)*/
inner join `fubotv-prod.data_insights.tmsid_genre_mapping` m on v.tms_id =m.tms_id 
inner join `fubotv-dev.business_analytics.station_mapping_table_51220` s on s.station_name =v.channel 
where v.start_time >=TIMESTAMP((select BOP from days))
AND start_time < TIMESTAMP((select EOP from days))
AND (package_mapping like('%basic%'))
/*and (lower(s.network_owner) like '%cbs%')*/
),
/* hours & uniques*/
hours_uniques as 
(select EOP, date_trunc(day,month) as month,station_mapping,
case when month <= '2019-12-01' and network_owner = 'Disney' then 'FOX'
when month > '2019-12-01' and network_owner = 'FOX' AND (station_mapping IN ('Fox SportsTime Ohio','BabyTV','The CW','YES Network','FOX Sports Arizona','FOX Sports Carolinas','FOX Sports Cincinnati',
'FOX Sports Detroit','FOX Sports Florida','FOX Sports Indiana','FOX Sports Kansas City','FOX Sports Midwest','FOX Sports New Orleans','FOX Sports North','FOX Sports Ohio','FOX Sports Oklahoma','FOX Sports Prime Ticket',
'FOX Sports San Diego','FOX Sports South','FOX Sports Southeast','FOX Sports Southwest','FOX Sports Sun','FOX Sports Tennessee','FOX Sports West','FOX Sports Wisconsin')) then NULL
when month > '2020-06-01' and network_owner = 'Turner Networks' then NULL
when month >= '2020-01-01' and month <= '2020-07-01' and network_owner = 'Disney' then NULL
else network_owner end as network_owner, sum(duration)/3600 as hours, count(distinct user_id) as uniques
from viewership
group by 1, 2, 3, 4
order by 1,2
)
,
/* percent watched*/
daily_hours_streamed as (
select distinct t1.EOP, t1.account_code, date_trunc(t1.day,month) month,station_mapping,
case when month <= '2019-12-01' and network_owner = 'Disney' then 'FOX'
when month > '2019-12-01' and network_owner = 'FOX' AND (station_mapping IN ('Fox SportsTime Ohio','BabyTV','The CW','YES Network','FOX Sports Arizona','FOX Sports Carolinas','FOX Sports Cincinnati',
'FOX Sports Detroit','FOX Sports Florida','FOX Sports Indiana','FOX Sports Kansas City','FOX Sports Midwest','FOX Sports New Orleans','FOX Sports North','FOX Sports Ohio','FOX Sports Oklahoma','FOX Sports Prime Ticket',
'FOX Sports San Diego','FOX Sports South','FOX Sports Southeast','FOX Sports Southwest','FOX Sports Sun','FOX Sports Tennessee','FOX Sports West','FOX Sports Wisconsin')) then NULL
when month > '2020-06-01' and network_owner = 'Turner Networks' then NULL
when month >= '2020-01-01' and month <= '2020-07-01' and network_owner = 'Disney' then NULL
else network_owner end as network_owner, sum(t2.duration)/3600 as stream_hours
from daily_paying_users t1
inner join viewership t2 on t1.account_code=t2.user_id and t1.day=t2.day 
and date_trunc(t1.day,month)=t2.month
group by 1, 2, 3, 4,5
),
users1 as
(
select distinct t1.EOP, account_code, month, station_mapping,network_owner
from daily_hours_streamed t1
where stream_hours >= 1
),
users as (
select distinct EOP, account_code, month, station_mapping,network_owner
from users1
),
denominator as (
select sum(billing_days) as total_billing_days, EOP, month, 'viewed' as type 
from paying_days
group by 2,3
),
numerator as (
select   t1.month, t1.account_code , station_mapping, network_owner, case when t2.account_code is null then 0 else t1.billing_days end as streamed
From paying_days t1
left outer join users t2 on t1.account_code = t2.account_code and t1.month=t2.month
),
numerator2 as (
select sum(streamed) as total_streamed, station_mapping, network_owner, month, 'viewed' as type
from numerator
group by 2,3,4
),
percent_watched as (
select EOP, t1.month, t1.station_mapping, t1.network_owner, round((t1.total_streamed/t2.total_billing_days),2) as pct_watched
from numerator2 t1
inner join denominator t2 on t1.type = t2.type and t1.month=t2.month 
),
/* Hours/sub by Genre*/
daily_paying_users_combined as (
select EOP, date_trunc(day, month) month, day, count(distinct account_code) as daily_paying_users_combined
from daily_paying_users
group by 1, 2, 3
),
daily_hours_stream as (
select day, date_trunc(day,month) month, station_mapping,
case when month <= '2019-12-01' and network_owner = 'Disney' then 'FOX'
when month > '2019-12-01' and network_owner = 'FOX' AND (station_mapping IN ('Fox SportsTime Ohio','BabyTV','The CW','YES Network','FOX Sports Arizona','FOX Sports Carolinas','FOX Sports Cincinnati',
'FOX Sports Detroit','FOX Sports Florida','FOX Sports Indiana','FOX Sports Kansas City','FOX Sports Midwest','FOX Sports New Orleans','FOX Sports North','FOX Sports Ohio','FOX Sports Oklahoma','FOX Sports Prime Ticket',
'FOX Sports San Diego','FOX Sports South','FOX Sports Southeast','FOX Sports Southwest','FOX Sports Sun','FOX Sports Tennessee','FOX Sports West','FOX Sports Wisconsin')) then NULL
when month > '2020-06-01' and network_owner = 'Turner Networks' then NULL
when month >= '2020-01-01' and month <= '2020-07-01' and network_owner = 'Disney' then NULL
else network_owner end as network_owner, sum(duration)/3600 as stream_hours
from viewership
group by 1, 2, 3, 4
),
combined as (
Select EOP, t1.day, t1.month, station_mapping, network_owner, t2.stream_hours/t1.daily_paying_users_combined as hours_per_user
from daily_paying_users_combined t1
inner join daily_hours_stream t2 on t1.day=t2.day and t1.month=t2.month 
),
hours_sub as (
select EOP, month, station_mapping, network_owner, round(sum(hours_per_user),2) as hrs_per_sub
from combined
group by 1, 2, 3, 4
)
select EOP, h.month,h.station_mapping, h.network_owner, Hours, uniques, round(hours/uniques,2) as hours_per_viewer, pct_watched, hrs_per_sub
from hours_uniques h
inner join percent_watched p using(EOP,month,station_mapping,network_owner)
inner join hours_sub s using(EOP, month,station_mapping,network_owner)
order by 2 asc