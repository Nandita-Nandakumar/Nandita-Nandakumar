with paid_users as
(
Select account_code,date_trunc(sub_first_dt,MONTH) as first_paying_month
from `fubotv-prod.data_insights.daily_status_static_update`
where 1=1
and sub_first_dt >= '2021-01-01' ---- change as needed
and sub_first_dt < '2021-12-01' ----- change as needed
and final_status_restated in ('paid','paid but canceled','paid and scheduled pause')
/*and (plan_code in ('fubo-extra','fubotv-basic') or lower(plan_code) like '%bundle%')
and lower(plan_code) not like '%latino%'
and lower(plan_code_invoice) in ('fubotv-basic','fubo-extra')*/
--AND sub_current_nth_day_sql >= 5
and sub_nth_month_by_firstlast_plan_sql = 1
),
first_view_raw as
(
select user_id, station_mapping, s.network_owner,primary_genre_group,t1.tms_id, playback_type,m.program_title,
row_number() over (partition by user_id order by start_time asc) as row
from `fubotv-prod.data_insights.video_in_progress_via_va_view` t1
inner join `fubotv-prod.data_insights.tmsid_genre_mapping` m on t1.tms_id =m.tms_id
inner join `fubotv-dev.business_analytics.station_mapping_table_51220` s on s.station_name =t1.channel
where 1=1
and duration >= 300
and UPPER(t1.tms_id) NOT IN ("","00000000000000","TMS-ID-UNAVAILABLE")
and t1.tms_id is not null
and lower(playback_type)  like '%live%'
--AND DATE(start_time, 'EST') = '2021-06-03'
),
first_view as
(
select user_id, network_owner,station_mapping,program_title, tms_id
from first_view_raw
where 1=1
and row = 1
)

, first_view_users AS (
SELECT DISTINCT account_code, tms_id as first_viewed_tms_id, program_title as first_viewed_program
FROM 
(
select distinct station_mapping,program_title,tms_id, account_code
from paid_users t1
inner join first_view t2 on t1.account_code=t2.user_id
where 1=1
--and tms_id IN ('EP033149890044','FUBOCOM0000001','FUBO20210528202830','FUBO20210528204447','FUBO20210601193949',
--'FUBO20210603185306','FUBO20210601192345','FUBO20210601192444','FUBO20210601192536','FUBO20210601192633','EP038605420001',
--'EP038758570001','EP038605440001','EP038758620001','EP038758570002','EP038758620002','FUBO20210603185156','EP033149890048',
--'FUBO20210602141534','FUBO20210601190616','FUBO20210601190749','FUBO20210601192718','FUBO20210601192803','FUBO20210601192836',
--'FUBO20210601192918','FUBO20210601192951','EP038605420002','EP038605440002','EP038758570003','EP038758620003') -- full list
and tms_id IN ('FUBO20210603185156','EP033149890048','FUBO20210602141534','FUBO20210601190616','FUBO20210601190749','FUBO20210601192718',
'FUBO20210601192803','FUBO20210601192836','FUBO20210601192918','FUBO20210601192951','EP038605420002','EP038605440002','EP038758570003',
'EP038758620003') -- 06/08 games and shows
--AND tms_id IN ('EP038605420001','EP038758570001','EP033149890044','FUBO20210603185306','FUBOCOM0000001','FUBO20210601192345',
--'FUBO20210528202830','FUBO20210601192444','FUBO20210528204447','FUBO20210601192536','EP038605440001','EP038758620001') -- 06/03 matches
--AND tms_id IN ('FUBO20210601193949','FUBO20210601192633','EP038758570002','EP038758620002') -- 06/04
)
)

, longest_view_raw as (
--SELECT DISTINCT user_id FROM ( -- 1st checkpoint for user count
SELECT user_id, viewing_day, station_mapping, tms_id, program_title, first_viewed_tms_id, first_viewed_program, SUM(hours) AS hours 
FROM 
(
select user_id, date(start_time,'EST') as viewing_day, station_mapping, s.network_owner,t0.first_viewed_tms_id , m.program_title, m.tms_id, first_viewed_program, playback_type,SUM(duration)/3600 as hours
from `fubotv-prod.data_insights.video_in_progress_via_va_view` t1
INNER JOIN first_view_users t0 ON t0.account_code = t1.user_id
inner join `fubotv-prod.data_insights.tmsid_genre_mapping` m on t1.tms_id =m.tms_id
inner join `fubotv-dev.business_analytics.station_mapping_table_51220` s on s.station_name =t1.channel
where 1=1
and UPPER(t1.tms_id) NOT IN ("","00000000000000","TMS-ID-UNAVAILABLE")
and t1.tms_id is not null
and lower(playback_type) LIKE '%live%'
and DATE(start_time,'EST') = '2021-06-08' --change date
GROUP BY 1,2,3,4,5,6,7,8,9
)
GROUP BY 1,2,3,4,5,6,7
ORDER BY 1,2, 8 DESC
)

--SELECT DISTINCT user_id FROM ( -- 3rd checkpoint for user_count
SELECT user_id, viewing_day, station_mapping, tms_id, program_title, first_viewed_tms_id, first_viewed_program , hours
FROM 
(
--SELECT DISTINCT user_id FROM ( -- 2nd checkpoint for user_count
SELECT user_id, viewing_day, station_mapping, tms_id, program_title, first_viewed_tms_id, longest_view_raw.first_viewed_program , SUM(hours) AS hours, 
row_number() over (partition by user_id order by SUM(hours) DESC) as max_hours
FROM longest_view_raw 
GROUP BY 1,2,3,4,5,6,7
ORDER BY 1, 9 ASC
)
--)
WHERE max_hours = 1
--)
