-- Holiday Movie Request

with users as (
Select t1.account_code,t2.user_id
from `fubotv-prod.data_insights.daily_status_static_update` t1
inner join `fubotv-dev.business_analytics.account_code_mapping` t2 on t1.account_code=t2.account_code
where final_status_v2_sql like '%paid%'
and day = '2020-11-17'
-- only US english 
and (plan_code in ('fubotv-basic','fubo-extra')
or plan_code like '%Bundle%')
and lower(plan_code) not like '%latino%'
and lower(plan_code_invoice) in ('fubotv-basic','fubo-extra')

/* US english and Latino plans 

),

hours_1 as(
Select
t1.user_id,sum(duration)/3600 as viewing_hours
from `fubotv-prod.data_insights.video_in_progress_via_va_view` t1
inner join users t2 on t1.user_id=t2.user_id
inner join `fubotv-prod.data_insights.tmsid_genre_mapping` t3 on t1.tms_id=t3.tms_id
WHERE t3.tms_id IS NOT NULL
AND UPPER(t3.tms_id) NOT IN ("","00000000000000","TMS-ID-UNAVAILABLE")
and lower(t1.genres) like '%holiday%'
and date(start_time) >= '2020-10-26'
and date(start_time) < '2020-11-16'
and t1.program_title not in ("A Valentine's Match",
'A Walton Easter',
'American Horror House',
'Be My Valentine',
'Boo! A Madea Halloween',
'Candy Corn',
'Easter Under Wraps',
'Edward Scissorhands',
'Good Witch Halloween',
'Good Witch: Curse From a Rose',
'Good Witch: Secrets of Grey House',
'Good Witch: Spellbound',
'Goosebumps 2: Haunted Halloween',
'Grave Halloween',
'Gremlins',
'Halloween',
'Halloween',
'Halloween 4',
'Halloween 4: The Return of Michael Myers',
'Halloween 5',
'Halloween 5: The Revenge of Michael Myers',
'Halloween 6',
'Halloween 6: The Curse of Michael Myers',
'Halloween H20: 20 Years Later',
'Halloween II',
'Halloween: Resurrection',
'Hell Fest',
'House of the Witch',
'Jersey Shore Shark Attack',
'My Bloody Valentine',
"The Good Witch's Gift",
'The Hollow',
'The Lost Valentine',
"Tyler Perry's Boo 2! A Madea Halloween",
"Tyler Perry's Boo! A Madea Halloween",
'Valentine Ever After',
'Very, Very Valentine')
group by 1
)
Select count(distinct user_id) as users
from hours_1