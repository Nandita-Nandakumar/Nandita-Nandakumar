/* Baseball - MLB */
-- Last Updated : 06/29/2022
-- New ask : MLB 60+ split into 2 groups. 
-- Group 1 : Subs in Illinois, Connecticut, Maine, Massachusetts, New Hampshire, Rhode Island, and Vermont
-- Group 2 : Group 2 = Subs NOT in Illinois, Connecticut, Maine, Massachusetts, New Hampshire, Rhode Island, and Vermont (aka everybody who doesn't fall in to group 1)

-- Currently active and Paid - Including Employees
-- Watched MLB over the past 90 days for an average of more than 60 mins per game. 


with active_users as (
select distinct d.account_code, state
from `fubotv-prod.data_insights.daily_status_static_update` d
inner join `fubotv-dev.business_analytics.email_mapping` e using(account_code)
inner join `fubotv-dev.business_analytics.ref_current_paid_users_state_zip` f using (account_code)
where d.day = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) -- June 28th
and d.final_status_v2_sql not in ('expired','paused')
and (d.plan_code in ('fubotv-basic','fubo-extra','family-quarter')
or d.plan_code like '%Bundle%' or lower(d.plan_code) like '%quarter%' or lower(d.plan_code) like '%qrtr%')
and lower(d.plan_code) not like '%latino%' and lower(d.plan_code) not like '%spain%' and lower(d.plan_code) not like '%can%'
and lower(d.plan_code_invoice) in ('fubotv-basic','fubo-extra','fubotv-basic-qrtr')
and e.email not like '%@apple.com'
and e.email not like '%@roku.com'
and e.email is not null
and e.email <> ""
),
coupon_users as (
Select distinct account_code,coupon_code
from `fubotv-prod.data_insights.daily_status_static_update`
where (lower(coupon_code) like '%csteam%'
  or lower(coupon_code) like '%aff201%'
  or lower(coupon_code) like '%voxmedia%'
  or lower(coupon_code) like '%bengrad%'
  or lower(coupon_code) like '%bd%'
  or lower(coupon_code) like '%nctcpartner%'
  or lower(coupon_code) like '%mincoupon%'
  or lower(coupon_code) like '%1yearofffubo%'
  or lower(coupon_code) like '%nbcdemoaccounts%'
  or lower(coupon_code) like '%testing100off%'
  or lower(coupon_code) like '%fan100%'
  or lower(coupon_code) like '%partnergift%'
  or lower(coupon_code) like '%fansided1year%'
  or lower(coupon_code) like '%qaaccounts%'
  or lower(coupon_code) like '%pressaccount%'
  or lower(coupon_code) like '%bbpartners%'
  or lower(coupon_code) like '%pass4revolt%'
  or lower(coupon_code) like '%internalandtesting%'
  or lower(coupon_code) like '%fubo100%'  
  or lower(coupon_code) like '%complimentary%'
  or lower(coupon_code) like '%compedspain%'
  or lower(coupon_code) like '%alabrad%'
  or lower(coupon_code) like '%takedown%'
  or lower(coupon_code) = 'affiliatecomp'
  or lower(coupon_code) = 'partnermonitoring'
  or lower(coupon_code) = 'claccount'
  or lower(coupon_code) = 'partner100-7'
  or lower(coupon_code) = 'partner100-5'
  or lower(coupon_code) = 'compedbizdev'
 )
),
active_users_exclude_coupons as (
Select distinct t1.account_code, state
from active_users t1
left outer join coupon_users t2 on t1.account_code=t2.account_code
where t2.account_code is NULL
)
, viewership as (
SELECT DISTINCT user_id, state, ROUND(AVG(viewing_hours),1) AS Avg_Hours
FROM (
SELECT user_id, state, g.tms_id, sum(duration)/3600 as viewing_hours
FROM `fubotv-prod.data_insights.video_in_progress_via_va_view` t2
inner join active_users_exclude_coupons t1 on  t1.account_code=t2.user_id
inner join `fubotv-prod.data_insights.tmsid_genre_mapping` g on t2.tms_id=g.tms_id
where DATE(start_time) >= '2022-03-12' --- 90 days
AND DATE(start_time) <= '2022-06-12' 
and lower(g.primary_genre) like ('baseball%') 
and lower(t2.league_names) like '%mlb%'
group by 1,2,3
ORDER BY 1,2
)
GROUP BY 1,2
)

, consolidated_data AS (
select DISTINCT user_id, state
from viewership
where Avg_Hours > 1 -- 60 mins average per game
ORDER BY 2 DESC
)

SELECT DISTINCT user_id
FROM consolidated_data
-- WHERE state IN ('IL','CT', 'ME', 'MA', 'NH', 'RI', 'VT') -- Group 1
-- WHERE state NOT IN ('IL','CT', 'ME', 'MA', 'NH', 'RI', 'VT') -- Group 2