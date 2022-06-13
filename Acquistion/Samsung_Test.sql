--NN Test
WITH 
samsung_LP AS
(
SELECT receivedat_date, min(receivedat) receivedat, user_id, properties_event_metadata_referrer affiliate_signup_url, 'metadata_referral' url_field
FROM
(
SELECT DISTINCT DATE(receivedat) receivedat_date, DATE(CAST(properties_event_date AS TIMESTAMP)) event_date, timestamp, receivedat,
  CASE
      WHEN userId IS NOT NULL AND userId != '' AND userId != 'None' THEN userId
      ELSE properties_unique_id
  END AS user_id,
properties_event_metadata_url,
properties_event_metadata_referrer,
FROM `fubotv-prod.upload_segment_realtime.other`
where 
  event='page_view'
  and (lower(properties_event_metadata_referrer) like '%irmp%' or lower(properties_event_metadata_referrer) like '%irad%')
  and DATE(receivedat) BETWEEN DATE('2021-01-21') and CURRENT_DATE()
)
WHERE user_id IS NOT NULL and user_id<>'<nil>'
  and properties_event_metadata_referrer like '%signup%'
  and properties_event_metadata_referrer like '%samsung%'
group by 1,3,4,5
order by 2,1 
)
,
samsung_LP_max AS 
(
  SELECT user_id, max(receivedat_date) receivedat_date, max(receivedat) receivedat 
  from samsung_LP 
  group by 1
)
,
static_update AS
(
  SELECT distinct account_code, trial_started_at
  FROM `fubotv-prod.data_insights.daily_status_static_update`
  WHERE 1=1 
  AND trial_started_at>='2021-01-21'
)

-- Samsung Affiliate Referral to Trial User List

, Exposed_Users AS (
    SELECT t1.receivedat, t1.user_id, t2.trial_started_at, DATETIME_DIFF(trial_started_at, receivedat, minute) LP_trial_diff_minutes,
        CASE 
        WHEN DATETIME_DIFF(trial_started_at, receivedat, minute)<=20160 THEN 'Yes'
        WHEN DATETIME_DIFF(trial_started_at, receivedat, minute)>20160 THEN 'No'
        ELSE 'X'
        END AS DAY_14_TEST
    FROM samsung_LP_max t1 
    LEFT JOIN static_update t2 on t1.user_id=t2.account_code 
)

-- Part 1 of the Analysis

, Event_Page_View AS (
    SELECT DISTINCT DATE(receivedat) receivedat_date, DATE(CAST(properties_event_date AS TIMESTAMP)) event_date, timestamp, receivedat,
        CASE
        WHEN userId IS NOT NULL AND userId != '' AND userId != 'None' THEN userId
        ELSE properties_unique_id
        END AS user_id,
    properties_event_metadata_url,
    properties_event_metadata_referrer,
    FROM `fubotv-prod.upload_segment_realtime.other`
    where 
    event='page_view'
    --and (lower(properties_event_metadata_referrer) like '%irmp%' or lower(properties_event_metadata_referrer) like '%irad%')
    and DATE(receivedat) BETWEEN DATE('2021-01-21') and CURRENT_DATE()
)

, first_sign_up_event AS (
SELECT DISTINCT user_id, receivedat as sign_up_dat, properties_event_metadata_url
FROM 
(
SELECT DISTINCT t1.user_id, t2.receivedat,t2.properties_event_metadata_url, 
row_number() OVER (PARTITION BY  t1.user_id ORDER BY t2.receivedat ASC ) as r,
FROM Exposed_Users  t1
LEFT OUTER JOIN Event_Page_View  t2 ON t1.user_id = t2.user_id 
where  t2.receivedat_date  = date(trial_started_at)
AND  LOWER(properties_event_metadata_url) LIKE '%signup%'
order by 1,2
)
WHERE r = 1
)

,first_connect_event AS (
  SELECT DISTINCT user_id, receivedat as connect_dat, properties_event_metadata_url
FROM 
(
SELECT DISTINCT t1.user_id, t2.receivedat,t2.properties_event_metadata_url, 
row_number() OVER (PARTITION BY  t1.user_id ORDER BY t2.receivedat ASC ) as Order_of_Events,
FROM Exposed_Users  t1
LEFT OUTER JOIN Event_Page_View  t2 ON t1.user_id = t2.user_id 
where  t2.receivedat_date  = date(trial_started_at)
AND  LOWER(properties_event_metadata_url) LIKE '%connect%'
order by 1,2
)
WHERE Order_of_Events = 1
)

, Signup_to_Connect AS (
SELECT DISTINCT t1.user_id, DATETIME_DIFF(connect_dat, sign_up_dat, minute) sign_up_to_connect_diff_minutes,
 CASE 
        WHEN DATETIME_DIFF(connect_dat, sign_up_dat, minute)<=30 THEN 'Yes'
        ELSE 'No'
        END AS CONVERT_WITHIN_30_MINS
FROM first_sign_up_event t1
JOIN first_connect_event t2 ON t1.user_id = t2.user_id
)

-- Part 2 of Analysis

-- Trialers who are on Smart_tv

,Segment_Realtime_Info AS (
SELECT CASE
      WHEN userId IS NOT NULL AND userId != '' AND userId != 'None' THEN userId
      ELSE properties_unique_id
      END AS user_id, 
DATE(receivedat) receivedat_date, 
DATE(CAST(properties_event_date AS TIMESTAMP)) event_date, 
timestamp, 
receivedat,
event,
properties_event_metadata_url,
properties_event_metadata_referrer,
properties_device_category 
FROM `fubotv-prod.upload_segment_realtime.other` 
)

,Trialers_Device_Cat AS (
SELECT DISTINCT t1.user_id, t1.trial_started_at,  properties_device_category
FROM Exposed_Users  t1
INNER JOIN  Segment_Realtime_Info t2 on t1.user_id = t2.user_id
WHERE properties_device_category IS NOT NULL
and LOWER(properties_device_category) = 'smart_tv'
and t1.trial_started_at IS NOT NULL 
ORDER BY 1
)

--Trialers on Smart Tv who visited prior to trial

/*
,num_of_visits_prior_to_trial AS (

SELECT user_id, --receivedat_date,
 sum(r) as number_of_visits_prior_to_trial
from 
(
SELECT DISTINCT t2.user_id, receivedat_date, date(trial_started_at) as trial_started_at_date, 1 as r
FROM Segment_Realtime_Info t1
JOIN Trialers_Device_Cat t2 ON t1.user_id = t2.user_id
WHERE receivedat_date  < DATE(t2.trial_started_at)
and LOWER(t2.properties_device_category) = 'smart_tv'
and event IS NOT NULL
AND receivedat IS NOT NULL
ORDER BY 1,2
)
GROUP BY 1
)
*/

--,visit_dates AS (

SELECT user_id, 
--MIN(receivedat_date) as First_Time_Visit, -- first visit
MAX(receivedat_date) as Latest_Visit, -- latest visit
FROM 
(
SELECT DISTINCT t2.user_id, receivedat_date, date(trial_started_at) as trial_started_at_date, 1 as r
FROM Segment_Realtime_Info t1
JOIN Trialers_Device_Cat t2 ON t1.user_id = t2.user_id
WHERE receivedat_date  < DATE(t2.trial_started_at)
and LOWER(t2.properties_device_category) = 'smart_tv'
and event IS NOT NULL
AND receivedat IS NOT NULL
ORDER BY 1,2
)
GROUP BY 1