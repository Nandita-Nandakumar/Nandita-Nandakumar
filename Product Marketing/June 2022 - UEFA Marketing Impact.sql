---------------------------------------------------------------- Product Marketing - UEFA June 2022 --------------------------------------------------------------------------
WITH Uefa_June_List AS 
(
  SELECT DISTINCT *
  FROM `fubotv-dev.product_marketing.NN_June2022_UEFA_emails`
)

, current_subs AS (
  SELECT DISTINCT t2.user_id, Date as email_date, email_name, open_type
  FROM `fubotv-prod.data_insights.daily_status_static_update` t1
  INNER JOIN `fubotv-dev.business_analytics.account_code_mapping` t2 on t1.account_code=t2.account_code
  INNER JOIN `fubotv-dev.business_analytics.email_mapping` t4 ON t2.account_code = t4.account_code
  INNER JOIN Uefa_June_List t3 ON t4.email = t3.email
  WHERE LOWER(final_status_v2_sql) like ('paid%')
  AND day = current_date()-2
  ORDER BY 2 DESC
  )

  /*
  SELECT COUNT( DISTINCT user_id )
  FROM current_subs
  */
  
, viewership AS (
  SELECT DISTINCT user_id, Hours, match_count
  FROM
  (
  SELECT DISTINCT user_id, SUM(duration)/3600 AS Hours, COUNT(DISTINCT t1.tms_id) as match_count
  FROM `fubotv-prod.data_insights.video_in_progress_via_va_view` t1
  JOIN `fubotv-dev.business_analytics.uefa_nations_league_asset` t3 ON t1.tms_id = t3.tms_id  and date(t1.start_time, 'EST') = t3.date_matchday 
  WHERE DATE(start_time, 'EST') >= '2022-06-01'
  GROUP BY 1
  ORDER BY 1
  )
)

SELECT COUNT(DISTINCT t1.user_id) as uniques, AVG(Hours) as avg_hours, AVG(t1.match_count) as avg_matches
FROM viewership t1
LEFT JOIN current_subs t2 ON t1.user_id = t2.user_id -- make this inner join
WHERE t2.user_id IS NULL -- take this out if the aboe is inner join




--------------------------------------------------------------- Product Marketing WINBACK - UEFA June 2022 --------------------------------------------------------------------------
WITH Uefa_June_List AS 
(
  SELECT DISTINCT *
  FROM `fubotv-dev.product_marketing.NN_June2022_UEFA_emails`
  WHERE LOWER(Email_Name) = 'winback'
)



, current_subs AS (
  SELECT DISTINCT t2.user_id, Date as email_date, email_name, open_type, plan_code
  FROM `fubotv-prod.data_insights.daily_status_static_update` t1
  INNER JOIN `fubotv-dev.business_analytics.account_code_mapping` t2 on t1.account_code=t2.account_code
  INNER JOIN `fubotv-dev.business_analytics.email_mapping` t4 ON t2.account_code = t4.account_code
  INNER JOIN Uefa_June_List t3 ON t4.email = t3.email
  WHERE LOWER(final_status_v2_sql) like ('paid%')
  AND day = current_date()-2
  ORDER BY 2 DESC
  )


  SELECT COUNT( DISTINCT user_id )
  FROM current_subs

---------------------------------------------- Product Marketing - FTP Attribution --- UEFA June 2022 --------------------------------------------------------------
WITH Uefa_June_List AS 
(
  SELECT DISTINCT *
  FROM `fubotv-dev.product_marketing.NN_June2022_UEFA_emails`
  WHERE LOWER(Email_Name) = 'ftpbanner'
)



, current_subs AS (
  SELECT DISTINCT t2.user_id, Date as email_date, email_name, open_type
  FROM `fubotv-prod.data_insights.daily_status_static_update` t1
  INNER JOIN `fubotv-dev.business_analytics.account_code_mapping` t2 on t1.account_code=t2.account_code
  INNER JOIN `fubotv-dev.business_analytics.email_mapping` t4 ON t2.account_code = t4.account_code
  INNER JOIN Uefa_June_List t3 ON t4.email = t3.email
  WHERE LOWER(final_status_v2_sql) like ('paid%')
  AND day = current_date()-2
  ORDER BY 2 DESC
  )

  , played_ftp AS (
SELECT DISTINCT
timestamp,
userid,
properties_device_category AS device_category,
properties_profile_id AS profile_id,
properties_data_event_context_program_id AS tms_id,
properties_data_event_context_ftp_game_question_id AS ftp_question_id
FROM `fubotv-prod.data_engineering.de_archiver_other` t1
JOIN `fubotv-dev.business_analytics.uefa_nations_league_asset` t3 ON t1.properties_data_event_context_program_id = t3.tms_id
WHERE receivedat >= '2022-06-01' and receivedat <= '2022-06-20'
AND lower(properties_device_category) IN ('roku','web','iphone','android_tv','android_phone','android_tablet', 'ipad', 'fire_tv', 'smart_tv')
AND event IN ('ui_interaction')
AND properties_event_sub_category IN ('ftp_game')
AND properties_data_event_context_component = 'ftp_game_question'  
AND userid IS NOT NULL
AND properties_data_event_context_program_id IN ( 'EP028519650345' , 
'EP028519650381',
'EP028519650410'
-- 'EP028519650423' -- This game wasn't promoted
)
  )

SELECT DISTINCT user_id, COUNT(DISTINCT t2.ftp_question_id) as questions_answered, COUNT(DISTINCT tms_id) as matches_played
FROM current_subs t1
JOIN played_ftp t2 ON t1.user_id = t2.userid
GROUP BY 1