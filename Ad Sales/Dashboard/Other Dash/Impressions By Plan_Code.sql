WITH reseller_data AS
(
    SELECT DISTINCT DATE(PARSE_TIMESTAMP('%Y%m%d%H%M%S',event_time), 'America/New_York') as event_date, transaction_id, selling_partner_id as reseller_id, COUNT (*) as impressions
    FROM `fubotv-prod.dmp.de_sync_freewheel_logs` t1
    WHERE DATE(PARSE_TIMESTAMP('%Y%m%d%H%M%S',event_time), 'America/New_York') >= '2021-04-01'
    AND event_name = 'defaultImpression' -- sum of default impressions 
    AND sales_channel_type = 'Reseller Sold' --Reseller
    AND selling_partner_id IS NOT NULL 
    AND selling_partner_id NOT IN ('516827','512091','519508','393638','515272','512022','511438','511999') -- resellers that are excluded
    AND site_section_id NOT IN('16100680','16193797','16192906','16194461','16194463','16194481','16197756','16197766','16214682') -- removing fsn
    GROUP BY 1,2,3
    ORDER By 3 DESC
)

,reseller_info AS
(
    SELECT event_date, transaction_id, SUM(impressions) as impressions
    FROM reseller_data
    GROUP BY 1,2
    ORDER BY 1 DESC
)

,direct_data AS
(
    SELECT DISTINCT DATE(PARSE_TIMESTAMP('%Y%m%d%H%M%S',event_time), 'America/New_York') as event_date, transaction_id, COUNT (*) as impressions
    FROM `fubotv-prod.dmp.de_sync_freewheel_logs` t1
    WHERE DATE(PARSE_TIMESTAMP('%Y%m%d%H%M%S',event_time), 'America/New_York') >= '2021-04-01'
    AND event_name = 'defaultImpression' -- sum of default impressions 
    AND sales_channel_type = 'Direct Sold' --Direct partner
    AND selling_partner_id IS NULL 
    AND site_section_id NOT IN('16100680','16193797','16192906','16194461','16194463','16194481','16197756','16197766','16214682') -- removing fsn
    AND placement_id NOT IN ('28357916','28357921','44015286','44015291','44025664','44025669','44026928','51228140','51255705','-1') -- removing house ads and others
    AND placement_id IS NOT NULL
    GROUP BY 1,2
    ORDER By 1 DESC
)

,direct_info AS 
(
    SELECT DISTINCT event_date, transaction_id, SUM(impressions) as impressions
    FROM direct_data t1
    GROUP BY 1,2
    ORDER BY 1 DESC
)

,log_info_temp AS 
(
    SELECT DISTINCT t1.*
    FROM reseller_info t1
    UNION ALL 
    SELECT DISTINCT t2.*
    FROM direct_info t2
)

,log_info AS 
(
    SELECT DISTINCT event_date, transaction_id, SUM(impressions) as impressions
    FROM log_info_temp 
    GROUP BY 1,2
    ORDER BY 1 DESC
)

, user_info AS
(
    SELECT DISTINCT 
    event_date, 
    transaction_id, 
    user_id as account_code
    FROM `fubotv-prod.FW_Views.FW_Logs_View_EST` 
    ORDER BY 1
)

, v4_log_data AS 
(
    SELECT DISTINCT 
    t1.event_date, 
    account_code, 
    SUM(impressions) AS impressions
    FROM log_info t1
    INNER JOIN user_info t2 USING (event_date, transaction_id)
    GROUP BY 1,2
)

,v4_log_users AS
(
    SELECT DISTINCT event_date, COUNT(account_code) as total_users
    FROM v4_log_data 
    GROUP BY 1
)

,daily_status_data AS
(

    SELECT DISTINCT 
    day, 
    t2.account_code, 
    plan_code,
    plan_name,
    FROM `fubotv-prod.data_insights.daily_status_static_update` t1
    INNER JOIN `fubotv-dev.business_analytics.account_code_mapping` t2 ON t1.account_code = t2.user_id
    ORDER BY 2, 1 DESC
)

,avg_subs AS
(
    SELECT t1.day as Sub_Date, plan_code, plan_name, count(DISTINCT(t1.account_code)) AS Sub_Count
    FROM `fubotv-prod.data_insights.daily_status_static_update` t1
    WHERE LOWER(final_status_restated) IN ('paid', 'paid and scheduled pause','paid but canceled')
    GROUP BY 1,2,3
    ORDER BY 1 DESC
)

SELECT DISTINCT 
t1.event_date, 
plan_code, 
plan_name,
COUNT(total_users) as total_users,
COUNT(Sub_Count) as Sub_counts,
SUM(impressions) AS impressions
FROM v4_log_data t1
INNER JOIN daily_status_data t2 ON (t2.day = t1.event_date AND t1.account_code = t2.account_code)
INNER JOIN v4_log_users  t3 ON t1.event_date = t3.event_date
INNER JOIN avg_subs t4 on t1.event_date = t2.Sub_Date
# WHERE t1.event_date <= '2021-06-28' #adjust date
GROUP BY 1,2
ORDER BY 1 DESC

-------------------------------- NEW ---------------------------------------------------------------------

# Impression by Plan Code and Plan Name

WITH reseller_data AS
(
    SELECT DISTINCT DATE(PARSE_TIMESTAMP('%Y%m%d%H%M%S',event_time), 'America/New_York') as event_date, transaction_id, selling_partner_id as reseller_id, COUNT (*) as impressions
    FROM `fubotv-prod.dmp.de_sync_freewheel_logs` t1
    WHERE DATE(PARSE_TIMESTAMP('%Y%m%d%H%M%S',event_time), 'America/New_York') >= '2021-04-01'
    AND event_name = 'defaultImpression' -- sum of default impressions 
    AND sales_channel_type = 'Reseller Sold' --Reseller
    AND selling_partner_id IS NOT NULL 
    AND selling_partner_id NOT IN ('516827','512091','519508','393638','515272','512022','511438','511999') -- resellers that are excluded
    AND site_section_id NOT IN('16100680','16193797','16192906','16194461','16194463','16194481','16197756','16197766','16214682') -- removing fsn
    GROUP BY 1,2,3
    ORDER By 3 DESC
)
,reseller_info AS
(
    SELECT event_date, transaction_id, SUM(impressions) as impressions
    FROM reseller_data
    GROUP BY 1,2
    ORDER BY 1 DESC
)
,direct_data AS
(
    SELECT DISTINCT DATE(PARSE_TIMESTAMP('%Y%m%d%H%M%S',event_time), 'America/New_York') as event_date, transaction_id, COUNT (*) as impressions
    FROM `fubotv-prod.dmp.de_sync_freewheel_logs` t1
    WHERE DATE(PARSE_TIMESTAMP('%Y%m%d%H%M%S',event_time), 'America/New_York') >= '2021-04-01'
    AND event_name = 'defaultImpression' -- sum of default impressions 
    AND sales_channel_type = 'Direct Sold' --Direct partner
    AND selling_partner_id IS NULL 
    AND site_section_id NOT IN('16100680','16193797','16192906','16194461','16194463','16194481','16197756','16197766','16214682') -- removing fsn
    AND placement_id NOT IN ('28357916','28357921','44015286','44015291','44025664','44025669','44026928','51228140','51255705','-1') -- removing house ads and others
    AND placement_id IS NOT NULL
    GROUP BY 1,2
    ORDER By 1 DESC
)
,direct_info AS 
(
    SELECT DISTINCT event_date, transaction_id, SUM(impressions) as impressions
    FROM direct_data t1
    GROUP BY 1,2
    ORDER BY 1 DESC
)
,log_info_temp AS 
(
    SELECT DISTINCT t1.*
    FROM reseller_info t1
    UNION ALL 
    SELECT DISTINCT t2.*
    FROM direct_info t2
)
,log_info AS 
(
    SELECT DISTINCT event_date, transaction_id, SUM(impressions) as impressions
    FROM log_info_temp 
    GROUP BY 1,2
    ORDER BY 1 DESC
)
, user_info AS
(
    SELECT DISTINCT 
    event_date, 
    transaction_id, 
    user_id as account_code
    FROM `fubotv-prod.FW_Views.FW_Logs_View_EST` 
    ORDER BY 1
)
, v4_log_data AS 
(
    SELECT DISTINCT 
    t1.event_date, 
    account_code, 
    SUM(impressions) AS impressions
    FROM log_info t1
    INNER JOIN user_info t2 USING (event_date, transaction_id)
    GROUP BY 1,2
)
,v4_log_users AS
(
    SELECT DISTINCT event_date, COUNT(account_code) as total_users
    FROM v4_log_data 
    GROUP BY 1
)
,daily_status_data AS
(
    SELECT DISTINCT 
    day, 
    t2.account_code, 
    plan_code,
    plan_name,
    FROM `fubotv-prod.data_insights.daily_status_static_update` t1
    INNER JOIN `fubotv-dev.business_analytics.account_code_mapping` t2 ON t1.account_code = t2.user_id
    ORDER BY 2, 1 DESC
)
SELECT DISTINCT 
t1.event_date, 
plan_code, 
plan_name,
COUNT(total_users) as total_users,
SUM(impressions) AS impressions
FROM v4_log_data t1
INNER JOIN daily_status_data t2 ON (t2.day = t1.event_date AND t1.account_code = t2.account_code)
INNER JOIN v4_log_users  t3 ON t1.event_date = t3.event_date
# WHERE t1.event_date = '2021-07-12' #adjust date
GROUP BY 1,2,3
ORDER BY 1 DESC