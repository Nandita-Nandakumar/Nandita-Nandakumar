---------------------------------------------------------------------------------------- NN - FW Logs -----------------------------------------------------------------------------------------------
# Scheduled Query + Table :  fubotv-dev.business_analytics.NN_FW_Impressions_Station_PlanCode 

WITH reseller_data AS
(
    SELECT DISTINCT DATE(PARSE_TIMESTAMP('%Y%m%d%H%M%S',event_time), 'America/New_York') as event_date, transaction_id, selling_partner_id as reseller_id, COUNT (*) as impressions
    FROM `fubotv-prod.dmp.de_sync_freewheel_logs_bak` t1
    WHERE DATE(PARSE_TIMESTAMP('%Y%m%d%H%M%S',event_time), 'America/New_York') >= '2021-03-08'
    AND event_name = 'defaultImpression' -- sum of default impressions 
    AND sales_channel_type = 'Reseller Sold' --Reseller
    AND selling_partner_id IS NOT NULL 
    AND selling_partner_id NOT IN ('516827','512091','519508','393638','515272','512022','511438','511999') -- resellers that are excluded
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
    FROM `fubotv-prod.dmp.de_sync_freewheel_logs_bak` t1
    WHERE DATE(PARSE_TIMESTAMP('%Y%m%d%H%M%S',event_time), 'America/New_York') >= '2021-03-08'
    AND event_name = 'defaultImpression' -- sum of default impressions 
    AND sales_channel_type = 'Direct Sold' --Direct partner
    AND selling_partner_id IS NULL 
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
    station_id,
    user_id as account_code
    FROM `fubotv-prod.FW_Views.FW_Logs_View_EST` 
    ORDER BY 1
)

, v4_log_data AS 
(
    SELECT DISTINCT 
    t1.event_date, 
    account_code, 
    station_id,
    SUM(impressions) AS impressions
    FROM log_info t1
    INNER JOIN user_info t2 USING (event_date, transaction_id)
    GROUP BY 1,2,3
)

,daily_status_data AS
(

    SELECT DISTINCT 
    day, 
    t2.account_code, 
    t1.plan_code,
    plan_type,
    FROM `fubotv-prod.data_insights.daily_status_static_update` t1
    INNER JOIN `fubotv-dev.business_analytics.account_code_mapping` t2 ON t1.account_code = t2.user_id
    INNER JOIN `fubotv-prod.business_analytics.NN_plancode_to_plantype_v2` t3 ON t1.plan_code = t3.plan_code
    ORDER BY 2, 1 DESC
)

, station_details AS (
    SELECT distinct CAST(station_id AS string ) as station_id , station_name, station_mapping, network_owner, fast_channel
    FROM `fubotv-prod.dmp.de_sync_stations`
    JOIN `fubotv-dev.business_analytics.station_mapping_table_51220` USING (station_name)
)

, consolidated_data AS (
SELECT DISTINCT 
event_date, 
plan_code, 
plan_type,
t1.station_id, 
SUM(impressions) AS impressions
FROM v4_log_data t1
INNER JOIN daily_status_data t2 ON (t2.day = t1.event_date AND t1.account_code = t2.account_code)
--INNER JOIN station_details t3 ON CAST(t1.station_id AS STRING) = t3.station_id
GROUP BY 1,2,3,4
ORDER BY 1 DESC
)

, all_data AS (
SELECT t1.*, 
CASE WHEN t2.station_id = '131921' THEN "fubo Latino Network"
WHEN t2.station_id = '81763' THEN "BabyTV Spanish"
WHEN t2.station_id = '93687' THEN "Paramount Network (Canada)"
WHEN t2.station_id = '726500001' THEN "MLB Network (Canada)"
WHEN t2.station_id = '794560001' THEN "Benfica TV (Canada)"
WHEN t2.station_id = '123852' THEN "fubo Sports Network 2 (Canada)"
WHEN t2.station_id = '887490001' THEN "beIN SPORTS (Canada)"
WHEN t2.station_id = '1042130001' THEN "The Fight Network (Canada)"
WHEN t2.station_id = '1085390001' THEN "GAME+ (Canada)"
WHEN t2.station_id = '1201230001' THEN "fubo Sports Network (Canada)"
WHEN t2.station_id = '79670' THEN "SHOxBET"
WHEN t2.station_id = '78575' THEN "Magnolia Network"
WHEN t2.station_id = '95030' THEN "GAC Family"
WHEN t2.station_id = '887540001' THEN "beIN SPORTS En Espa√±ol (Canada)"
ELSE station_mapping
END AS station_mapping,
t2.network_owner,
fast_channel,
FROM consolidated_data t1
JOIN station_details t2 ON CAST(t1.station_id AS STRING) = t2.station_id
)

SELECT DISTINCT t1.* EXCEPT (network_owner, fast_channel), 
CASE WHEN station_mapping = "beIN SPORTS (Canada)" THEN "beIN SPORTS (Canada)"
WHEN station_mapping = "beIN SPORTS En Espa√±ol (Canada)" THEN "beIN SPORTS (Canada)"
WHEN station_mapping = "BabyTV Spanish" THEN "FOX"
WHEN station_mapping = "My Network TV" THEN "CBS"
WHEN station_mapping = "GAC Family" THEN "GAC Media"
ELSE network_owner 
END AS Network_Name,
CASE WHEN fast_channel = true THEN "yes" ElSE "no" END AS fast_channel_group
FROM all_data t1
WHERE event_date >= '2021-04-01'
AND event_date <= current_date()-1
ORDER BY 1 DESC
