/*WITH subs AS
(
    Select distinct t3.user_id as fubo_account_code,t2.email as email
    from `fubotv-prod.data_insights.daily_status_static_update` t1 
    inner join `fubotv-dev.business_analytics.email_mapping` t2 on t1.account_code=t2.account_code
    inner join `fubotv-dev.business_analytics.account_code_mapping` t3 on t1.account_code=t3.account_code
    where t1.day = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
)
, v4_logs AS 
(
    SELECT DISTINCT user_id, platform_device_id as device_id
    FROM `fubotv-prod.dmp.de_sync_freewheel_logs` t1
    inner join `fubotv-prod.fw_test.Fw_log_Transaction_id_Test` t2 ON t1.transaction_id = t2.transaction_id
    WHERE user_id IS NOT NULL
)

Select DISTINCT device_id , TO_HEX(MD5(LOWER(email))) as md5_hash_email
FROM subs t1
LEFT OUTER JOIN v4_logs t2 on t2.user_id = t1.fubo_account_code
WHERE device_id IS NOT NULL
--ORDER BY 2

-- FOR device id
 REGEXP_EXTRACT(url, r"_fw_did=([^&]*)") AS device_id,

--using v4 logs
 WITH subs AS
(
    Select distinct t3.user_id as fubo_account_code,t2.email as email
    from `fubotv-prod.data_insights.daily_status_static_update` t1 
    inner join `fubotv-dev.business_analytics.email_mapping` t2 on t1.account_code=t2.account_code
    inner join `fubotv-dev.business_analytics.account_code_mapping` t3 on t1.account_code=t3.account_code
    where t1.day = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
)
, v4_logs AS 
(
    SELECT DISTINCT t2.user_id, t2.user_device_id as device_id
    FROM `fubotv-prod.dmp.de_sync_freewheel_logs` t1
    inner join `fubotv-prod.fw_test.Fw_log_Transaction_id_Test` t2 ON t1.transaction_id = t2.transaction_id
    WHERE user_id IS NOT NULL

)

Select DISTINCT device_id , 
TO_HEX(MD5(LOWER(email))) as md5_hash_email
FROM subs t1
LEFT OUTER JOIN v4_logs t2 on t2.user_id = t1.fubo_account_code
WHERE device_id IS NOT NULL
AND device_id NOT IN ('x','1','0','', ' ')
ORDER BY 2

-- using segment_realtime_other table
WITH subs AS
(
    Select distinct t3.user_id as fubo_account_code,t2.email as email
    from `fubotv-prod.data_insights.daily_status_static_update` t1 
    inner join `fubotv-dev.business_analytics.email_mapping` t2 on t1.account_code=t2.account_code
    inner join `fubotv-dev.business_analytics.account_code_mapping` t3 on t1.account_code=t3.account_code
    where t1.day = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
)
, Segment_Realtime_Info AS (
SELECT CASE
      WHEN userId IS NOT NULL AND userId != '' AND userId != 'None' THEN userId
      ELSE properties_unique_id
      END AS user_id, 
properties_device_id
FROM `fubotv-prod.upload_segment_realtime.other` 
WHERE DATE(receivedat) >= '2020-01-01'
)

Select DISTINCT properties_device_id as device_id, 
TO_HEX(MD5(LOWER(email))) as md5_hash_email
FROM subs t1
LEFT OUTER JOIN Segment_Realtime_Info t2 on t2.user_id = t1.fubo_account_code
WHERE properties_device_id IS NOT NULL
AND properties_device_id NOT IN ('x','1','0','', ' ')
ORDER BY 2

-- Using v4 logs - FOR IP, devices and user agentss

WITH dev1 AS
(
SELECT
count(*) as count,
REGEXP_EXTRACT(url, r"_fw_did=([^&]*)") AS device_id,
REGEXP_EXTRACT(url, r"_fw_ip=([^&]*)") AS ip_address,
timestamp AS timestamp,
request_user_agent AS ua
FROM `fubotv-prod.AdBeaconing.prod_logs` 
WHERE _PARTITIONTIME BETWEEN TIMESTAMP("2021-05-16") AND TIMESTAMP("2021-05-17")
AND url LIKE '/fubo_imp%eventType=defaultImpression%'
GROUP BY 2,3,4,5
)

,dev2 AS
(
SELECT
count(*) as count,
REGEXP_EXTRACT(url, r"_fw_did_idfa=([^&]*)") AS device_id,
REGEXP_EXTRACT(url, r"_fw_ip=([^&]*)") AS ip_address,
timestamp AS timestamp,
request_user_agent AS ua
FROM `fubotv-prod.AdBeaconing.prod_logs` 
WHERE _PARTITIONTIME BETWEEN TIMESTAMP("2021-05-16") AND TIMESTAMP("2021-05-17")
AND url LIKE '/fubo_imp%eventType=defaultImpression%'
GROUP BY 2,3,4,5
)

SELECT REGEXP_REPLACE(device_id, r'(.*?)\%3A(.*?)', '') as device_id, ip_address,timestamp,ua 
FROM 
(
SELECT DISTINCT t1.*
FROM dev1 t1 
UNION ALL
SELECT DISTINCT t2.*
FROM dev2 t2
)
WHERE device_id IS NOT NULL 
AND device_id NOT IN ('x','1','0','', ' ')
*/

/*
--using v4 logs - hashed emails and devices 

 
 WITH subs AS
(
    Select distinct t3.user_id as fubo_account_code,t2.email as email
    from `fubotv-prod.data_insights.daily_status_static_update` t1 
    inner join `fubotv-dev.business_analytics.email_mapping` t2 on t1.account_code=t2.account_code
    inner join `fubotv-dev.business_analytics.account_code_mapping` t3 on t1.account_code=t3.account_code
    where t1.day = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
)
, v4_logs AS 
(
    SELECT DISTINCT t2.user_id, t2.user_device_id as device_id
    FROM `fubotv-prod.dmp.de_sync_freewheel_logs` t1
    inner join `fubotv-prod.FW_Views.FW_Logs_View` t2 ON t1.transaction_id = t2.transaction_id
    WHERE user_id IS NOT NULL

)

,reg_dev_ids AS
(
Select DISTINCT device_id , 
TO_HEX(MD5(LOWER(email))) as md5_hash_email
FROM subs t1
LEFT OUTER JOIN v4_logs t2 on t2.user_id = t1.fubo_account_code
WHERE device_id IS NOT NULL
AND device_id NOT IN ('x','1','0','', ' ')
and LOWER(device_id) NOT LIKE ('%\\%3a%') -- amazon_advertising_id%3Abb8e1c97-9889-4460-89ae-9d95ff1c29d7

ORDER BY 2
)

,other_dev_ids_1 AS (
SELECT
REGEXP_REPLACE(device_id, r'(.*?)\%3AX(.*?)', '') as device_id , md5_hash_email
FROM 
(
SELECT 
device_id,
TO_HEX(MD5(LOWER(email))) as md5_hash_email
FROM subs t1
LEFT OUTER JOIN v4_logs t2 on t2.user_id = t1.fubo_account_code
WHERE device_id IS NOT NULL
AND device_id NOT IN ('x','1','0','', ' ')
and LOWER(device_id) like ("%\\%3ax%")
)
)

,other_dev_ids_2 AS (
SELECT
REGEXP_REPLACE(device_id, r'(.*?)\%3A(.*?)', '') as device_id , md5_hash_email
FROM 
(
SELECT 
device_id,
TO_HEX(MD5(LOWER(email))) as md5_hash_email
FROM subs t1
LEFT OUTER JOIN v4_logs t2 on t2.user_id = t1.fubo_account_code
WHERE device_id IS NOT NULL
AND device_id NOT IN ('x','1','0','', ' ')
and LOWER(device_id) like ("%\\%3a%")
)
)

SELECT t1.*
FROM reg_dev_ids t1
UNION DISTINCT
SELECT t2.* 
FROM other_dev_ids_1 t2
UNION DISTINCT
SELECT t3.*
FROM other_dev_ids_2 t3

*/

------------------------------------------------------- Updated on June 1 2022 to include Plan_Type ----------------------------------------------------------------------
WITH subs AS
(
    Select distinct t3.user_id as fubo_account_code, plan_code, t2.email as email
    from `fubotv-prod.data_insights.daily_status_static_update` t1
    inner join `fubotv-dev.business_analytics.email_mapping` t2 on t1.account_code=t2.account_code
    inner join `fubotv-dev.business_analytics.account_code_mapping` t3 on t1.account_code=t3.account_code
    where t1.day = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
    AND LOWER(plan_code) not like '%spain%'
)
, v4_logs AS
(
    SELECT DISTINCT t2.user_id, t2.user_device_id as device_id
    FROM `fubotv-prod.dmp.de_sync_freewheel_logs_bak` t1
    inner join `fubotv-prod.FW_Views.FW_Logs_View` t2 ON t1.transaction_id = t2.transaction_id
    WHERE user_id IS NOT NULL

)

,reg_dev_ids AS
(
Select DISTINCT device_id,
plan_code,
TO_HEX(MD5(LOWER(email))) as md5_hash_email
FROM subs t1
LEFT OUTER JOIN v4_logs t2 on t2.user_id = t1.fubo_account_code
WHERE device_id IS NOT NULL
AND device_id NOT IN ('x','1','0','', ' ')
and LOWER(device_id) NOT LIKE ('%\\%3a%') -- amazon_advertising_id%3Abb8e1c97-9889-4460-89ae-9d95ff1c29d7

ORDER BY 2
)

,other_dev_ids_1 AS (
SELECT
REGEXP_REPLACE(device_id, r'(.*?)\%3AX(.*?)', '') as device_id , plan_code, md5_hash_email
FROM
(
SELECT
device_id,
plan_code,
TO_HEX(MD5(LOWER(email))) as md5_hash_email
FROM subs t1
LEFT OUTER JOIN v4_logs t2 on t2.user_id = t1.fubo_account_code
WHERE device_id IS NOT NULL
AND device_id NOT IN ('x','1','0','', ' ')
and LOWER(device_id) like ("%\\%3ax%")
)
)

,other_dev_ids_2 AS (
SELECT
REGEXP_REPLACE(device_id, r'(.*?)\%3A(.*?)', '') as device_id , plan_code, md5_hash_email
FROM
(
SELECT
device_id,
plan_code,
TO_HEX(MD5(LOWER(email))) as md5_hash_email
FROM subs t1
LEFT OUTER JOIN v4_logs t2 on t2.user_id = t1.fubo_account_code
WHERE device_id IS NOT NULL
AND device_id NOT IN ('x','1','0','', ' ')
and LOWER(device_id) like ("%\\%3a%")
)
)

, data AS (
SELECT t1.*
FROM reg_dev_ids t1
UNION DISTINCT
SELECT t2.*
FROM other_dev_ids_1 t2
UNION DISTINCT
SELECT t3.*
FROM other_dev_ids_2 t3
WHERE device_id NOT IN ('X')
)

SELECT DISTINCT device_id, md5_hash_email, 
CASE WHEN LOWER(plan_code) LIKE "%latino%" THEN "latino"
ELSE "english" END AS plan_type
FROM data