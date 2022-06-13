#IRI
WITH logs AS
(
SELECT
count(*) as count,
REGEXP_EXTRACT(url, r"_fw_vcid2=(?i)fubotv:?%?3?A?([[:alnum:]]{24})") AS user_id,
REGEXP_EXTRACT(url, r"creativeId=([[:alnum:]_]*)") AS creative_id,
REGEXP_EXTRACT(url, r"fw_pl=([^&]*)") AS placement_name,
timestamp as timestamp
FROM `fubotv-prod.AdBeaconing.prod_logs` t1
WHERE DATE(_PARTITIONTIME) BETWEEN "2021-04-19" AND "2021-04-25"
AND resp_status = '200'
AND url LIKE '/fubo_imp%eventType=defaultImpression%'
AND url LIKE '%fw_pl=Axe%'
AND timestamp BETWEEN "2021-04-19T04:00+0000" AND "2021-04-25T03:59:59+0000"
GROUP BY user_id, creative_id, placement_name, timestamp
ORDER BY timestamp desc
)
SELECT t1.count, t1.creative_id, t1.placement_name, t1.timestamp, TO_HEX(MD5(LOWER(t3.email))) as md5_hash_email
FROM logs t1
inner join `fubotv-dev.business_analytics.account_code_mapping` t2 on t2.user_id =t1.user_id
inner join `fubotv-dev.business_analytics.email_mapping` t3 on t2.account_code =t3.account_code

--v4 logs

WITH logs AS (
SELECT DISTINCT 
DATE(PARSE_TIMESTAMP('%Y%m%d%H%M%S',event_time)) as event_date,
all_request_kv,
site_section_id,
transaction_id,
event_type,
event_name,
placement_id,
mrm_rule_id,
ad_unit_id,
creative_id,
creative_duration,
channel_id,
video_asset_id as asset_id,
platform_device_id as device_id, 
content_provider_partner_id,
distribution_partner_id,
selling_partner_id,
sales_channel_type,
count (*) counts
FROM `fubotv-prod.dmp.de_sync_freewheel_logs` t1
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
)
      

, all_info AS 

(
Select t1.*,t2.user_id, t3.placement_name, t4.campaign_id_mrm
from logs t1
inner join `fubotv-prod.fw_test.Fw_log_Transaction_id_Test` t2 ON t1.transaction_id = t2.transaction_id
inner join placement_info t3 ON t1.placement_id = t3.id
inner join campaign_info t4 on t3.id = t4.placement_id_mrm
WHERE LOWER (event_name) = 'defaultimpression'
AND t1.event_date BETWEEN '2021-04-26' AND '2021-05-02'
AND LOWER(placement_name) LIKE '%axe%'
AND user_id IS NOT NULL
)
, email_info AS
(
SELECT t3.account_code, email
FROM `fubotv-dev.business_analytics.account_code_mapping` t2 
inner join `fubotv-dev.business_analytics.email_mapping` t3 on t2.account_code =t3.account_code
)
Select counts, event_date as exposure_date, campaign_id_mrm as campaign_id, placement_id, creative_id, TO_HEX(MD5(LOWER(email))) as md5_hash_email
from all_info t1
inner join email_info t2 on t1.user_id = t2.account_code
order by 2


# with timestamp and not date

WITH logs AS (
SELECT DISTINCT 
event_time,
all_request_kv,
site_section_id,
transaction_id,
event_type,
event_name,
placement_id,
mrm_rule_id,
ad_unit_id,
creative_id,
creative_duration,
channel_id,
video_asset_id as asset_id,
platform_device_id as device_id, 
content_provider_partner_id,
distribution_partner_id,
selling_partner_id,
sales_channel_type,
count (*) counts
FROM `fubotv-prod.dmp.de_sync_freewheel_logs` t1
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
)
,placement_info AS
(
    SELECT DISTINCT t2.placement_id_mrm as id, t2.placement_name as placement_name
    FROM `fubotv-prod.dmp.de_sync_freewheel_reports_custom_audit_report_v2` as t2 
)

,campaign_info AS
(
SELECT DISTINCT placement_id_mrm, campaign_id_mrm
FROM `fubotv-prod.dmp.de_sync_freewheel_reports_custom_audit_report_v2` 
)

, all_info AS 

(
Select t1.*,t2.user_id, t3.placement_name, t4.campaign_id_mrm
from logs t1
inner join `fubotv-prod.fw_test.Fw_log_Transaction_id_Test` t2 ON t1.transaction_id = t2.transaction_id
inner join placement_info t3 ON t1.placement_id = t3.id
inner join campaign_info t4 on t3.id = t4.placement_id_mrm
WHERE LOWER (event_name) = 'defaultimpression'
AND DATE(PARSE_TIMESTAMP('%Y%m%d%H%M%S',event_time)) BETWEEN '2021-04-26' AND '2021-05-02'
AND LOWER(placement_name) LIKE '%axe%'
AND user_id IS NOT NULL
)
, email_info AS
(
SELECT t3.account_code, email
FROM `fubotv-dev.business_analytics.account_code_mapping` t2 
inner join `fubotv-dev.business_analytics.email_mapping` t3 on t2.account_code =t3.account_code
)
Select counts, event_time as exposure_date, campaign_id_mrm as campaign_id, placement_id, creative_id, TO_HEX(MD5(LOWER(email))) as md5_hash_email
from all_info t1
inner join email_info t2 on t1.user_id = t2.account_code
order by 2
