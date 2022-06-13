-- Overall Impression

SELECT DISTINCT DATE(PARSE_TIMESTAMP('%Y%m%d%H%M%S',event_time), 'America/New_York') as event_date, COUNT (*) AS Impressions
FROM `fubotv-prod.dmp.de_sync_freewheel_logs` 
WHERE event_name = 'defaultImpression' -- sum of default impressions 
AND sales_channel_type IS NOT NULL
GROUP BY 1
ORDER BY 1

-- Direct
SELECT DISTINCT DATE(PARSE_TIMESTAMP('%Y%m%d%H%M%S',event_time), 'America/New_York') as event_date, COUNT (*) AS Impressions
FROM `fubotv-prod.dmp.de_sync_freewheel_logs` t1
WHERE DATE(PARSE_TIMESTAMP('%Y%m%d%H%M%S',event_time), 'America/New_York') >= '2021-02-01'
AND DATE(PARSE_TIMESTAMP('%Y%m%d%H%M%S',event_time), 'America/New_York') <= '2021-03-08'
AND event_name = 'defaultImpression' -- sum of default impressions 
AND sales_channel_type = 'Direct Sold' --Direct partner
AND selling_partner_id IS NULL 
AND site_section_id NOT IN('16100680','16193797','16192906','16194461','16194463','16194481','16197756','16197766','16214682') -- removing fsn
AND placement_id NOT IN ('28357916','28357921','44015286','44015291','44025664','44025669','44026928','51228140','51255705','-1', '51666331','51502145') -- removing house ads and others
AND placement_id IS NOT NULL
GROUP BY 1
ORDER BY 1

-- Reseller
SELECT DISTINCT DATE(PARSE_TIMESTAMP('%Y%m%d%H%M%S',event_time), 'America/New_York') as event_date, COUNT (*) AS Impressions
FROM `fubotv-prod.dmp.de_sync_freewheel_logs` 
WHERE DATE(PARSE_TIMESTAMP('%Y%m%d%H%M%S',event_time), 'America/New_York') >= '2021-02-01'
AND DATE(PARSE_TIMESTAMP('%Y%m%d%H%M%S',event_time), 'America/New_York') <= '2021-03-08'
AND event_name = 'defaultImpression' -- sum of default impressions 
AND sales_channel_type = 'Reseller Sold' --Reseller
AND selling_partner_id IS NOT NULL 
AND selling_partner_id NOT IN ('516827','512091','519508','393638','515272','512022','511438','511999') -- resellers that are excluded
AND site_section_id NOT IN('16100680','16193797','16192906','16194461','16194463','16194481','16197756','16197766','16214682') -- removing fsn
GROUP BY 1
ORDER BY 1
-- TImestamp conversion
DATE(PARSE_TIMESTAMP('%Y%m%d%H%M%S',event_time), 'America/New_York')

-- Direct w/ Placement in CAR
WITH log_info AS
(
    SELECT DISTINCT placement_id as placement_id, COUNT (*) as impressions
    FROM `fubotv-prod.dmp.de_sync_freewheel_logs` t1
    WHERE DATE(PARSE_TIMESTAMP('%Y%m%d%H%M%S',event_time), 'America/New_York') = '2021-02-18'
    AND event_name = 'defaultImpression' -- sum of default impressions 
    AND sales_channel_type = 'Direct Sold' --Direct partner
    AND selling_partner_id IS NULL 
    AND site_section_id NOT IN('16100680','16193797','16192906','16194461','16194463','16194481','16197756','16197766','16214682') -- removing fsn
    AND placement_id NOT IN ('28357916','28357921','44015286','44015291','44025664','44025669','44026928','51228140','51255705','-1') -- removing house ads and others
    AND placement_id IS NOT NULL
    GROUP BY 1
    ORDER By 2 DESC
),
placement_info AS
(
    SELECT DISTINCT t2.placement_id_mrm as id, t2.placement_name as placement_name, TIMESTAMP(PARSE_DATE('%m/%d/%Y',start_date)) as start_date
    FROM `fubotv-prod.dmp.de_sync_freewheel_reports_custom_audit_report_v2` as t2 

)
SELECT DISTINCT t1.placement_id,placement_name, impressions 
FROM log_info t1
JOIN placement_info AS t2 ON t1.placement_id  = t2.id
WHERE LOWER(placement_name) NOT LIKE  '%january'
ORDER BY 2 ASC


--OR w/ Companion


WITH log_info AS
(
    SELECT DISTINCT DATE(PARSE_TIMESTAMP('%Y%m%d%H%M%S',event_time), 'America/New_York') as event_date, placement_id as placement_id, 
    COUNT (*) as impressions
    FROM `fubotv-prod.dmp.de_sync_freewheel_logs` t1
    WHERE DATE(PARSE_TIMESTAMP('%Y%m%d%H%M%S',event_time), 'America/New_York') >= '2021-02-18'
    AND DATE(PARSE_TIMESTAMP('%Y%m%d%H%M%S',event_time), 'America/New_York') <= '2021-02-28'
    AND event_name = 'defaultImpression' -- sum of default impressions 
    AND sales_channel_type = 'Direct Sold' --Direct partner
    AND selling_partner_id IS NULL 
    AND site_section_id NOT IN('16100680','16193797','16192906','16194461','16194463','16194481','16197756','16197766','16214682') -- removing fsn
    AND placement_id NOT IN ('28357916','28357921','44015286','44015291','44025664','44025669','44026928','51228140','51255705','-1', '51666331','51502145') -- removing house ads and others
    AND placement_id IS NOT NULL
    GROUP BY 1,2
    ORDER By 3 DESC
),
placement_info AS
(
    SELECT DISTINCT t2.id as id, t2.name as placement_name, 
    FROM `fubotv-prod.dmp.de_sync_freewheel_companion_placement_v4` as t2 

)
SELECT DISTINCT t1.event_date , t1.placement_id,placement_name, impressions 
FROM log_info t1
JOIN placement_info AS t2 ON t1.placement_id  = t2.id
ORDER BY 2 ASC


-- Reseller w/name
WITH log_info AS
(
    SELECT DISTINCT DATE(PARSE_TIMESTAMP('%Y%m%d%H%M%S',event_time), 'America/New_York') as event_date, selling_partner_id as reseller_id, COUNT (*) as impressions
    FROM `fubotv-prod.dmp.de_sync_freewheel_logs` t1
    WHERE DATE(PARSE_TIMESTAMP('%Y%m%d%H%M%S',event_time), 'America/New_York') >= '2021-03-08'
    AND event_name = 'defaultImpression' -- sum of default impressions 
    AND sales_channel_type = 'Reseller Sold' --Reseller
    AND selling_partner_id IS NOT NULL 
    AND selling_partner_id NOT IN ('516827','512091','519508','393638','515272','512022','511438','511999') -- resellers that are excluded
    AND site_section_id NOT IN('16100680','16193797','16192906','16194461','16194463','16194481','16197756','16197766','16214682') -- removing fsn
    GROUP BY 1,2
    ORDER By 3 DESC
),
reseller_info AS
(
    SELECT DISTINCT t2.id as reseller_id, t2.name as reseller_name
    FROM `fubotv-prod.dmp.de_sync_freewheel_companion_network_v4` as t2
)
SELECT t1.event_date, t1.reseller_id,reseller_name, impressions 
FROM log_info t1
JOIN reseller_info AS t2 ON t1.reseller_id = t2.reseller_id 
ORDER BY 2 ASC


-- VOD test

WITH log_info AS
(
    SELECT DISTINCT event_name,placement_id as placement_id, video_asset_id as asset_id,-- REGEXP_EXTRACT(all_request_kv, r"_fw_vcid2=(?i)fubotv:?%?3?A?([[:alnum:]]{24})") AS user_id,
    platform_device_id as device_id, COUNT (*) as impressions,
    FROM `fubotv-prod.dmp.de_sync_freewheel_logs` t1
    WHERE DATE(PARSE_TIMESTAMP('%Y%m%d%H%M%S',event_time), 'America/New_York') = '2021-02-18'
   -- AND event_name = 'defaultImpression' -- sum of default impressions 
    --AND placement_id IS NOT NULL
    AND all_request_kv LIKE ('%playback_type=vod%')
    GROUP BY 1,2,3,4
    ORDER By 5 DESC
),
placement_info AS
(
    SELECT DISTINCT t2.id as id, t2.name as placement_name, 
    FROM `fubotv-prod.dmp.de_sync_freewheel_companion_placement_v4` as t2 
),
asset_info AS
(
    SELECT DISTINCT t2.id as id, t2.name as asset_name, 
    FROM `fubotv-prod.dmp.de_sync_freewheel_companion_video_asset_v4` as t2 
),
device_info AS
(
    SELECT DISTINCT t2.id as id, t2.name as device_name, 
    FROM `fubotv-prod.dmp.de_sync_freewheel_companion_platform_browser_os_device_v4` as t2 
)
SELECT DISTINCT event_name,placement_name, asset_name,device_name, impressions
FROM log_info t1
LEFT JOIN placement_info AS t2 ON t1.placement_id  = t2.id
LEFT JOIN asset_info AS t3 ON t1.asset_id = t3.id 
LEFT JOIN device_info AS t4 ON t1.device_id = t4.id
ORDER BY 4 DESC

-- ad beaconing 
SELECT DISTINCT
REGEXP_EXTRACT(url, r"playback_type=([a-zA-Z_]*)") AS playback_type,
REGEXP_EXTRACT(url, r"eventType=([a-zA-Z_]*)") AS event_type,
FROM `fubotv-prod.AdBeaconing.prod_logs` 
WHERE DATE(_PARTITIONTIME) = "2021-03-08"
AND url LIKE ('%playback_type=vod%')

--Tableau test
WITH log_info AS
(
    SELECT DISTINCT 
    DATE(PARSE_TIMESTAMP('%Y%m%d%H%M%S',event_time), 'America/New_York') as event_date,
    site_section_id,
    transaction_id, 
    event_type,
    event_name,
    placement_id as placement_id, 
    video_asset_id as asset_id,
    REGEXP_EXTRACT(all_request_kv, r"playback_type=([a-zA-Z_]*)") AS playback_type, 
    platform_device_id,
    sales_channel_type,
    count (*) counts
    FROM `fubotv-prod.dmp.de_sync_freewheel_logs` t1
    GROUP BY 1,2,3,4,5,6,7,8,9,10
),
site_section_info AS
(
    SELECT id as site_id, name as site_name
    from `fubotv-prod.dmp.de_sync_freewheel_companion_site_section_v4`
),
device_info AS
(
    SELECT id as device_id, name as device_name
    from `fubotv-prod.dmp.de_sync_freewheel_companion_platform_browser_os_device_v4`
)

SELECT t1.*, t2.site_name, t3.device_name
FROM log_info t1
LEFT JOIN site_section_info t2 ON t1.site_section_id = t2.site_id 
LEFT JOIN device_info t3 ON t1.platform_device_id = t3.device_id 
WHERE transaction_id = '1615179601201335581-a120'
ORDER BY transaction_id

--IN test Tableau
SELECT DISTINCT 
DATE(PARSE_TIMESTAMP('%Y%m%d%H%M%S',event_time), 'America/New_York') as event_date,
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


-- viewership query for station and device to join
Select
date(t1.start_time) as Date_range, user_id, t5.station_name as Channel_Name, playback_type, device_category , t1.series_title,t1.program_title, t1.episode_title, SUM(duration)/3600 as Hours, COUNT(DISTINCT(t1.user_id)) AS Uniques
from `fubotv-prod.data_insights.video_in_progress_via_va_view` t1
inner join `fubotv-prod.data_insights.tmsid_genre_mapping` t3 on t1.tms_id=t3.tms_id
inner join `fubotv-dev.business_analytics.AdInsertable_Networks` t4 on t1.channel=t4.channel
inner join `fubotv-dev.business_analytics.station_mapping_table_51220` t5 ON t5.station_name = t1.channel
WHERE t3.tms_id IS NOT NULL
AND UPPER(t3.tms_id) NOT IN ("","00000000000000","TMS-ID-UNAVAILABLE")
group by 1,2,3,4,5,6,7,8
order by 1 DESC   



--BQ test vod #s for prpgram and user
Select user_id, channel, series_title, program_title, episode_title, playback_type, sum(duration)/3600 as hours
from `fubotv-prod.data_insights.video_in_progress_via_va_view` 
WHERE date(start_time) = '2021-03-08'
and playback_type = 'vod'
group by 1,2,3,4,5,6


-- prod analytics

WITH ad_beacons AS (
SELECT  CAST(timestamp AS timestamp) as timestamp, DATE(_PARTITIONTIME) as pdate
, REGEXP_EXTRACT(url, r"caid=([a-zA-Z0-9]*_[a-zA-Z0-9]*)") AS asset_id
, REGEXP_EXTRACT(url, r"caid=([a-zA-Z0-9]*)_[a-zA-Z0-9]*") AS tms_id
, REGEXP_EXTRACT(url, r"caid=[a-zA-Z0-9]*_([a-zA-Z0-9]*)") AS content_provider
, REGEXP_EXTRACT(url, r"vprn=([0-9]*)") AS session_id
, REGEXP_EXTRACT(url, r"_fw_vcid2=(?i)fubotv:?%?3?A?([a-zA-Z0-9]{24})") AS user_id
, REGEXP_EXTRACT(url, r"device=([a-zA-Z_]*)") AS device
, REGEXP_EXTRACT(url, r"adId=([0-9]*)") AS ad_id
, REGEXP_EXTRACT(url, r"creativeId=([0-9]*)") AS creative_id
, REGEXP_EXTRACT(url, r"ad_idx=([0-9]*)") AS ad_index
, CAST(REGEXP_EXTRACT(url, r"creativeDuration=([0-9]*)") AS INT64) AS creative_duration
, REGEXP_EXTRACT(url, r"playback_type=([a-zA-Z_]*)") AS playback_type
, REGEXP_EXTRACT(url, r"eventType=([a-zA-Z_]*)") AS event_type
, REGEXP_EXTRACT(url, r"nw=([0-9]*)") AS network_id -- check FW companion file
, REGEXP_EXTRACT(url, r"fw_pl=([^&]*)") AS placement_name
, time_elapsed, client_ip, geo_city, geo_country_code, request
, url
, resp_status
-- , * except (url, time_elapsed, timestamp, client_ip, geo_city, geo_country_code, request)
FROM `fubotv-prod.AdBeaconing.prod_logs` 
WHERE DATE(_PARTITIONTIME) > '2021-01-01'
AND url LIKE '/fubo_imp%'
)

, kg_content AS (
SELECT tms_id
, ROUND(AVG(COALESCE(CAST(duration AS INT64),TIMESTAMP_DIFF(TIMESTAMP(SUBSTR(asset_id,LENGTH(asset_id)-19,20)),TIMESTAMP(SUBSTR(asset_id,LENGTH(asset_id)-40,20)),MINUTE))),1) AS asset_duration_min
FROM `fubotv-prod.dmp.content_asset_details`
WHERE channel_id IS NOT NULL
GROUP BY 1
)

, join_kg AS (
SELECT c.asset_duration_min, a.*
, d.device AS device_2
FROM ad_beacons a
LEFT JOIN kg_content c
ON a.tms_id = c.tms_id
LEFT JOIN `fubotv-dev.product_analytics.device_mapping` d
ON a.device = d.device_raw
)

, viewership AS (
SELECT v.* EXCEPT (device_category, playback_type), d.device, p.playback_type
FROM `fubotv-prod.dmp.viewership_activities` v
LEFT JOIN `fubotv-prod.data_insights_ETL.device_category_name_mapping_static` d
ON v.device_category = d.device_category_raw
LEFT JOIN `fubotv-prod.data_insights_ETL.playback_type_name_mapping_static` p
ON v.playback_type = p.playback_type_raw
)

, join_viewership AS (
SELECT a.asset_duration_min, ROUND(v.duration/60,1) AS asset_view_duration_min, a.* EXCEPT(asset_duration_min) 
FROM join_kg a
LEFT JOIN viewership v
ON a.user_id = v.user_id
AND a.tms_id = v.tms_id
AND lower(a.device_2) = lower(v.device)
AND lower(a.playback_type) = lower(v.playback_type)
AND (REGEXP_CONTAINS(lower(a.content_provider),lower(v.channel)) OR REGEXP_CONTAINS(lower(v.channel),lower(a.content_provider)))
AND a.timestamp >= TIMESTAMP_SUB(v.start_time, INTERVAL 5 MINUTE)
AND a.timestamp <= TIMESTAMP_SUB(TIMESTAMP_ADD(v.start_time, INTERVAL v.duration SECOND), INTERVAL 5 MINUTE)
)

SELECT *
FROM join_viewership



-- ad num calls and time fills rate test

SELECT DISTINCT
DATE(PARSE_TIMESTAMP('%Y%m%d%H%M%S',event_time), 'UTC') as event_date,
event_name,
COUNT(*) as num_ad_calls,
SUM(CAST(max_duration AS FLOAT64 )) AS total_avail_time_sec,
SUM(
    CASE WHEN CAST(ad_duration AS FLOAT64) > CAST(max_duration AS FLOAT64) 
    THEN CAST(max_duration AS FLOAT64)
    ELSE CAST(ad_duration AS FLOAT64) END
    ) AS total_fill_time_sec,
SUM(CASE WHEN CAST(ad_duration AS FLOAT64 ) > CAST(max_duration AS FLOAT64) 
THEN CAST(max_duration AS FLOAT64) ELSE CAST(ad_duration AS FLOAT64 ) END) / SUM(CAST (max_duration AS FLOAT64 )) AS 
    time_fill_rate_pct,
FROM `fubotv-prod.dmp.de_sync_freewheel_logs`
WHERE DATE(PARSE_TIMESTAMP('%Y%m%d%H%M%S',event_time), 'UTC') = '2021-03-22'
AND max_duration IS NOT NULL 
GROUP BY 1,2
ORDER BY 1


-- Ad Sales Station and Channel Mapping BQ
SELECT DISTINCT Date(Event_date) as Event_Date, t2.Station_Name_Mapped, t2.Network_Group_Mapped,  Net_Counted_Ads
FROM `fubotv-dev.business_analytics.FW_MRM_Analytics_Station_Data` t1
INNER JOIN `fubotv-dev.business_analytics.FW_MRM_Analytics_Station_Mapping_Data` t2 on t1.Video_Group_Name = t2.Video_Group_Name
ORDER BY 1 DESC 

-- Ad proxy time fill rate
SELECT 
  COUNT(*) AS num_ad_calls,
  SUM(jsonPayload.maxd) AS total_avail_time_sec,
  SUM(
    CASE WHEN jsonPayload.total_ad_duration > CAST(jsonPayload.maxd AS   
    FLOAT64) THEN CAST(jsonPayload.maxd AS FLOAT64) ELSE 
    jsonPayload.total_ad_duration END) AS total_fill_time_sec,
  SUM(CASE WHEN jsonPayload.total_ad_duration > CAST(jsonPayload.maxd  
    AS FLOAT64) THEN CAST(jsonPayload.maxd AS FLOAT64) ELSE 
    jsonPayload.total_ad_duration END) / SUM(jsonPayload.maxd) AS 
    time_fill_rate_pct,
FROM `fubotv-prod.ad_proxy.ad_proxy_20210113`
WHERE 1=1
  AND jsonPayload.message = 'VMAP'
  AND jsonPayload._fw_station IS NOT NULL
  AND jsonPayload.maxd IS NOT NULL