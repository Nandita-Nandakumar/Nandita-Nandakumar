WITH proxy_data AS
(
SELECT
    SPLIT(jsonpayload.caid,"_")[OFFSET(0)] as tms_id,
 -- jsonPayload._fw_user_agent as device,
    jsonPayload.total_ad_duration, 
    jsonPayload.maxd,
  --REGEXP_EXTRACT(jsonpayload.outbound__url, r"device[model]=([a-zA-Z_]*)") AS device_model,
  COUNT(*) AS num_ad_calls,
  SUM(jsonPayload.maxd) AS total_avail_time_sec,
FROM `fubotv-prod.ad_proxy.ad_proxy_202106*`
WHERE 1=1
  AND jsonPayload.message = 'VMAP'
  AND jsonPayload._fw_station IS NOT NULL
  AND jsonPayload.maxd IS NOT NULL
  AND DATE(timestamp) >= '2021-06-03' AND DATE(timestamp) <= '2021-06-09'
  AND SPLIT(jsonpayload.caid,"_")[OFFSET(0)] IN ('EP033149890044','FUBOCOM0000001','FUBO20210528202830',
  'FUBO20210528204447','FUBO20210601193949','FUBO20210603185306','FUBO20210601192345','FUBO20210601192444',
  'FUBO20210601192536','FUBO20210601192633','EP038605420001','EP038758570001','EP038605440001','EP038758620001',
  'EP038758570002','EP038758620002','FUBO20210603185156','EP033149890048','FUBO20210602141534','FUBO20210601190616',
  'FUBO20210601190749','FUBO20210601192718','FUBO20210601192803','FUBO20210601192836','FUBO20210601192918',
  'FUBO20210601192951','EP038605420002','EP038605440002','EP038758570003','EP038758620003')
GROUP BY 1,2,3
)

, beaconing_data AS
(
  SELECT
  SPLIT(jsonpayload.http_request_query_caid, "_")[OFFSET(0)] as tms_id,
        #jsonPayload.http_request_query_csid as csid,
        #jsonPayload.http_request_query_prof as prof,
        SUM(CASE WHEN (jsonPayload.http_request_query_fw_pl LIKE "%House Ad%" OR jsonPayload.http_request_query_fw_pl = "All_LIVE_Endpoints") THEN 1 ELSE 0 END) as house_ads_imps,
        SUM(CASE WHEN (jsonPayload.http_request_query_fw_pl NOT LIKE "%House Ad%" AND jsonPayload.http_request_query_fw_pl != "All_LIVE_Endpoints") THEN 1 ELSE 0 END) as normal_imps,
        SUM(jsonPayload.http_request_query_creativeduration) as total_ad_duration,
        SUM(CASE WHEN (jsonPayload.http_request_query_fw_pl NOT LIKE "%House Ad%" AND jsonPayload.http_request_query_fw_pl != "All_LIVE_Endpoints") THEN jsonPayload.http_request_query_creativeduration ELSE 0 END) as total_ad_duration_without_house_ads,
        COUNT(*) as total_imps_count
    FROM `fubotv-prod.AdBeaconing_Impressions_2.ad_beaconing_*`
    WHERE _TABLE_SUFFIX BETWEEN "20210603" AND "20210609"
    AND jsonPayload.http_request_query_eventType = "defaultImpression"
    AND jsonPayload.http_request_query_playback_type = "live"
    AND SPLIT(jsonpayload.http_request_query_caid, "_")[OFFSET(0)] IN ('EP033149890044','FUBOCOM0000001','FUBO20210528202830',
  'FUBO20210528204447','FUBO20210601193949','FUBO20210603185306','FUBO20210601192345','FUBO20210601192444',
  'FUBO20210601192536','FUBO20210601192633','EP038605420001','EP038758570001','EP038605440001','EP038758620001',
  'EP038758570002','EP038758620002','FUBO20210603185156','EP033149890048','FUBO20210602141534','FUBO20210601190616',
  'FUBO20210601190749','FUBO20210601192718','FUBO20210601192803','FUBO20210601192836','FUBO20210601192918',
  'FUBO20210601192951','EP038605420002','EP038605440002','EP038758570003','EP038758620003')
  AND jsonPayload.http_request_query_fw_pl NOT LIKE ('%House Ad%')
  AND jsonPayload.http_request_query_fw_pl NOT LIKE ('%All_LIVE_Endpoints%')
    GROUP BY 1
    ORDER BY 1 ASC
)

SELECT t1.tms_id, t1.total_ad_duration, total_ad_duration_without_house_ads, maxd, num_ad_calls 
FROM proxy_data t1
INNER JOIN beaconing_data t2 USING (tms_id)



SELECT timestamp, jsonPayload.total_ad_duration, jsonPayload.creative_duration,jsonPayload.maxd,jsonPayload.caid, jsonPayload._fw_station, jsonPayload.ad_titles,
FROM `fubotv-prod.ad_proxy.ad_proxy_20210603`
WHERE jsonPayload._fw_vcid2 LIKE '%5f3a9e40bdc7cb0001a523ce'
AND jsonPayload.message = 'VMAP'
AND jsonPayload._fw_station IS NOT NULL
AND jsonPayload.maxd IS NOT NULL
ORDER BY 1 ASC

SELECT timestamp, jsonPayload.maxd, jsonPayload.total_ad_duration, count(*) as num_ad_calls
FROM `fubotv-prod.ad_proxy.ad_proxy_20210603`
WHERE jsonPayload._fw_vcid2 LIKE '%5f3a9e40bdc7cb0001a523ce'
AND jsonPayload.message = 'VMAP'
AND jsonPayload._fw_station IS NOT NULL
AND jsonPayload.maxd IS NOT NULL
GROUP BY 1,2,3
ORDER BY 1 ASC


SELECT  jsonPayload.creative_duration,jsonPayload.ad_titles
FROM `fubotv-prod.ad_proxy.ad_proxy_20210603`
WHERE jsonPayload._fw_vcid2 LIKE '%5f3a9e40bdc7cb0001a523ce'
AND "fubo Sports Network Live House Ad campaign version 3" IN UNNEST(jsonPayload.ad_titles)
#AND jsonPayload.ad_titles LIKE ('%House%')
AND jsonPayload.message = 'VMAP'
AND jsonPayload._fw_station IS NOT NULL
AND jsonPayload.maxd IS NOT NULL


SELECT 
count(*) as count,
REGEXP_EXTRACT(url, r"_fw_vcid2=(?i)fubotv:?%?3?A?([[:alnum:]]{24})") AS user_id,
REGEXP_EXTRACT(url, r"creativeId=([[:alnum:]_]*)") AS creative_id,
REGEXP_EXTRACT(url, r"creativeDuration=([0-9]*)") AS creative_duration,
REGEXP_EXTRACT(url, r"fw_pl=([^&]*)") AS placement_name,
timestamp as timestamp
FROM `fubotv-prod.AdBeaconing.prod_logs` 
WHERE _PARTITIONTIME = TIMESTAMP("2021-06-03")
AND resp_status = '200'
AND url LIKE '/fubo_imp%eventType=defaultImpression%'
AND url LIKE '%5f3a9e40bdc7cb0001a523ce%'
GROUP BY 2,3,4,5,6
ORDER BY timestamp desc


#current test 

WITH ad_proxy_data AS
(
    SELECT DISTINCT
    tms_id,
    CASE WHEN d1 = 'mobile_app_iphone_live' AND d2 = 'iOS' THEN 'iphone'
    WHEN d2 = 'iOS' AND d1 = 'mobile_app_ipad_live' THEN 'ipad'
    WHEN d2 = 'appletv' THEN 'appletv'
    WHEN d2 = 'desktop' THEN 'web'
    WHEN d2 = 'android' AND d1 = 'mobile_app_android_phone_live' THEN 'android_phone'
    WHEN d2 = 'android' AND d1= 'mobile_app_android_tablet_live' THEN 'android_tablet'
    WHEN d2 = 'chromecast' THEN 'chromecast'
    WHEN d2 = 'mobileweb' THEN 'mobileweb'
    WHEN d2 = 'roku' THEN 'roku'
    WHEN d2 = 'androidtv' THEN 'androidtv'
    WHEN d2 ='firetv' THEN 'firetv'
    WHEN d2 = 'smarttv' AND d1 = 'connected_tv_xbox_live' THEN 'xbox'
    WHEN d2 ='smarttv' ANDWITH ad_proxy_data AS
(
    SELECT DISTINCT
    tms_id,
    CASE WHEN d1 = 'mobile_app_iphone_live' AND d2 = 'iOS' THEN 'iphone'
    WHEN d2 = 'iOS' AND d1 = 'mobile_app_ipad_live' THEN 'ipad'
    WHEN d2 = 'appletv' THEN 'appletv'
    WHEN d2 = 'desktop' THEN 'web'
    WHEN d2 = 'android' AND d1 = 'mobile_app_android_phone_live' THEN 'android_phone'
    WHEN d2 = 'android' AND d1= 'mobile_app_android_tablet_live' THEN 'android_tablet'
    WHEN d2 = 'chromecast' THEN 'chromecast'
    WHEN d2 = 'mobileweb' THEN 'mobileweb'
    WHEN d2 = 'roku' THEN 'roku'
    WHEN d2 = 'androidtv' THEN 'androidtv'
    WHEN d2 ='firetv' THEN 'firetv'
    WHEN d2 = 'smarttv' AND d1 = 'connected_tv_xbox_live' THEN 'xbox'
    WHEN d2 ='smarttv' AND d1 != 'connected_tv_xbox_live' THEN 'smart_tv'
    END AS device_cat,
    SUM (ad_duration) as ad_duration,
    SUM (max_duration) as max_duration, 
    SUM (num_ad_calls) as ad_requests
    FROM 
    (
    SELECT
    #timestamp,
    SPLIT(jsonpayload.outbound__caid,"_")[OFFSET(0)] as tms_id,
    REGEXP_EXTRACT(jsonPayload.csid, r"fubo_([a-zA-Z_]*)") as d1,
    SPLIT(jsonPayload.prof, "_")[OFFSET(1)] as d2,
    jsonPayload.total_ad_duration as ad_duration, 
    jsonPayload.maxd as max_duration,
    COUNT(*) AS num_ad_calls,
    SUM(jsonPayload.maxd) AS total_avail_time_sec,
    FROM `fubotv-prod.ad_proxy.ad_proxy_202106*`
    WHERE 1=1
    AND jsonPayload.message = 'VMAP'
    AND jsonPayload._fw_station IS NOT NULL
    AND jsonPayload.maxd IS NOT NULL
    AND jsonPayload.csid LIKE "%live%"
    AND DATE(timestamp) >= '2021-06-03' AND DATE(timestamp) <= '2021-06-09'
    AND SPLIT(jsonpayload.outbound__caid,"_")[OFFSET(0)] IN ('EP033149890044','FUBOCOM0000001','FUBO20210528202830',
    'FUBO20210528204447','FUBO20210601193949','FUBO20210603185306','FUBO20210601192345','FUBO20210601192444',
    'FUBO20210601192536','FUBO20210601192633','EP038605420001','EP038758570001','EP038605440001','EP038758620001',
    'EP038758570002','EP038758620002','FUBO20210603185156','EP033149890048','FUBO20210602141534','FUBO20210601190616',
    'FUBO20210601190749','FUBO20210601192718','FUBO20210601192803','FUBO20210601192836','FUBO20210601192918',
    'FUBO20210601192951','EP038605420002','EP038605440002','EP038758570003','EP038758620003')
    #AND replace(jsonPayload._fw_vcid2,"fuboTV:","") LIKE '%5f3a9e40bdc7cb0001a523ce%'
    GROUP BY 1,2,3,4,5,6
    ORDER BY 1 
    )
    GROUP BY 1,2
    ORDER BY 5 DESC
)
# Isolating House Ads Served
, ad_beaconing_data AS 
(
    SELECT 
    DISTINCT
    /*
    CASE WHEN device IS NULL THEN NULL
    WHEN device = '' THEN NULL
    WHEN device = 'on' THEN NULL
    WHEN device = 'dvr' THEN NULL
    WHEN device = 'tv' THEN 'androidtv' 
    WHEN device = 'vizio' THEN 'smart_tv'
    WHEN device = 'xbox' THEN 'xbox'
    WHEN device = 'androidtv' THEN 'androidtv'
    WHEN device = 'appletv' THEN 'appletv'
    WHEN device = 'mobile' THEN 'mobileweb'
    WHEN device = 'iphone' THEN 'iphone'
    WHEN device = 'fire' THEN 'firetv'
    WHEN device = 'tablet' THEN 'android_tablet'
    WHEN device = 'lgtv' THEN 'smart_tv'
    WHEN device = 'android_phone' THEN 'android_phone'
    WHEN device = 'apple_tv' THEN 'appletv'
    WHEN device = 'samsung_tv' THEN 'smart_tv'
    WHEN device = 'phone' THEN 'android_phone'
    WHEN device = 'lg_tv' THEN 'smart_tv'
    WHEN device = 'web' THEN 'web'
    WHEN device = 'ipad' THEN 'ipad'
    WHEN device = 'ios_tablet' THEN 'ipad'
    WHEN device = 'hisense' THEN 'smart_tv'
    WHEN device = 'ios_phone' THEN 'iphone'
    WHEN device = 'android_tv' THEN 'androidtv'
    WHEN device = 'android_tablet' THEN 'android_tablet'
    WHEN device = 'samsungtv' THEN 'smart_tv'
    WHEN device = 'chromecast' THEN 'chromecast'
    WHEN device = 'desktop' THEN 'web'
    WHEN device = 'fire_tv' THEN 'firetv'
    WHEN device = 'roku' THEN 'roku' 
    END AS device_cat, */
    SUM(creative_duration) AS House_ads_duration
    FROM 
        (
            SELECT 
            REGEXP_EXTRACT(url, r"caid=([a-zA-Z0-9]*)_[a-zA-Z0-9]*") AS tms_id,
            CAST(REGEXP_EXTRACT(url, r"creativeDuration=([0-9]*)") AS INT64) AS creative_duration,
            REGEXP_EXTRACT(url, r"fw_pl=([^&]*)") AS placement_name,
            COALESCE(REGEXP_EXTRACT(url, r"device=([a-zA-Z_]*)"), REGEXP_EXTRACT(url, r"csid=[a-zA-Z_]*_([a-zA-Z]*)_[a-zA-Z]*")) AS device,
            FROM `fubotv-prod.AdBeaconing.prod_logs` 
            WHERE _PARTITIONTIME BETWEEN TIMESTAMP("2021-06-03") AND TIMESTAMP("2021-06-09")
            AND resp_status = '200'
            AND url LIKE '/fubo_imp%eventType=defaultImpression%'
            #AND url like '%5f3a9e40bdc7cb0001a523ce%'
            AND url LIKE ('%live%')
            ORDER BY 2 desc
        )
    WHERE (placement_name  LIKE ('%LIVE_Endpoints%')  OR placement_name  LIKE ('%House+Ad%'))
    AND tms_id IN ('EP033149890044','FUBOCOM0000001','FUBO20210528202830',
    'FUBO20210528204447','FUBO20210601193949','FUBO20210603185306','FUBO20210601192345','FUBO20210601192444',
    'FUBO20210601192536','FUBO20210601192633','EP038605420001','EP038758570001','EP038605440001','EP038758620001',
    'EP038758570002','EP038758620002','FUBO20210603185156','EP033149890048','FUBO20210602141534','FUBO20210601190616',
    'FUBO20210601190749','FUBO20210601192718','FUBO20210601192803','FUBO20210601192836','FUBO20210601192918',
    'FUBO20210601192951','EP038605420002','EP038605440002','EP038758570003','EP038758620003')
    --GROUP BY 1
    ORDER BY 1
)

SELECT  
t1.device_cat,
SUM(ad_duration) as ad_duration,
SUM(max_duration) as max_duration,
SUM(House_ads_duration) as House_ads_duration, 
SUM(ad_requests) as ad_requests
FROM ad_proxy_data t1 
JOIN ad_beaconing_data t2 USING (tms_id, device_cat)
GROUP BY 1


#06/24

SELECT
  REGEXP_EXTRACT(jsonPayload.csid, r"fubo_([a-zA-Z_]*)") as device1,
  SPLIT(jsonPayload.prof, "_")[OFFSET(1)] as device2,
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
    FROM `fubotv-prod.ad_proxy.ad_proxy_202106*`
    WHERE 1=1
    AND jsonPayload.message = 'VMAP'
    AND jsonPayload.maxd IS NOT NULL
    AND DATE(timestamp) >= '2021-06-03' AND DATE(timestamp) <= '2021-06-09'
    AND SPLIT(jsonpayload.outbound__caid,"_")[OFFSET(0)] IN  ('EP033149890044','FUBOCOM0000001','FUBO20210528202830',
    'FUBO20210528204447','FUBO20210601193949','FUBO20210603185306','FUBO20210601192345','FUBO20210601192444',
    'FUBO20210601192536','FUBO20210601192633','EP038605420001','EP038758570001','EP038605440001','EP038758620001',
    'EP038758570002','EP038758620002','FUBO20210603185156','EP033149890048','FUBO20210602141534','FUBO20210601190616',
    'FUBO20210601190749','FUBO20210601192718','FUBO20210601192803','FUBO20210601192836','FUBO20210601192918',
    'FUBO20210601192951','EP038605420002','EP038605440002','EP038758570003','EP038758620003') 
    GROUP BY 1,2
    ORDER BY 1 
)
# Isolating House Ads Served
, ad_beaconing_data AS 
(
    SELECT 
    DISTINCT
    tms_id,
    device,
    SUM(creative_duration) AS House_ads_duration
    FROM 
        (
            SELECT 
            REGEXP_EXTRACT(url, r"caid=([a-zA-Z0-9]*)_[a-zA-Z0-9]*") AS tms_id,
            CAST(REGEXP_EXTRACT(url, r"creativeDuration=([0-9]*)") AS INT64) AS creative_duration,
            REGEXP_EXTRACT(url, r"fw_pl=([^&]*)") AS placement_name,
            COALESCE(REGEXP_EXTRACT(url, r"device=([a-zA-Z_]*)"), REGEXP_EXTRACT(url, r"csid=[a-zA-Z_]*_([a-zA-Z]*)_[a-zA-Z]*")) AS device,
            FROM `fubotv-prod.AdBeaconing.prod_logs` 
            WHERE TIMESTAMP >= ("2021-06-02") AND TIMESTAMP <= ("2021-06-09")
            AND resp_status = '200'
            ORDER BY 2 desc
        )
    WHERE (placement_name  LIKE ('%LIVE_Endpoints%')  OR placement_name  LIKE ('%House+Ad%'))
    AND tms_id IN ('EP033149890044','FUBOCOM0000001','FUBO20210528202830',
    'FUBO20210528204447','FUBO20210601193949','FUBO20210603185306','FUBO20210601192345','FUBO20210601192444',
    'FUBO20210601192536','FUBO20210601192633','EP038605420001','EP038758570001','EP038605440001','EP038758620001',
    'EP038758570002','EP038758620002','FUBO20210603185156','EP033149890048','FUBO20210602141534','FUBO20210601190616',
    'FUBO20210601190749','FUBO20210601192718','FUBO20210601192803','FUBO20210601192836','FUBO20210601192918',
    'FUBO20210601192951','EP038605420002','EP038605440002','EP038758570003','EP038758620003') 
    GROUP BY 1,2
    ORDER BY 1
)

# TFR - On Conmebol Days for Fubo

SELECT
  REGEXP_EXTRACT(jsonPayload.csid, r"fubo_([a-zA-Z_]*)") as device1,
  SPLIT(jsonPayload.prof, "_")[OFFSET(1)] as device2,
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
    FROM `fubotv-prod.ad_proxy.ad_proxy_202106*`
    WHERE 1=1
    AND jsonPayload.message = 'VMAP'
    AND jsonPayload.maxd IS NOT NULL
    AND DATE(timestamp) >= '2021-06-03' AND DATE(timestamp) <= '2021-06-09'
    AND tms_id IN ('EP033149890044','FUBOCOM0000001','FUBO20210528202830',
    'FUBO20210528204447','FUBO20210601193949','FUBO20210603185306','FUBO20210601192345','FUBO20210601192444',
    'FUBO20210601192536','FUBO20210601192633','EP038605420001','EP038758570001','EP038605440001','EP038758620001',
    'EP038758570002','EP038758620002','FUBO20210603185156','EP033149890048','FUBO20210602141534','FUBO20210601190616',
    'FUBO20210601190749','FUBO20210601192718','FUBO20210601192803','FUBO20210601192836','FUBO20210601192918',
    'FUBO20210601192951','EP038605420002','EP038605440002','EP038758570003','EP038758620003') 
    GROUP BY 1,2
    ORDER BY 1 

# House Ads Available

SELECT 
    DISTINCT
    device, 
    SUM(creative_duration) AS House_ads_duration
    FROM 
        (
            SELECT 
            REGEXP_EXTRACT(url, r"caid=([a-zA-Z0-9]*)_[a-zA-Z0-9]*") AS tms_id,
            CAST(REGEXP_EXTRACT(url, r"creativeDuration=([0-9]*)") AS INT64) AS creative_duration,
            REGEXP_EXTRACT(url, r"fw_pl=([^&]*)") AS placement_name,
            COALESCE(REGEXP_EXTRACT(url, r"device=([a-zA-Z_]*)"), REGEXP_EXTRACT(url, r"csid=[a-zA-Z_]*_([a-zA-Z]*)_[a-zA-Z]*")) AS device,
            FROM `fubotv-prod.AdBeaconing.prod_logs` 
            WHERE TIMESTAMP >= ("2021-06-02") AND TIMESTAMP <= ("2021-06-09")
            AND resp_status = '200'
            ORDER BY 2 desc
        )
    WHERE (placement_name  LIKE ('%LIVE_Endpoints%')  OR placement_name  LIKE ('%House+Ad%'))
    GROUP BY 1
    ORDER BY 1

    #FSN Prior Week
    SELECT
  REGEXP_EXTRACT(jsonPayload.csid, r"fubo_([a-zA-Z_]*)") as d1,
  SPLIT(jsonPayload.prof, "_")[OFFSET(1)] as d2,
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
    FROM `fubotv-prod.ad_proxy.ad_proxy_2021*`
    WHERE 1=1
    AND jsonPayload.message = 'VMAP'
    AND jsonPayload.maxd IS NOT NULL
    AND jsonPayload._fw_station = "120123"
    AND DATE(timestamp) >= '2021-05-27' AND DATE(timestamp) <= '2021-06-02'
    GROUP BY 1,2
    ORDER BY 1 

    #House FSN Prior Week

      SELECT 
    DISTINCT
    CASE WHEN device IS NULL THEN NULL
    WHEN device = '' THEN NULL
    WHEN device = 'on' THEN NULL
    WHEN device = 'dvr' THEN NULL
    WHEN device = 'tv' THEN 'androidtv' 
    WHEN device = 'vizio' THEN 'smart_tv'
    WHEN device = 'xbox' THEN 'xbox'
    WHEN device = 'androidtv' THEN 'androidtv'
    WHEN device = 'appletv' THEN 'appletv'
    WHEN device = 'mobile' THEN 'mobileweb'
    WHEN device = 'iphone' THEN 'iphone'
    WHEN device = 'fire' THEN 'firetv'
    WHEN device = 'tablet' THEN 'android_tablet'
    WHEN device = 'lgtv' THEN 'smart_tv'
    WHEN device = 'android_phone' THEN 'android_phone'
    WHEN device = 'apple_tv' THEN 'appletv'
    WHEN device = 'samsung_tv' THEN 'smart_tv'
    WHEN device = 'phone' THEN 'android_phone'
    WHEN device = 'lg_tv' THEN 'smart_tv'
    WHEN device = 'web' THEN 'web'
    WHEN device = 'ipad' THEN 'ipad'
    WHEN device = 'ios_tablet' THEN 'ipad'
    WHEN device = 'hisense' THEN 'smart_tv'
    WHEN device = 'ios_phone' THEN 'iphone'
    WHEN device = 'android_tv' THEN 'androidtv'
    WHEN device = 'android_tablet' THEN 'android_tablet'
    WHEN device = 'samsungtv' THEN 'smart_tv'
    WHEN device = 'chromecast' THEN 'chromecast'
    WHEN device = 'desktop' THEN 'web'
    WHEN device = 'fire_tv' THEN 'firetv'
    WHEN device = 'roku' THEN 'roku' 
    END AS device_cat, 
    SUM(creative_duration) AS House_ads_duration
    FROM 
        (
            SELECT 
            REGEXP_EXTRACT(url, r"caid=([a-zA-Z0-9]*)_[a-zA-Z0-9]*") AS tms_id,
            CAST(REGEXP_EXTRACT(url, r"creativeDuration=([0-9]*)") AS INT64) AS creative_duration,
            REGEXP_EXTRACT(url, r"fw_pl=([^&]*)") AS placement_name,
            COALESCE(REGEXP_EXTRACT(url, r"device=([a-zA-Z_]*)"), REGEXP_EXTRACT(url, r"csid=[a-zA-Z_]*_([a-zA-Z]*)_[a-zA-Z]*")) AS device,
            FROM `fubotv-prod.AdBeaconing.prod_logs` 
            WHERE TIMESTAMP >= ("2021-05-27") AND TIMESTAMP <= ("2021-06-02")
            AND url like '%_fw_station=120123%'
            AND resp_status = '200'
            ORDER BY 2 desc
        )
    WHERE (placement_name  LIKE ('%LIVE_Endpoints%')  OR placement_name  LIKE ('%House+Ad%'))
    GROUP BY 1
    ORDER BY 1