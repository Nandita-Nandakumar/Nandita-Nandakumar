# Proxy Data
# Scheduled Query : NN_Ad_Ops_Proxy_v2_Past
# Table: fubotv-dev.business_analytics.NN_Ad_Ops_Proxy_v2_Past

WITH cte_ad_requests as(
    SELECT distinct 
        DATE( timestamp ) AS Date_UTC,
        REGEXP_EXTRACT(jsonPayload.outbound__url,"csid=([^&]*)") AS outbound_csid,
        REGEXP_EXTRACT(jsonPayload.outbound__url,"txn_id=([^&]*)") AS txn_id,
        jsonPayload._fw_station as station_id,
        s.display_name,
        s.network,
        jsonpayload.mind as total_avail_duration,
        CASE WHEN jsonpayload.total_ad_duration > jsonPayload.mind THEN jsonPayload.mind ELSE jsonpayload.total_ad_duration END as total_ad_server_duration,
    FROM `fubotv-prod.ad_proxy.*`
        left outer join `fubotv-prod.dmp.de_sync_stations` s on jsonPayload._fw_station = cast(s.station_id as string)
    WHERE jsonPayload.message = "VMAP"
    AND jsonPayload.playback_type = "live"
),

station_details AS (
    SELECT distinct CAST(station_id AS string ) as station_id , station_name, station_mapping, network_owner
    FROM `fubotv-prod.dmp.de_sync_stations`
    JOIN `fubotv-dev.business_analytics.station_mapping_table_51220` USING (station_name)
)

, cte_imps as (
        SELECT
        distinct 
        DATE( timestamp ) AS Date_UTC,
        jsonpayload.http_request_query_txn_id AS txn_id,
        jsonpayload.http_request_query_ad_idx AS ad_idx,
        jsonPayload.http_request_query_creativeduration as total_ad_duration_filled,
        CASE WHEN (jsonPayload.http_request_query_fw_pl LIKE "%House Ad%" OR jsonPayload.http_request_query_fw_pl = "All_LIVE_Endpoints") THEN 1 ELSE 0 END as house_ads_imps,
        CASE WHEN (jsonPayload.http_request_query_fw_pl NOT LIKE "%House Ad%" AND jsonPayload.http_request_query_fw_pl != "All_LIVE_Endpoints") THEN 1 ELSE 0 END as normal_imps,
        CASE WHEN (jsonPayload.http_request_query_fw_pl NOT LIKE "%House Ad%" AND jsonPayload.http_request_query_fw_pl != "All_LIVE_Endpoints") THEN jsonPayload.http_request_query_creativeduration ELSE 0 END as total_ad_duration_filled_without_house_ads,
    FROM `fubotv-prod.AdBeaconing_Impressions_2.*`
    WHERE  jsonPayload.http_request_query_eventType = "defaultImpression"
    AND jsonPayload.http_request_query_playback_type = "live"
),
cte_aggregate_requests_imps as (
 SELECT
        a.Date_UTC,
        a.outbound_csid,
        a.txn_id,
        a.station_id,
        a.network,
        a.display_name as station_name,
        a.total_avail_duration,
        a.total_ad_server_duration,
        a.total_ad_server_duration / a.total_avail_duration * 100 as ad_server_fill_rate,
        count(i.txn_id) as impression_count,
        sum(i.house_ads_imps) as house_ads_imps,
        sum(i.normal_imps) as normal_imps,
        SUM(i.total_ad_duration_filled) as total_ad_duration,
        sum(i.total_ad_duration_filled_without_house_ads) as total_ad_duration_filled_without_house_ads,
        sum(case when i.total_ad_duration_filled is not null then i.total_ad_duration_filled else 0 end ) / a.total_avail_duration * 100 as time_fill_rate_including_house_ads,
        sum(case when i.total_ad_duration_filled_without_house_ads is not null then i.total_ad_duration_filled_without_house_ads else 0 end ) / a.total_avail_duration * 100 as time_fill_rate_excluding_house_ads
    FROM
    cte_ad_requests a
left JOIN
    cte_imps i ON a.txn_id = i.txn_id
group by 1,2,3,4,5,6,7,8,9
)

, consolidated_data AS (
select DISTINCT
        Date_UTC,
        station_id,
        count(txn_id) as ad_request_counts,
        sum(total_avail_duration) as total_avail_duration,
        sum(total_ad_server_duration) as total_ad_server_duration,
        sum(impression_count) as total_imps_count,
        sum(house_ads_imps) as house_ads_imps,
        sum(normal_imps) as normal_imps,
        sum(total_ad_duration) as total_ad_duration,
        sum(total_ad_duration_filled_without_house_ads) as total_ad_duration_without_house_ads,
from cte_aggregate_requests_imps
WHERE Date_UTC <= Current_date()-1
group by 1,2
order by 1 desc 
)

------- modifying station names and network owners to remove duplication across station_ids
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
END AS station_name,
t2.network_owner,
FROM consolidated_data t1
JOIN station_details t2 ON t1.station_id = t2.station_id
)

SELECT DISTINCT t1.* EXCEPT (network_owner), 
CASE WHEN station_name = "beIN SPORTS (Canada)" THEN "beIN SPORTS (Canada)"
WHEN station_name = "beIN SPORTS En Espa√±ol (Canada)" THEN "beIN SPORTS (Canada)"
WHEN station_name = "BabyTV Spanish" THEN "FOX"
WHEN station_name = "My Network TV" THEN "CBS"
WHEN station_name = "GAC Family" THEN "GAC Media"
ELSE network_owner 
END AS Network_Name
FROM all_data t1
WHERE Date_UTC >= '2021-01-01'
AND Date_UTC <= current_date()-1
ORDER BY 1 DESC

# Proxy Data - Hourly
# Scheduled Query : NN_Ad_Ops_Proxy_v2_Current
# Table: fubotv-dev.business_analytics.NN_Ad_Ops_Proxy_v2_Current

WITH cte_ad_requests as(
    SELECT distinct 
        DATE( timestamp ) AS Date_UTC,
        EXTRACT(HOUR FROM DATETIME(timestamp)) AS Date_Hour_UTC,
        REGEXP_EXTRACT(jsonPayload.outbound__url,"csid=([^&]*)") AS outbound_csid,
        REGEXP_EXTRACT(jsonPayload.outbound__url,"txn_id=([^&]*)") AS txn_id,
        jsonPayload._fw_station as station_id,
        s.display_name,
        s.network,
        jsonpayload.mind as total_avail_duration,
        CASE WHEN jsonpayload.total_ad_duration > jsonPayload.mind THEN jsonPayload.mind ELSE jsonpayload.total_ad_duration END as total_ad_server_duration,
    FROM `fubotv-prod.ad_proxy.*`
        left outer join `fubotv-prod.dmp.de_sync_stations` s on jsonPayload._fw_station = cast(s.station_id as string)
    WHERE jsonPayload.message = "VMAP"
    AND jsonPayload.playback_type = "live"
),

station_details AS (
    SELECT distinct CAST(station_id AS string ) as station_id , station_name, station_mapping, network_owner
    FROM `fubotv-prod.dmp.de_sync_stations`
    JOIN `fubotv-dev.business_analytics.station_mapping_table_51220` USING (station_name)
)

, cte_imps as (
        SELECT
        distinct 
        DATE( timestamp ) AS Date_UTC,
        EXTRACT(HOUR FROM DATETIME(timestamp)) AS Date_Hour_UTC,
        jsonpayload.http_request_query_txn_id AS txn_id,
        jsonpayload.http_request_query_ad_idx AS ad_idx,
        jsonPayload.http_request_query_creativeduration as total_ad_duration_filled,
        CASE WHEN (jsonPayload.http_request_query_fw_pl LIKE "%House Ad%" OR jsonPayload.http_request_query_fw_pl = "All_LIVE_Endpoints") THEN 1 ELSE 0 END as house_ads_imps,
        CASE WHEN (jsonPayload.http_request_query_fw_pl NOT LIKE "%House Ad%" AND jsonPayload.http_request_query_fw_pl != "All_LIVE_Endpoints") THEN 1 ELSE 0 END as normal_imps,
        CASE WHEN (jsonPayload.http_request_query_fw_pl NOT LIKE "%House Ad%" AND jsonPayload.http_request_query_fw_pl != "All_LIVE_Endpoints") THEN jsonPayload.http_request_query_creativeduration ELSE 0 END as total_ad_duration_filled_without_house_ads,
    FROM `fubotv-prod.AdBeaconing_Impressions_2.*`
    WHERE  jsonPayload.http_request_query_eventType = "defaultImpression"
    AND jsonPayload.http_request_query_playback_type = "live"
),

cte_aggregate_requests_imps as (
 SELECT
        a.Date_UTC,
        a.Date_Hour_UTC,
        a.outbound_csid,
        a.txn_id,
        a.station_id,
        a.network,
        a.display_name as station_name,
        a.total_avail_duration,
        a.total_ad_server_duration,
        a.total_ad_server_duration / a.total_avail_duration * 100 as ad_server_fill_rate,
        count(i.txn_id) as impression_count,
        sum(i.house_ads_imps) as house_ads_imps,
        sum(i.normal_imps) as normal_imps,
        SUM(i.total_ad_duration_filled) as total_ad_duration,
        sum(i.total_ad_duration_filled_without_house_ads) as total_ad_duration_filled_without_house_ads,
        sum(case when i.total_ad_duration_filled is not null then i.total_ad_duration_filled else 0 end ) / a.total_avail_duration * 100 as time_fill_rate_including_house_ads,
        sum(case when i.total_ad_duration_filled_without_house_ads is not null then i.total_ad_duration_filled_without_house_ads else 0 end ) / a.total_avail_duration * 100 as time_fill_rate_excluding_house_ads
    FROM
    cte_ad_requests a
left JOIN
    cte_imps i ON a.txn_id = i.txn_id
group by 1,2,3,4,5,6,7,8,9,10
)

, consolidated_data AS (
select DISTINCT
        Date_UTC,
        Date_Hour_UTC,
        station_id,
        count(txn_id) as ad_request_counts,
        sum(total_avail_duration) as total_avail_duration,
        sum(total_ad_server_duration) as total_ad_server_duration,
        sum(impression_count) as total_imps_count,
        sum(house_ads_imps) as house_ads_imps,
        sum(normal_imps) as normal_imps,
        sum(total_ad_duration) as total_ad_duration,
        sum(total_ad_duration_filled_without_house_ads) as total_ad_duration_without_house_ads,
from cte_aggregate_requests_imps
WHERE Date_UTC >= Current_date()-1
group by 1,2,3
order by 1 desc 
)

------- modifying station names and network owners to remove duplication across station_ids
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
END AS station_name,
t2.network_owner,
FROM consolidated_data t1
JOIN station_details t2 ON t1.station_id = t2.station_id
)

SELECT DISTINCT t1.* EXCEPT (network_owner), 
CASE WHEN station_name = "beIN SPORTS (Canada)" THEN "beIN SPORTS (Canada)"
WHEN station_name = "beIN SPORTS En Espa√±ol (Canada)" THEN "beIN SPORTS (Canada)"
WHEN station_name = "BabyTV Spanish" THEN "FOX"
WHEN station_name = "My Network TV" THEN "CBS"
WHEN station_name = "GAC Family" THEN "GAC Media"
ELSE network_owner 
END AS Network_Name
FROM all_data t1
ORDER BY 1,2
################################################################### TABLEAU QUERIES #########################################################
-------------------------------------------------------------- Tableau Query ----------------------------------------------------

----- NN Ad Proxy - Past Data 
SELECT t1.*
FROM `fubotv-dev.business_analytics.NN_Ad_Ops_Proxy_v2_Past` t1
WHERE Date_UTC >= '2022-01-01'

---- NN Ad proxy - Current Data

WITH today AS(
SELECT * EXCEPT (Max_Hour)
FROM 
(
SELECT DISTINCT *,MAX(Date_Hour_UTC) OVER() Max_Hour
FROM `fubotv-dev.business_analytics.NN_Ad_Ops_Proxy_v2_Current`
WHERE Date_UTC = CURRENT_DATE()
ORDER BY 1 DESC
)
WHERE Date_Hour_UTC < Max_Hour -- Removes the incomplete hour
)

, yesterday AS (
SELECT DISTINCT *
FROM `fubotv-dev.business_analytics.NN_Ad_Ops_Proxy_v2_Current`
WHERE Date_UTC = CURRENT_DATE() -1
ORDER BY 1 DESC
)

, final AS (
SELECT DISTINCT t1.*
FROM today t1
UNION ALL 
SELECT DISTINCT t2.*
FROM yesterday t2
)

SELECT DISTINCT *
FROM final
ORDER BY 1 DESC , 2 DESC