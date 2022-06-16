#############################################################################################################################################
########################## SCHEDULED QUERY ####################################

# Impression Variance - Beaconing Table 
# Scheduled Query : NN_Ad_Ops_Impressions_Variance_Beaconing
# Table: fubotv-dev.business_analytics.NN_Ad_Ops_Impressions_Variance_Beaconing
# Cadence : Daily Refresh

SELECT
DATE(timestamp) AS Date_UTC,
REGEXP_EXTRACT(jsonpayload.http_request_query_prof,r'_(\w*)\_') as device,
jsonPayload.http_request_query_eventtype as imp_type,
count(*) as Imp_count,
FROM `fubotv-prod.AdBeaconing_Impressions_2.*` --- This encompasses both the ad_beaconing_ and stdout_ tables
where jsonPayload.http_request_query_eventtype  in (
    'defaultImpression',
    'firstQuartile',
    'complete')
AND DATE(timestamp) <= Current_date()-1
AND REGEXP_EXTRACT(jsonpayload.http_request_query_prof,r'_(\w*)\_') IS NOT NULL
AND REGEXP_EXTRACT(jsonpayload.http_request_query_prof,r'_(\w*)\_') NOT IN ('dfp')
GROUP BY 1,2,3
ORDER BY 1,2,4 desc

# Impression Variance - Beaconing Table (current_date + 1 prev)
# Scheduled Query : NN_Ad_Ops_Impressions_Variance_Beaconing_Current
# Table: fubotv-dev.business_analytics.NN_Ad_Ops_Impressions_Variance_Beaconing_Current
# Cadence : Hourly Refresh

SELECT
DATE(timestamp) AS Date_UTC,
EXTRACT(HOUR FROM DATETIME(timestamp)) AS Date_Hour_UTC,
REGEXP_EXTRACT(jsonpayload.http_request_query_prof,r'_(\w*)\_') as device,
jsonPayload.http_request_query_eventtype as imp_type,
count(*) as Imp_count,
FROM `fubotv-prod.AdBeaconing_Impressions_2.*` --- This encompasses both the ad_beaconing_ and stdout_ tables
where jsonPayload.http_request_query_eventtype  in (
    'defaultImpression',
    'firstQuartile',
    'complete')
AND DATE(timestamp) >= Current_date()-1
AND REGEXP_EXTRACT(jsonpayload.http_request_query_prof,r'_(\w*)\_') IS NOT NULL
AND REGEXP_EXTRACT(jsonpayload.http_request_query_prof,r'_(\w*)\_') NOT IN ('dfp')
GROUP BY 1,2,3,4
ORDER BY 1,2,5 desc


############################################################## WITHIN TABLEAU ############################################################
# Impressions - Ad Beacons Tableau Dashboard Query

WITH today AS(
SELECT * EXCEPT (Max_Hour)
FROM 
(
SELECT DISTINCT *,MAX(Date_Hour_UTC) OVER() Max_Hour
FROM `fubotv-dev.business_analytics.NN_Ad_Ops_Impressions_Variance_Beaconing_Current`
WHERE Date_UTC = CURRENT_DATE()
ORDER BY 1 DESC
)
WHERE Date_Hour_UTC < Max_Hour -- Removes the incomplete hour
)

, yesterday AS (
SELECT DISTINCT *
FROM `fubotv-dev.business_analytics.NN_Ad_Ops_Impressions_Variance_Beaconing_Current`
WHERE Date_UTC != CURRENT_DATE()
ORDER BY 1 DESC
)

SELECT DISTINCT t1.*
FROM today t1
UNION ALL 
SELECT DISTINCT t2.*
FROM yesterday t2


# Ad Ops Dashboard - Main
#NN - Ad Ops - Current Query:
WITH today AS(
SELECT * EXCEPT (Max_Hour)
FROM 
(
SELECT DISTINCT *,MAX(Date_Hour_UTC) OVER() Max_Hour
FROM `fubotv-dev.business_analytics.NN_Ad_Ops_Monetized_Fill_Rate_Current`
WHERE Date_UTC = CURRENT_DATE()
ORDER BY 1 DESC
)
WHERE Date_Hour_UTC < Max_Hour -- Removes the incomplete hour
)

, yesterday AS (
SELECT DISTINCT *
FROM `fubotv-dev.business_analytics.NN_Ad_Ops_Monetized_Fill_Rate_Current`
WHERE Date_UTC != CURRENT_DATE()
ORDER BY 1 DESC
)

SELECT DISTINCT t1.*
FROM today t1
UNION ALL 
SELECT DISTINCT t2.*
FROM yesterday t2