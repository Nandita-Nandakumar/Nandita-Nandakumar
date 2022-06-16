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