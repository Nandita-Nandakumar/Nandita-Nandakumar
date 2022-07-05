################################################################# SCHEDULED QUERIES #############################################################
------ Magnite & SpotX adv ----
--- Scheduled Query : NN_Ad_Ops_Magnite_SpotX_Rev
--- Table : fubotv-dev.business_analytics.NN_Ad_Ops_Magnite_SpotX_Rev
--- Updates at 5:20AM every morning.
-- Last Updated : 07/05 - category mapping ; 07/01 - Removed Distinct from Raw Tables

WITH magnite_data AS (
  SELECT 
  DATE(day) as day,
  'magnite' as data_source,
  LOWER(t1.advertiser_domain) as ad_domain,
  category,
  (total_net_cpm) AS Net_CPM,
  (total_gross_cpm) AS Gross_CPM,
  SUM(total_impressions) as Impressions,
  SUM(total_gross_revenue) as Gross_Revenue,
  SUM(total_net_revenue) as Net_Revenue
  FROM `fubotv-prod.dmp.de_ads_report_magnite_revenue` t1
  LEFT JOIN `fubotv-prod.adops_avails_research.magite_advertiser_category_mapping` t2 ON t1.advertiser_domain = t2.advertiser_domain
  WHERE DATE(day) >= '2022-01-01'
  AND t1.advertiser_domain IS NOT NULL
  AND t1.advertiser_domain != ''
  AND total_net_revenue IS NOT NULL
  AND total_net_revenue != 0
  AND total_gross_cpm IS NOT NULL
  AND total_gross_cpm != 0 
  GROUP BY 1,2,3,4,5,6
)

, spotx_data AS (
  SELECT 
  DATE(day) as day,
  'spotx' as data_source,
  LOWER(t1.advertiser_domain) as ad_domain,
  category,
  NULL AS Net_CPM,
  NULL AS Gross_CPM,
  sum(total_impressions) as Impressions, 
  SUM(total_gross_revenue) as Gross_revenue,
  SUM(total_net_revenue) as Net_revenue, 
  FROM `fubotv-prod.dmp.de_ads_report_spotx_revenue` t1
  LEFT JOIN `fubotv-prod.adops_avails_research.spotx_advertiser_category_mapping` t2 ON t1.advertiser_domain = t2.advertiser_domain
  WHERE DATE(day) >= '2022-02-01'-- AND DATE(day) <= '2022-05-05'
  AND t1.advertiser_domain IS NOT NULL
  AND t1.advertiser_domain != ''
  AND total_net_revenue IS NOT NULL
  AND total_net_revenue != 0
  GROUP BY 1,2,3,4,5,6
  ORDER BY 3
)

,combined_data AS (

SELECT  t1.*
FROM magnite_data t1
UNION DISTINCT
SELECT  t2.*
FROM spotx_data t2 
ORDER BY 1
)

SELECT DISTINCT 
day,
data_source,
LOWER(ad_domain) AS advertiser_domain,
category,
Net_CPM,
Gross_CPM,
sum(Impressions) as Impressions, 
SUM(Gross_revenue) as Gross_revenue,
SUM(Net_revenue) as Net_revenue, 
FROM combined_data
GROUP BY 1,2,3,4,5,6

------------------------------------------------------------------------------------------------------------------

---- APS Data --------
--- Scheduled Query : NN_Ad_Ops_APS_Rev
--- Table : fubotv-dev.business_analytics.NN_Ad_Ops_APS_Rev
--- last update : 07/05 - category mapping ; 07/01 - Removed DISTINCT

WITH aps_data AS (
SELECT 
Date AS day,
'aps' AS data_source,
REGEXP_REPLACE(LOWER(Buyer), r"(\sus|\sca)", "") As ad_domain, -- cleanup of buyers
category,
(impressions) AS Impressions,
(Earnings) AS Gross_Revenue,
eCPM AS Net_CPM
FROM `fubotv-prod.dmp.de_ads_report_aps_advertiser_revenue` t1
LEFT JOIN `fubotv-prod.adops_avails_research.aps_advertiser_category_mapping` t2 ON t1.Buyer = t2.advertiser_domain
WHERE Date >= '2022-01-01'
AND Buyer IS NOT NULL
AND Buyer != ""
ORDER BY 1
--GROUP BY 1,2,3
)

,clean_data AS (
SELECT DISTINCT
day,
data_source,
LOWER(ad_domain) AS advertiser_domain,
category,
SUM(impressions) AS impressions,
SUM(Gross_Revenue) AS Gross_Revenue,
(Net_CPM) AS Net_CPM
FROM aps_Data
GROUP BY 1,2,3,4,7
ORDER BY 1,3
)

SELECT DISTINCT *
FROM clean_data
ORDER BY 1

###################################################################### TABLEAU #####################################################

---- NN Ad Ops - Revenue Dash # included category mapping 
(
SELECT 
t1.day,
t1.data_source,
LOWER(t1.advertiser_domain) AS advertiser_domain,
category,
t1.Net_CPM,
t1.Gross_CPM,
NULL as eCPM,
t1.Impressions,
t1.Net_Revenue,
t1.Gross_Revenue
FROM `fubotv-dev.business_analytics.NN_Ad_Ops_Magnite_SpotX_Rev` t1
)
UNION ALL
(
SELECT 
t2.day,
t2.data_source,
LOWER(t2.advertiser_domain) AS advertiser_domain,
category,
NULL AS Net_CPM,
NULL AS Gross_CPM,
t2.Net_CPM AS eCPM,
t2.Impressions,
NULL AS Net_Revenue, 
t2.Gross_Revenue,
FROM`fubotv-dev.business_analytics.NN_Ad_Ops_APS_Rev` t2 
)

--------------------------------------------------------------- Tableau -> Advertiser Domain Mapping ----------------------------
IF CONTAINS(LOWER([Ad Domain]), 'amazon' ) THEN 'Amazon' END
ELSEIF CONTAINS(LOWER([Ad Domain]),  'bayer') THEN 'Bayer'
ELSEIF CONTAINS(LOWER([Ad Domain]),  'cibc' ) THEN 'cibc.com'
ELSEIF CONTAINS(LOWER([Ad Domain]),  'disney') THEN 'Disney'
ELSEIF CONTAINS(LOWER([Ad Domain]),  'at&t') THEN 'AT&T'
ELSEIF CONTAINS(LOWER([Ad Domain]),  'horizon') THEN 'Horizon US'
ELSEIF CONTAINS(LOWER([Ad Domain]),  'intuit') THEN 'Intuit'
ELSEIF CONTAINS(LOWER([Ad Domain]),  'turbo') THEN 'Intuit'
ELSEIF CONTAINS(LOWER([Ad Domain]),  'chase') THEN 'JP Morgan Chase'
ELSEIF CONTAINS(LOWER([Ad Domain]),  'honest company') THEN 'Honest Company'
ELSEIF CONTAINS(LOWER([Ad Domain]),  'mindshare') THEN 'Mindshare'
ELSEIF CONTAINS(LOWER([Ad Domain]),  'mazda') THEN 'Mazda'
ELSEIF CONTAINS(LOWER([Ad Domain]),  'acura') THEN 'Acura'
ELSEIF CONTAINS(LOWER([Ad Domain]),  'honda') THEN 'Honda'
ELSEIF CONTAINS(LOWER([Ad Domain]),  'subaru') THEN 'Subaru'
ELSEIF CONTAINS(LOWER([Ad Domain]),  'toyota') THEN 'Toyota'
ELSEIF CONTAINS(LOWER([Ad Domain]),  'nissan') THEN 'Nissan'
ELSEIF CONTAINS(LOWER([Ad Domain]),  'chevrolet') THEN 'Chevrolet'
ELSEIF CONTAINS(LOWER([Ad Domain]),  'cbslocal') THEN 'CBS Local'
ELSEIF CONTAINS(LOWER([Ad Domain]),  'shoprite') THEN 'Shoprite'
ELSEIF CONTAINS(LOWER([Ad Domain]),  'tide.com') THEN 'P&G' 
ELSEIF CONTAINS(LOWER([Ad Domain]),  'febreze.com') THEN 'P&G' 
ELSEIF CONTAINS(LOWER([Ad Domain]),  'ilovegain.com') THEN 'P&G' 
ELSEIF CONTAINS(LOWER([Ad Domain]),  'procter') THEN 'P&G' 
ELSEIF CONTAINS(LOWER([Ad Domain]),  'downy.com') THEN 'P&G' 
ELSEIF CONTAINS(LOWER([Ad Domain]),  'bountytowels.com') THEN 'P&G' 
ELSEIF CONTAINS(LOWER([Ad Domain]),  'dawn-dish.com') THEN 'P&G' 
ELSEIF CONTAINS(LOWER([Ad Domain]),  'charmin.com') THEN 'P&G' 
ELSEIF CONTAINS(LOWER([Ad Domain]),  'tampax.com') THEN 'P&G' 
ELSEIF CONTAINS(LOWER([Ad Domain]),  'purina - US') THEN 'P&G' 
ELSEIF CONTAINS(LOWER([Ad Domain]),  'pampers.com') THEN 'P&G' 
ELSEIF CONTAINS(LOWER([Ad Domain]),  'bouncefresh.com') THEN 'P&G' 
ELSEIF CONTAINS(LOWER([Ad Domain]),  'p&g') THEN 'P&G' 
ELSEIF CONTAINS(LOWER([Ad Domain]),  'unstopables.com') THEN 'P&G' 
ELSEIF CONTAINS(LOWER([Ad Domain]),  'nine-elements.com') THEN 'P&G' 
ELSEIF CONTAINS(LOWER([Ad Domain]),  'cascadeclean.com') THEN 'P&G' 
ELSEIF CONTAINS(LOWER([Ad Domain]),  'headandshoulders.com') THEN 'P&G' 
ELSEIF CONTAINS(LOWER([Ad Domain]),  'herbalessences.com') THEN 'P&G' 
ELSEIF CONTAINS(LOWER([Ad Domain]),  'always.com') THEN 'P&G' 
ELSEIF CONTAINS(LOWER([Ad Domain]),  'crest.com') THEN 'P&G' 
ELSEIF CONTAINS(LOWER([Ad Domain]),  'pggoodeveryday.com') THEN 'P&G' 
ELSEIF CONTAINS(LOWER([Ad Domain]),  'oralb.com') THEN 'P&G' 
ELSEIF CONTAINS(LOWER([Ad Domain]),  'audi') THEN 'Audi'
ELSEIF CONTAINS(LOWER([Ad Domain]),  'vw.com') THEN 'Volkswagon'
ELSEIF CONTAINS(LOWER([Ad Domain]),  'coca-cola') THEN 'The Coca-Cola Company - US'
ELSEIF CONTAINS(LOWER([Ad Domain]),  'facebook.com') THEN 'Facebook'
ELSEIF CONTAINS(LOWER([Ad Domain]),  'instagram.com') THEN 'Facebook'
ELSE [Ad Domain]
END