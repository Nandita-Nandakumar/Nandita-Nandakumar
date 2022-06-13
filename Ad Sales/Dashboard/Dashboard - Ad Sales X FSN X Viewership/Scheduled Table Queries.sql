-------------------------- FSN Pacing - YT - Impressions -----------------------------------
# Table Name : fubotv-dev.business_analytics.NN_fsn_pacing_yt_impressions
# Ref Table : ref_fsn_yt_impressions {GOOGLE SHEET TAB NAME AS IS while creating table}
# Scheduled Query Name : Fubotv-dev  --- fsn_pacing_yt_impressions
# Every Hour : 15th min of the hour

SELECT 
DATE(PARSE_DATE('%m/%d/%Y',Event_Date)) as Event_Date,
Platform_Type,
Platform,
Station_Name,
Network_Group,
Net_Counted_Ads,
ROUND(Revenue,2) AS Revenue,
ROUND(CPM,2) AS CPM
FROM 
(
SELECT 
CASE WHEN LOWER(Event_Date) = 'blank' THEN NULL
ELSE Event_Date
END AS Event_Date,
Platform_Type,
Platform,
Station_Name,
Network_Group,
Net_Counted_Ads,
Revenue,
CPM
FROM `fubotv-dev.business_analytics.ref_fsn_yt_impressions`
)
WHERE Event_Date IS NOT NULL

-------------------------- FSN Pacing - Impressions -----------------------------------
# Table Name : fubotv-dev.business_analytics.NN_fsn_pacing_impressions
# Ref Table : ref_fsn_pacing_impressions
# Scheduled Query Name : Fubotv-dev  --- fsn_pacing_impressions
# Every Hour : 15th min of the hour

SELECT 
DATE(PARSE_DATE('%m/%d/%Y',Event_Date)) AS Event_Date,
Platform_Type,
Platform,
Station_Name,
Network_Group,
CAST(Net_Counted_Ads AS INT64) AS Net_Counted_Ads,
ROUND(Revenue,2) AS Revenue,
ROUND(CPM,2) AS CPM
FROM 
(
SELECT 
CASE WHEN LOWER(Event_Date) = 'blank' THEN NULL
WHEN LOWER(Event_Date) = '#REF!' THEN NULL
ELSE Event_Date
END AS Event_Date,
Platform_Type,
Platform,
Station_Name,
Network_Group,
CASE WHEN LOWER(Net_Counted_Ads) = 'blank' THEN NULL
WHEN LOWER(Net_Counted_Ads) = 'na' THEN NULL
ELSE Net_Counted_Ads
END AS Net_Counted_Ads,
Revenue,
CPM
FROM `fubotv-dev.business_analytics.ref_fsn_pacing_impressions`
)
WHERE Event_Date IS NOT NULL
AND  Event_Date != '#REF!'

-------------------------- FSN Pacing - YT - Viewership -----------------------------------
# Table Name : fubotv-dev.business_analytics.NN_fsn_pacing_yt_viewership
# Ref Table : ref_fsn_pacing_yt_viewership
# Scheduled Query Name : Fubotv-dev  --- fsn_pacing_yt_viewership
# Every Hour : 15th min of the hour

SELECT 
DATE(PARSE_DATE('%m/%d/%Y',Event_Date)) as Date_Range,
Platform_Partner,
Channel_Name,
Network_Name,
Playback_Type,
ad_insertable_network,
on_off_platform as on_off_plat,
SUM(Hours) AS Hours,
SUM(Uniques) AS Uniques,
SUM (Streams) AS Streams,
FROM 
(
SELECT 
CASE WHEN LOWER(Date_Range) = 'blank' THEN NULL
ELSE Date_Range
END AS Event_Date,
Platform_Partner,
Channel_Name,
Network_Name,
Playback_Type,
ad_insertable_network,
Hours,
Uniques,
Streams,
on_off_platform
FROM `fubotv-dev.business_analytics.ref_fsn_pacing_yt_viewership`
)
WHERE Event_Date IS NOT NULL
GROUP BY 1,2,3,4,5,6,7


