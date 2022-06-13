# View Name: NN_Dashboard_FSN_Pacing_On_Off_YT_Stations_Impressions

# Impressions Pull

/* NN - Station Impressions + FSN Pacing Impressions + FSN YT Impressions */

WITH all_excl_fsn AS 
(
SELECT DISTINCT Event_Date, 
"On Platform" AS Platform_Type, 
"fuboTV" AS Platform, 
Station_Name_Mapped, 
Network_Group_Mapped, 
0 AS CPM,
(SUM(Net_Counted_Ads)/1000) AS Revenue, 
SUM(Net_Counted_Ads) as Net_Counted_Ads
FROM 
(
SELECT DISTINCT Date(Event_date) as Event_Date, station_mapping AS  Station_Name_Mapped, network_owner AS Network_Group_Mapped,  Net_Counted_Ads
FROM `fubotv-dev.business_analytics.FW_MRM_Analytics_Station_Data` t1
INNER JOIN `fubotv-dev.business_analytics.FW_MRM_Analytics_Station_Mapping_Data_New` t2 on t1.Video_Group_Name = t2.Video_Group_Name
INNER JOIN `fubotv-dev.business_analytics.station_mapping_table_51220` t3 ON t2.Channel_Name = t3.station_name
WHERE station_mapping IS NOT NULL
ORDER BY 1
)
WHERE LOWER(Station_Name_Mapped) NOT IN ('fubo sports network')
GROUP BY 1,2,3,4,5,6
ORDER BY 1
)

, fsn AS
(
SELECT DISTINCT
Event_Date,
Platform_Type,
Platform,
Station_Name AS Station_Name_Mapped,
'fubo' AS Network_Group_Mapped,
CPM,
Revenue,
Net_Counted_Ads
FROM `fubotv-dev.business_analytics.NN_fsn_pacing_impressions`
)

,yt AS 
(
SELECT DISTINCT
Event_Date,
"Off Platform" AS Platform_Type,
"youtube" AS Platform,
"fubo Sports Network" AS Station_Name_Mapped,
"fubo" AS Network_Group_Mapped,
CPM,
0 AS Revenue, 
Net_Counted_Ads
FROM `fubotv-dev.business_analytics.NN_fsn_pacing_yt_impressions`   
)

, fsn_only AS (
SELECT 
t1.Event_Date,
t1.Platform_Type,
t1.Platform,
t1.Station_Name_Mapped,
t1.Network_Group_Mapped,
t1.CPM,
t1.Revenue,
CASE WHEN t1.Platform = 'youtube' AND t1.Net_Counted_Ads IS NULL AND t1.Event_Date = t2.Event_Date AND t1.Platform = t2.Platform THEN t2.Net_Counted_Ads
ELSE t1.Net_Counted_Ads END AS Net_Counted_Adss
FROM fsn t1
LEFT JOIN yt t2 ON t1.Event_Date = t2.Event_Date AND t2.Platform_Type = t1.Platform_Type AND t1.Platform = t2.Platform
)

,all_data_final AS (
SELECT DISTINCT t1.*
FROM all_excl_fsn t1
UNION ALL 
SELECT DISTINCT t2.*
FROM fsn_only t2
)

SELECT DISTINCT t1.*, Corp, Explanation
FROM all_data_final t1
JOIN `fubotv-dev.business_analytics.NN_Station_DAI_Explanation` t2 ---Explanation for DAI Eligible Stations
 ON t1.Station_Name_Mapped = t2.Station
WHERE DATE_TRUNC(Event_Date, month) <= DATE_TRUNC(current_date-1,month)