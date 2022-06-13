-- FW MRM Mapping Table
SELECT DISTINCT t1.Video_Group_Name, t2.Channel_Name, t3.station_mapping AS Station_Name_Mapped, t3.network_owner AS Network_Group_Mapped
FROM `fubotv-dev.business_analytics.FW_MRM_Analytics_Station_Data` t1
LEFT JOIN `fubotv-dev.business_analytics.FW_MRM_Analytics_Station_Mapping_Data_New` t2 on t1.Video_Group_Name = t2.Video_Group_Name
LEFT JOIN `fubotv-dev.business_analytics.station_mapping_table_51220` t3 ON t2.Channel_Name = t3.station_name
WHERE t2.Channel_Name IS NULL
ORDER BY 2

-- Ad Insertable Mapping Table
SELECT DISTINCT t1.Channel as Channel_Name, t2.channel as mapped_channel, ad_insertable
FROM `fubotv-prod.data_insights.video_in_progress_via_va_view` t1
LEFT JOIN `fubotv-dev.business_analytics.AdInsertable_Networks` t2 ON t1.Channel = t2.channel
--LEFT JOIN `fubotv-dev.business_analytics.station_mapping_table_51220` t3 ON t1.channel = t3.station_name
WHERE t1.Channel IS NOT NULL
AND t1.Channel NOT LIKE ""
AND LOWER(t1.Channel) NOT LIKE ('%test%')
AND t2.channel IS NULL
ORDER BY 2
