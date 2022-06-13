SELECT DISTINCT event_date, station_id, Impressions
FROM 
(
SELECT DISTINCT DATE(PARSE_TIMESTAMP('%Y%m%d%H%M%S',event_time), 'EST') as event_date, TIME(PARSE_TIMESTAMP('%Y%m%d%H%M%S',event_time), 'EST') as event_time, 
station_id, 
video_asset_id,
CASE WHEN station_id IN  (131921,  1238520005 , 1238520006 , 120123 , 1238520002 , 1238520004)
THEN 'Conmebol'
ELSE 'Others'
END c_group,
COUNT (*) AS Impressions
FROM `fubotv-prod.dmp.de_sync_freewheel_logs` t1
INNER JOIN `fubotv-prod.FW_Views.FW_Logs_View` t2 using (transaction_id)
WHERE event_name = 'defaultImpression' -- sum of default impressions 
AND sales_channel_type IS NOT NULL
AND  DATE(PARSE_TIMESTAMP('%Y%m%d%H%M%S',event_time), 'EST') >= '2021-05-27'
AND placement_id NOT IN ('28357916','28357921','44015286','44015291',
'44025664','44025669','44026928','51228140','51255705','-1', '51666331','51502145') -- removing house ads and others
GROUP BY 1,2,3,4,5
ORDER BY 1
)
WHERE c_group = 'Conmebol'


-- NEW -- need to remove house ads
WITH placement_info AS
(
    SELECT DISTINCT t2.id as placement_id, t2.name as placement_name
    FROM `fubotv-prod.dmp.de_sync_freewheel_companion_placement_v4` as t2
),

site_section_info AS
(
    SELECT DISTINCT id as site_section_id ,name as site_section_name
    FROM `fubotv-prod.dmp.de_sync_freewheel_companion_site_section_v4`
)

SELECT DISTINCT 
 event_time, 
 station_id,
 CASE station_id
    WHEN 120123 THEN 'fubo Sports Network'
    WHEN 1238520002 THEN 'fubo Sports Network 2'
    WHEN 1238520004 THEN 'fubo Sports Network 3'
    WHEN 131921 THEN 'Eliminatorias CONMEBOL'
    WHEN 1238520005 THEN 'Eliminatorias CONMEBOL 2'
    ELSE 'Eliminatorias CONMEBOL 3'
    END AS station_name,
 placement_name,
 site_section_name,
 SUm(Impressions) as Impressions
FROM 
(
SELECT DISTINCT DATE(PARSE_TIMESTAMP('%Y%m%d%H%M%S',event_time), 'EST') as event_date, TIME(PARSE_TIMESTAMP('%Y%m%d%H%M%S',event_time), 'EST') as event_time, 
station_id, 
video_asset_id,
placement_name,
site_section_name,
CASE WHEN station_id IN  (131921,  1238520005 , 1238520006 , 120123 , 1238520002 , 1238520004)
THEN 'Conmebol'
ELSE 'Others'
END c_group,
COUNT (*) AS Impressions
FROM `fubotv-prod.dmp.de_sync_freewheel_logs` t1
INNER JOIN `fubotv-prod.FW_Views.FW_Logs_View` t2 USING (transaction_id)
INNER JOIN placement_info t3 USING (placement_id)
INNER JOIN site_section_info t4 USING (site_section_id)
WHERE event_name = 'defaultImpression' -- sum of default impressions 
AND sales_channel_type IS NOT NULL
AND  DATE(PARSE_TIMESTAMP('%Y%m%d%H%M%S',event_time), 'EST') >= '2021-06-03'
GROUP BY 1,2,3,4,5,6
ORDER BY 1
)
WHERE c_group = 'Conmebol'
GROUP BY 1,2,3,4,5
ORDER BY 3,5 DESC

--experimenting

WITH placement_info AS
(
    SELECT DISTINCT t2.id as placement_id, t2.name as placement_name
    FROM `fubotv-prod.dmp.de_sync_freewheel_companion_placement_v4` as t2
),

site_section_info AS
(
    SELECT DISTINCT id as site_section_id ,name as site_section_name
    FROM `fubotv-prod.dmp.de_sync_freewheel_companion_site_section_v4`
)

,video_asset_info AS
(
    SELECT DISTINCT id as video_asset_id ,name as video_asset_name
    FROM `fubotv-prod.dmp.de_sync_freewheel_companion_site_section_v4`
)

SELECT DISTINCT 
 event_date,
 event_time, 
 station_id,
 CASE station_id
    WHEN 120123 THEN 'fubo Sports Network'
    WHEN 1238520002 THEN 'fubo Sports Network 2'
    WHEN 1238520004 THEN 'fubo Sports Network 3'
    WHEN 131921 THEN 'Eliminatorias CONMEBOL'
    WHEN 1238520005 THEN 'Eliminatorias CONMEBOL 2'
    ELSE 'Eliminatorias CONMEBOL 3'
    END AS station_name,
 placement_name,
 site_section_name,
 video_asset_id,
 video_asset_name,
 SUM(Impressions) as Impressions
FROM 
(
SELECT DISTINCT DATE(PARSE_TIMESTAMP('%Y%m%d%H%M%S',event_time), 'EST') as event_date, TIME(PARSE_TIMESTAMP('%Y%m%d%H%M%S',event_time), 'EST') as event_time, 
station_id, 
video_asset_id,
t5.video_asset_name,
placement_name,
site_section_name,
CASE WHEN station_id IN  (131921,  1238520005 , 1238520006 , 120123 , 1238520002 , 1238520004)
THEN 'Conmebol'
ELSE 'Others'
END c_group,
COUNT (*) AS Impressions
FROM `fubotv-prod.dmp.de_sync_freewheel_logs` t1
INNER JOIN `fubotv-prod.FW_Views.FW_Logs_View` t2 USING (transaction_id)
INNER JOIN placement_info t3 USING (placement_id)
INNER JOIN site_section_info t4 USING (site_section_id)
INNER JOIN video_asset_info t5 USING (video_asset_id)
WHERE event_name = 'defaultImpression'
AND placement_id NOT IN ('28357934','233521431','44015286','44015291','44025664','44025669','44026928','','-1') -- house ads
AND placement_id IS NOT NULL
AND  DATE(PARSE_TIMESTAMP('%Y%m%d%H%M%S',event_time), 'EST') = '2021-06-03'
GROUP BY 1,2,3,4,5,6,7
ORDER BY 1
)
WHERE c_group = 'Conmebol'
GROUP BY 1,2,3,4,5,6,7,8
ORDER BY 4,6 DESC


--Video name that worked

WITH log_info AS
(
SELECT DISTINCT event_date, station_id, video_asset_id, Impressions
FROM 
(
SELECT DISTINCT DATE(PARSE_TIMESTAMP('%Y%m%d%H%M%S',event_time), 'EST') as event_date, 
station_id, 
video_asset_id,
CASE WHEN station_id IN  (131921,  1238520005 , 1238520006 , 120123 , 1238520002 , 1238520004)
THEN 'Conmebol'
ELSE 'Others'
END c_group,
COUNT (*) AS Impressions
FROM `fubotv-prod.dmp.de_sync_freewheel_logs` t1
INNER JOIN `fubotv-prod.FW_Views.FW_Logs_View_EST` t2 using (transaction_id)
WHERE event_name = 'defaultImpression' -- sum of default impressions 
AND sales_channel_type IS NOT NULL
AND  DATE(PARSE_TIMESTAMP('%Y%m%d%H%M%S',event_time), 'EST') >= '2021-06-03'
GROUP BY 1,2,3
ORDER BY 1
)
WHERE c_group = 'Conmebol'
),

video_info AS
(
    SELECT DISTINCT t2.id as id, t2.name as video_name, 
    FROM `fubotv-prod.dmp.de_sync_freewheel_companion_video_asset_v4` as t2 
)

SELECT DISTINCT t1.*, t2.video_name
FROM log_info t1
INNER JOIN video_info t2 ON t1.video_asset_id = t2.id
