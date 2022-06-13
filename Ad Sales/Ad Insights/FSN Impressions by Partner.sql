WITH v4_logs_data AS
(
SELECT DISTINCT 
DATE(PARSE_TIMESTAMP('%Y%m%d%H%M%S',event_time),'EST') as event_date,
site_section_id,
placement_id,
transaction_id,
count (*) counts
FROM `fubotv-prod.dmp.de_sync_freewheel_logs` t1
WHERE LOWER(event_name) LIKE '%defaultimp%'
AND DATE(PARSE_TIMESTAMP('%Y%m%d%H%M%S',event_time), 'EST') >= '2021-04-01'
AND placement_id NOT IN ('28357916','28357921','44015286','44015291','44025664','44025669','44026928','51228140','51255705','-1') -- removing house ads and others
AND placement_id IS NOT NULL
AND site_section_id IS NOT NULL
GROUP BY 1,2,3,4
ORDER BY 1 DESC
)

, view_data AS
(
    SELECT DISTINCT 
    event_date, 
    transaction_id, 
    station_id,
    user_id as account_code
    FROM `fubotv-prod.FW_Views.FW_Logs_View_EST` 
    ORDER BY 1
)

, site_section_info AS
(
    SELECT 
    id as site_id, 
    name as site_name,
    split(name, '|')[SAFE_OFFSET(0)] as site_name1,
    split(name, '|')[SAFE_OFFSET(1)] as site_name2,
    split(name, '|')[SAFE_OFFSET(2)] as site_name3,
    split(name, '|')[SAFE_OFFSET(3)] as site_name4
    from `fubotv-prod.dmp.de_sync_freewheel_companion_site_section_v4`
)

SELECT t1.*, t2.station_id, t3.site_name, t3.site_name1, t3.site_name2, t3.site_name3, t3.site_name4
FROM v4_logs_data t1
JOIN view_data t2 ON t1.transaction_id = t2.transaction_id AND t1.event_date = t2.event_date
JOIN site_section_info t3 ON t1.site_section_id = t3.site_id 
WHERE station_id = 120123
AND t1.event_date = '2021-07-12'