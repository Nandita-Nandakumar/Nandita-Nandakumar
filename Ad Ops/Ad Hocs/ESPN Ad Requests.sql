--- ESPN Ad Requests
SELECT DATE(timestamp), count(1) as ad_request
FROM `fubotv-prod.ad_proxy.stdout_*` 
WHERE DATE(timestamp) IN ('2022-06-19', '2022-06-26')
AND jsonPayload.message = "VMAP"
AND jsonPayload.playback_type = "live"
AND cast(jsonPayload._fw_station as int64) = 10179
GROUP By 1
ORDER BY 1 DESC