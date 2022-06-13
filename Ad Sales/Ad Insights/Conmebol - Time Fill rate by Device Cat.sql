WITH ad_requests as(
    SELECT
        jsonpayload.outbound__caid as caid,
        jsonPayload.prof as prof,
        # FORMAT_TIMESTAMP("%m-%d-%Y-%H", timestamp, "America/New_York") AS hour,
        SUM(jsonpayload.mind) as total_avail_duration,
        SUM(CASE WHEN jsonpayload.total_ad_duration > jsonPayload.mind THEN jsonPayload.mind ELSE jsonpayload.total_ad_duration END) as total_ad_server_duration,
        COUNT(*) ad_request_counts
    FROM
        `fubotv-prod.ad_proxy.ad_proxy_*`
    WHERE
        jsonPayload.message = "VMAP"
    AND
        _TABLE_SUFFIX BETWEEN "20210528" AND "20210611"
    AND
        (jsonPayload.playback_type = "live")
    AND
        (
            jsonPayload._fw_station = "120123"
            OR jsonPayload._fw_station = "1238520002"
            OR jsonPayload._fw_station = "1238520004"
            OR jsonPayload._fw_station = "1238520005"
            OR jsonPayload._fw_station = "1238520006"
            OR jsonPayload._fw_station = "131921"
        )
    AND SPLIT(jsonPayload.outbound__caid, "_")[OFFSET(0)]
            IN (
                "EP033149890042",
                "EP033149890043",
                "EP033149890044",
                "EP033149890045",
                "EP033149890046",
                "EP033149890047",
                "EP033149890048",
                "EP033149890049",
                "EP033149890050",
                "EP033149890051",
                "EP036241260116",
                "EP036241260117",
                "EP036241260118",
                "EP036241260119",
                "EP036241260122",
                "EP036241260123",
                "EP036241260124",
                "EP036241260125",
                "EP036241260126",
                "EP036241260127",
                "EP038580530001",
                "EP038605420001",
                "EP038605420002",
                "EP038605440001",
                "EP038605440002",
                "EP038753280002",
                "EP038758570001",
                "EP038758570003",
                "EP038758620001",
                "EP038758620003",
                "FUBO20210528202830",
                "FUBO20210528204447",
                "FUBO20210601190616",
                "FUBO20210601190749",
                "FUBO20210601192345",
                "FUBO20210601192444",
                "FUBO20210601192536",
                "FUBO20210601192633",
                "FUBO20210601192718",
                "FUBO20210601192803",
                "FUBO20210601192836",
                "FUBO20210601192918",
                "FUBO20210601192951",
                "FUBO20210601193949",
                "FUBO20210602141534",
                "FUBO20210603185156",
                "FUBO20210603185306",
                "FUBOCOM0000001"
            )
    GROUP BY caid, prof
    ORDER BY caid
)
SELECT
    # imps.hour,
    imps.caid,
    ad_requests.prof,
    ad_requests.ad_request_counts,
    imps.house_ads_imps,
    imps.normal_imps,
    imps.total_imps_count,
    ad_requests.total_avail_duration,
    imps.total_ad_duration,
    ad_requests.total_ad_server_duration,
    imps.total_ad_duration_without_house_ads,
    ad_requests.total_ad_server_duration / ad_requests.total_avail_duration * 100 as ad_server_fill_rate,
    imps.total_ad_duration / ad_requests.total_avail_duration * 100 as time_fill_rate_including_house_ads,
    imps.total_ad_duration_without_house_ads / ad_requests.total_avail_duration * 100 as time_fill_rate_excluding_house_ads
FROM
(
    SELECT
        jsonPayload.http_request_query_caid as caid,
        jsonPayload.http_request_query_prof as prof,
        # FORMAT_TIMESTAMP("%m-%d-%Y-%H", timestamp, "America/New_York") AS hour,
        SUM(CASE WHEN (jsonPayload.http_request_query_fw_pl LIKE "%House Ad%" OR jsonPayload.http_request_query_fw_pl = "All_LIVE_Endpoints") THEN 1 ELSE 0 END) as house_ads_imps,
        SUM(CASE WHEN (jsonPayload.http_request_query_fw_pl NOT LIKE "%House Ad%" AND jsonPayload.http_request_query_fw_pl != "All_LIVE_Endpoints") THEN 1 ELSE 0 END) as normal_imps,
        SUM(jsonPayload.http_request_query_creativeduration) as total_ad_duration,
        SUM(CASE WHEN (jsonPayload.http_request_query_fw_pl NOT LIKE "%House Ad%" AND jsonPayload.http_request_query_fw_pl != "All_LIVE_Endpoints") THEN jsonPayload.http_request_query_creativeduration ELSE 0 END) as total_ad_duration_without_house_ads,
        COUNT(*) as total_imps_count
    FROM `fubotv-prod.AdBeaconing_Impressions_2.ad_beaconing_*`
    WHERE _TABLE_SUFFIX BETWEEN "20210528" AND "20210609"
    AND
        (
            jsonPayload.http_request_query__fw_station = "120123"
            OR jsonPayload.http_request_query__fw_station = "1238520002"
            OR jsonPayload.http_request_query__fw_station = "1238520004"
            OR jsonPayload.http_request_query__fw_station = "1238520005"
            OR jsonPayload.http_request_query__fw_station = "1238520006"
            OR jsonPayload.http_request_query__fw_station = "131921"
        )
    AND
        jsonPayload.http_request_query_eventType = "defaultImpression"
    AND
        (jsonPayload.http_request_query_playback_type = "live")
    AND
    SPLIT(jsonpayload.http_request_query_caid, "_")[OFFSET(0)] IN (
                "EP033149890042",
                "EP033149890043",
                "EP033149890044",
                "EP033149890045",
                "EP033149890046",
                "EP033149890047",
                "EP033149890048",
                "EP033149890049",
                "EP033149890050",
                "EP033149890051",
                "EP036241260116",
                "EP036241260117",
                "EP036241260118",
                "EP036241260119",
                "EP036241260122",
                "EP036241260123",
                "EP036241260124",
                "EP036241260125",
                "EP036241260126",
                "EP036241260127",
                "EP038580530001",
                "EP038605420001",
                "EP038605420002",
                "EP038605440001",
                "EP038605440002",
                "EP038753280002",
                "EP038758570001",
                "EP038758570003",
                "EP038758620001",
                "EP038758620003",
                "FUBO20210528202830",
                "FUBO20210528204447",
                "FUBO20210601190616",
                "FUBO20210601190749",
                "FUBO20210601192345",
                "FUBO20210601192444",
                "FUBO20210601192536",
                "FUBO20210601192633",
                "FUBO20210601192718",
                "FUBO20210601192803",
                "FUBO20210601192836",
                "FUBO20210601192918",
                "FUBO20210601192951",
                "FUBO20210601193949",
                "FUBO20210602141534",
                "FUBO20210603185156",
                "FUBO20210603185306",
                "FUBOCOM0000001"
    )
    GROUP BY caid, prof
    ORDER BY caid ASC
) imps
JOIN
    ad_requests ON ad_requests.caid = imps.caid AND ad_requests.prof = imps.prof
ORDER BY caid, prof ASC