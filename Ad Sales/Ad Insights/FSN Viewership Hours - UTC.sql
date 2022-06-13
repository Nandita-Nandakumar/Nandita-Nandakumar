# FSN Viewership Hours - IN UTC
WITH selected_tables AS 
(
    SELECT timestamp as date_stamp, client_ip as ip, 
    FROM `fubotv-prod.Sportsnet.Playout_202108*` -- for entire august -- use this to manipulate data
)

, data_info AS
(
    SELECT DISTINCT ip, date_stamp
    FROM selected_tables 
    ORDER BY 1,2
)

,previous_ts AS
(
    SELECT DISTINCT ip, date_stamp, LAG(date_stamp) OVER (PARTITION BY ip ORDER BY date_stamp) as previous_time_stamp
    FROM data_info
    ORDER BY 1,2
)

,diff_ts AS
(
    SELECT ip, date_stamp, previous_time_stamp, TIMESTAMP_DIFF(date_stamp, previous_time_stamp, SECOND) as diff_in_secs
    FROM previous_ts 
)

-- unacceptable flag to signal the start of a new "session"
,diff_flag AS
(
    SELECT * ,
    CASE WHEN (diff_in_secs >= 30 OR diff_in_secs IS NULL) THEN 1 ELSE 0 END AS session_start_flag
    FROM diff_ts 
    ORDER BY 1,2
)

-- creating a cumulative sum of the session_start_flag per ip
,session_id AS
(
    SELECT *,
        SUM(session_start_flag) OVER (PARTITION BY ip ORDER BY date_stamp ROWS BETWEEN
        UNBOUNDED PRECEDING AND CURRENT ROW) AS per_ip_session_id
    FROM diff_flag
)

, user_aggregates AS (
-- aggregate viewer details up by ip and per_ip_session_id 
SELECT 
    ip,
    per_ip_session_id,
    MIN(date_stamp) as session_start,
    MAX(date_stamp) AS session_end,
FROM session_id
GROUP BY ip, per_ip_session_id
ORDER BY 1,2
)

, user_session AS (
SELECT t1.*,
TIMESTAMP_DIFF( session_end , session_start, SECOND) AS viewing_secs 
FROM user_aggregates t1
)

SELECT SUM(viewing_secs)/3600 as viewing_hours
FROM user_session