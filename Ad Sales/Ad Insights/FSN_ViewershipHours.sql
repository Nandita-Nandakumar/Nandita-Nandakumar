WITH selected_tables AS 
(
    SELECT timestamp as date_stamp, client_ip as ip, 
    FROM `fubotv-prod.Sportsnet.Playout_2021*`
    WHERE timestamp <= '2021-08-31'
    ORDER BY 1
)

, info AS
(
    SELECT DISTINCT ip, date_stamp
    FROM selected_tables 
    ORDER BY 1,2
)

,lag_fun AS
(
    SELECT DISTINCT ip, date_stamp, LAG(date_stamp) OVER (PARTITION BY ip ORDER BY date_stamp) as previous_time_stamp
    FROM info
    ORDER BY 1,2
)

/*,diff_fun AS
(
    SELECT ip, date_stamp, previous_time_stamp, EXTRACT( SECOND FROM TIMESTAMP(date_stamp)) - EXTRACT( SECOND FROM TIMESTAMP( previous_time_stamp)) as diff_in_secs
    FROM lag_fun 
)
*/

, diff_fun AS
(
    SELECT ip, date_stamp, previous_time_stamp, date_stamp - previous_time_stamp as diff_in_secs
    FROM lag_fun 
    ORDER BY 1,2
)

,diff_secs_extract AS
(
    SELECT ip, date_stamp, previous_time_stamp, EXTRACT(SECOND FROM (diff_in_secs)) as diff_in_secs
    FROM diff_fun 
    ORDER BY 1,2
)

,diff_set_flag AS
(
    SELECT * ,
    CASE WHEN diff_in_secs >= 30 THEN "0" ELSE "1" END AS acceptable_flag
    FROM diff_secs_extract 
    ORDER BY 1,2
)

SELECT DATE_TRUNC(date_stamp, Month) as d_month, SUM(diff_in_secs)/3600 as total_hours_viewed
FROM diff_set_flag 
WHERE acceptable_flag = "1"
GROUP BY 1


------------------- with user sessions and IP

WITH selected_tables AS 
(
    SELECT timestamp as date_stamp, client_ip as ip, 
    FROM `fubotv-prod.Sportsnet.Playout_20210906`
    ORDER BY 1
)

, info AS
(
    SELECT DISTINCT ip, date_stamp
    FROM selected_tables 
    ORDER BY 1,2
)

,lag_fun AS
(
    SELECT DISTINCT ip, date_stamp, LAG(date_stamp) OVER (PARTITION BY ip ORDER BY date_stamp) as previous_time_stamp
    FROM info
    ORDER BY 1,2
)

,diff_fun AS
(
    SELECT ip, date_stamp, previous_time_stamp, TIMESTAMP_DIFF(date_stamp, previous_time_stamp, SECOND) as diff_in_secs
    FROM lag_fun 
)

-- unacceptable flag to signal the start of a new "session"
,diff_set_flag AS
(
    SELECT * ,
    CASE WHEN (diff_in_secs >= 30 OR diff_in_secs IS NULL) THEN 1 ELSE 0 END AS session_start_flag
    FROM diff_fun 
    ORDER BY 1,2
)

-- creating a cumulative sum of the session_start_flag per ip
,session_id AS
(
    SELECT *,
        SUM(session_start_flag) OVER (PARTITION BY ip ORDER BY date_stamp ROWS BETWEEN
        UNBOUNDED PRECEDING AND CURRENT ROW) AS per_ip_session_id
    FROM diff_set_flag
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