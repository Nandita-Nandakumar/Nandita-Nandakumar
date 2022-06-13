-- fox 4k

WITH view_data2 AS 
  (
  SELECT tms_id, user_id, REPLACE(channel, '–', '-') AS station_name, program_title, episode_title, start_time, DATE(start_time, "America/New_York") AS   day_streamed, duration, player_session_id, asset_id
  FROM `fubotv-prod.data_insights.video_in_progress_via_va_view` t
  INNER JOIN `fubotv-dev.business_analytics.station_mapping_table_1419` m ON m.station_name=t.channel
  WHERE LOWER(t.network_owner) LIKE '%fox%'
  AND LOWER(m.station_name) LIKE '%4%'
  AND EXTRACT(month FROM DATE(start_time, "America/New_York") ) = EXTRACT(month FROM DATE_SUB(CURRENT_DATE('America/New_York'), INTERVAL 10 day))
  AND EXTRACT(year FROM DATE(start_time, "America/New_York") ) = EXTRACT(year FROM DATE_SUB(CURRENT_DATE('America/New_York'), INTERVAL 10 day))
  GROUP BY 1, 2, 3, 4, 5, 6, 7, 8 , 9, 10
  ),
  sample AS 
  (
  SELECT tms_id, user_id, SUM(duration) AS ts
  FROM view_data2
  GROUP BY 1, 2
  ),
  view_data AS 
  (
  SELECT v.*
  FROM view_data2 v
  INNER JOIN sample s ON v.tms_id=s.tms_id AND v.user_id = s.user_id
  WHERE ts > 60 
  ),
  viewership AS 
  (
  SELECT 
    CASE WHEN properties_analytics_playback_type IS NOT NULL THEN LOWER(properties_analytics_playback_type)
    ELSE CASE WHEN properties_data_analytics_analytics_playback_type IS NOT NULL THEN LOWER(properties_data_analytics_analytics_playback_type)
    ELSE CASE WHEN properties_data_analytics_playback_type IS NOT NULL THEN LOWER(properties_data_analytics_playback_type)
    ELSE CASE WHEN properties_event_metadata_playback_type IS NOT NULL THEN LOWER(properties_event_metadata_playback_type)
    ELSE LOWER(properties_playback_type)
    END
    END
    END
    END AS playback_type,
    CASE WHEN properties_network_owner IS NOT NULL AND properties_network_owner != '' THEN properties_network_owner
    ELSE CASE WHEN properties_analytics_network_owner IS NOT NULL AND properties_analytics_network_owner != '' THEN properties_analytics_network_owner
    ELSE CASE WHEN properties_data_analytics_analytics_network_owner IS NOT NULL AND properties_data_analytics_analytics_network_owner != '' THEN properties_data_analytics_analytics_network_owner
    ELSE properties_data_analytics_network_owner
    END
    END
    END
    AS network_owner,
    CASE WHEN properties_channel IS NOT NULL AND properties_channel != '' THEN properties_channel
    ELSE CASE WHEN properties_data_analytics_station_name IS NOT NULL AND properties_data_analytics_station_name != '' THEN properties_data_analytics_station_name
    ELSE CASE WHEN properties_data_analytics_analytics_channel IS NOT NULL AND properties_data_analytics_analytics_channel != '' THEN properties_data_analytics_analytics_channel
    ELSE CASE WHEN properties_analytics_channel IS NOT NULL AND properties_analytics_channel != '' THEN properties_analytics_channel
    ELSE CASE WHEN properties_station_name IS NOT NULL AND properties_station_name != '' THEN properties_station_name
    ELSE properties_data_analytics_channel
    END
    END
    END
    END
    END
    AS station_name,
    CASE WHEN properties_tms_id IS NOT NULL AND properties_tms_id != '' THEN properties_tms_id
    ELSE CASE WHEN properties_analytics_tms_id IS NOT NULL AND properties_analytics_tms_id != '' THEN properties_analytics_tms_id
    ELSE CASE WHEN properties_data_analytics_analytics_tms_id IS NOT NULL AND properties_data_analytics_analytics_tms_id != '' THEN properties_data_analytics_analytics_tms_id
    ELSE CASE WHEN properties_event_metadata_tms_id IS NOT NULL AND properties_event_metadata_tms_id != '' THEN properties_event_metadata_tms_id
    ELSE properties_data_analytics_tms_id
    END
    END
    END
    END
    AS tms_id,
    CASE WHEN properties_data_analytics_series_title IS NOT NULL AND properties_data_analytics_series_title != '' THEN properties_data_analytics_series_title
    ELSE CASE WHEN properties_event_metadata_series_title IS NOT NULL AND properties_event_metadata_series_title != '' THEN properties_event_metadata_series_title
    ELSE CASE WHEN properties_data_analytics_analytics_series_title IS NOT NULL AND properties_data_analytics_analytics_series_title != '' THEN properties_data_analytics_analytics_series_title
    ELSE CASE WHEN properties_analytics_series_title IS NOT NULL AND properties_analytics_series_title != '' THEN properties_analytics_series_title
    ELSE properties_series_title
    END
    END
    END
    END
    AS series_title,
    CASE WHEN properties_data_analytics_analytics_episode_title IS NOT NULL AND properties_data_analytics_analytics_episode_title != '' THEN properties_data_analytics_analytics_episode_title
    ELSE CASE WHEN properties_event_metadata_series_title IS NOT NULL AND properties_event_metadata_series_title != '' THEN properties_event_metadata_series_title
    ELSE CASE WHEN properties_data_analytics_analytics_title IS NOT NULL AND properties_data_analytics_analytics_title != '' THEN properties_data_analytics_analytics_title
    ELSE CASE WHEN properties_data_analytics_episode_title IS NOT NULL AND properties_data_analytics_episode_title != '' THEN properties_data_analytics_episode_title
    ELSE CASE WHEN properties_analytics_title IS NOT NULL AND properties_analytics_title != '' THEN properties_analytics_title
    ELSE CASE WHEN properties_event_metadata_episode_title IS NOT NULL AND properties_event_metadata_episode_title != '' THEN properties_event_metadata_episode_title
    ELSE CASE WHEN properties_title IS NOT NULL AND properties_title != '' THEN properties_title
    ELSE CASE WHEN properties_analytics_series_title IS NOT NULL AND properties_analytics_series_title != '' THEN properties_analytics_series_title
    ELSE CASE WHEN properties_event_metadata_title IS NOT NULL AND properties_event_metadata_title != '' THEN properties_event_metadata_title
    ELSE CASE WHEN properties_data_analytics_title IS NOT NULL AND properties_data_analytics_title != '' THEN properties_data_analytics_title
    ELSE CASE WHEN properties_analytics_episode_title IS NOT NULL AND properties_analytics_episode_title != '' THEN properties_analytics_episode_title
    ELSE properties_episode_title
    END
    END
    END
    END
    END
    END
    END
    END
    END
    END
    END
    AS episode_title,
    CASE WHEN properties_data_analytics_genres IS NOT NULL AND properties_data_analytics_genres != '' THEN properties_data_analytics_genres
    ELSE CASE WHEN properties_data_analytics_analytics_genres IS NOT NULL AND properties_data_analytics_analytics_genres != '' THEN properties_data_analytics_analytics_genres
    ELSE CASE WHEN properties_analytics_genres IS NOT NULL AND properties_analytics_genres != '' THEN properties_analytics_genres
    ELSE properties_genres
    END
    END
    END
    AS genre_name,
    properties_device_category AS device_category, userid, receivedat, messageId
  FROM `fubotv-dev.upload_segment_realtime.viewership`
  WHERE event = 'video_in_progress'
  AND EXTRACT(month FROM DATE(receivedAt, "America/New_York") ) = EXTRACT(month FROM DATE_SUB(CURRENT_DATE('America/New_York'), INTERVAL 10 day))
  AND EXTRACT(year FROM DATE(receivedAt, "America/New_York") ) = EXTRACT(year FROM DATE_SUB(CURRENT_DATE('America/New_York'), INTERVAL 10 day))
  ),
  streams AS 
  (
  SELECT DISTINCT TIMESTAMP_TRUNC(receivedat, minute), tms_id, COUNT(DISTINCT userid) AS users
  FROM viewership
  WHERE LOWER(network_owner) LIKE '%fox%'
  AND LOWER(station_name) LIKE '%4%'
  GROUP BY 1, 2
  ),
  concurrent AS
  (
  SELECT d.tms_id, d.station_name, d.program_title, MAX(users) AS max_streams
  FROM view_data d
  INNER JOIN streams v USING (tms_id)
  GROUP BY 1, 2, 3
  )
  
SELECT d.station_name, d.program_title, d.episode_title, air_date, COUNT(DISTINCT user_id) AS uniques, ROUND(SUM(d.duration)/3600,2) AS hours, COUNT(player_session_id) AS video_start, MAX(max_streams) AS max_concurrent
FROM view_data d
INNER JOIN concurrent c USING (tms_id)
INNER JOIN `fubotv-prod.data_insights.programming_schedule` p USING (asset_id)
GROUP BY 1, 2, 3, 4
ORDER BY 4