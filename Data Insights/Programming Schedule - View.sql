

WITH

-- CTE 1 - Getting program details for every tms_id when row_number for kg_modified stamp DESC, r=1
  program_details AS (
  SELECT
    tms_id,
    series_id ,
    league_ids ,
    episode_title,
    series_title,
    program_title,
    genres,
    league_names
  FROM (
-- For every tms_id, if the kd_timestamp is latest (DESC) then the row_number r is set to 1,2,3 
--until new tms_id.. every tms_id has row_num (r) = 1 if it only has 1 line item
    SELECT
      tms_id,
      series_id ,
      league_ids ,
      episode_title,
      series_title,
      program_title,
      genres,
      league_names,
      ROW_NUMBER() OVER (PARTITION BY tms_id ORDER BY kg_modified_timestamp DESC ) AS r
    FROM
      `fubotv-prod.dmp.content_program_details` )
  WHERE
    r = 1 ),

-- CTE 2 - Content asset details - row number partition over channel id and start_time when timestamp is desc
  recent_content_assests_data AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY channel_id, start_time ORDER BY kg_modified_timestamp DESC) AS r
  FROM
    `fubotv-prod.dmp.content_asset_details`
  WHERE
    air_date IS NOT NULL ),

-- CTE 3 - form the previoys CTE, r =1 
  latest_by_start_time AS (
  SELECT
    *
  FROM
    recent_content_assests_data
  WHERE
    1=1
    AND r= 1 ),

-- CTE 4 - from previous CTE but seet previous_end_time 
  schedule_with_previous_end_time AS (
  SELECT
    *,
    LAG(end_time ) OVER (PARTITION BY channel_id ORDER BY start_time ) AS previous_end_time,
    LAG(end_time, 2 ) OVER (PARTITION BY channel_id ORDER BY start_time ) AS previous_end_time2,
    LAG(end_time, 3 ) OVER (PARTITION BY channel_id ORDER BY start_time ) AS previous_end_time3,
    LAG(end_time, 4 ) OVER (PARTITION BY channel_id ORDER BY start_time ) AS previous_end_time4,
    LAG(end_time, 5 ) OVER (PARTITION BY channel_id ORDER BY start_time ) AS previous_end_time5,
    LAG(end_time, 6 ) OVER (PARTITION BY channel_id ORDER BY start_time ) AS previous_end_time6,
    LAG(end_time, 7 ) OVER (PARTITION BY channel_id ORDER BY start_time ) AS previous_end_time7
  FROM
    latest_by_start_time),

-- CTE 5 - 
  programming_schedule AS (
  SELECT
    * EXCEPT ( r,
      previous_end_time,
      previous_end_time2,
      previous_end_time3,
      previous_end_time4,
      previous_end_time5,
      previous_end_time6,
      previous_end_time7 )
  FROM
    schedule_with_previous_end_time
  WHERE
    start_time >= previous_end_time
    AND start_time >= previous_end_time2
    AND start_time >= previous_end_time3
    AND start_time >= previous_end_time4
    AND start_time >= previous_end_time5
    AND start_time >= previous_end_time6
    AND start_time >= previous_end_time7 )
  
SELECT
  p.* EXCEPT(tms_id),
  sc.*
FROM
  programming_schedule AS sc
JOIN
  program_details AS p
ON
  sc.tms_id = p.tms_id