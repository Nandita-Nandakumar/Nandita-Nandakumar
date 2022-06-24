---------------------------------------------------- News Plus Add ON OPENS & CLICKS --------------------------------------------------------
WITH email_banner_stats AS (
    SELECT DISTINCT 
    t1.email, 
    CASE WHEN t1.email= t2.email THEN "yes" ELSE "no" END AS both_open_click,
    t1.Recorded_ON as Open_Time, 
    t2.Recorded_On AS Click_Time, 
    FROM `fubotv-prod.business_analytics.NN_News_Plus_Email_Opens` t1
    LEFT JOIN `fubotv-dev.business_analytics.NN_News_Plus_Email_Clicks` t2 ON t1.email = t2.Email
    ORDER BY 2 DESC
)

, current_subs_who_opened_clicked AS (
    Select distinct t2.user_id, both_open_click, Open_Time, Click_Time
    from `fubotv-prod.data_insights.daily_status_static_update` t1
    inner join `fubotv-dev.business_analytics.account_code_mapping` t2 on t1.account_code=t2.account_code
    INNER JOIN `fubotv-dev.business_analytics.email_mapping` t4 ON t2.account_code = t4.account_code
    INNER JOIN email_banner_stats t3 ON t4.email = t3.email
    where final_status_v2_sql like ('paid%')
    and day = current_date()-2
    ORDER BY 2 DESC
)


, current_subs_without_add_on AS (
    Select distinct t2.user_id, both_open_click, Open_Time, Click_Time, 
    from `fubotv-prod.data_insights.daily_status_static_update` t1
    inner join `fubotv-dev.business_analytics.account_code_mapping` t2 on t1.account_code=t2.account_code
    INNER JOIN `fubotv-dev.business_analytics.email_mapping` t4 ON t2.account_code = t4.account_code
    INNER JOIN email_banner_stats t3 ON t4.email = t3.email
    where final_status_v2_sql like ('paid%')
    and day = '2022-02-11'
    AND LOWER(add_ons)  LIKE ('%news-plus%')
    ORDER BY 2 DESC
)

 --- exclude the 2600

,news_plus_take AS (
    SELECT user_id, both_open_click, Open_Time, Click_Time, take_date
    FROM 
    (
    SELECT user_id, both_open_click, Open_Time, Click_Time, day as take_date, ROW_NUMBER() OVER (partition by user_id order by day ASC) AS first_take_date, 
    FROM current_subs_without_add_on AS t1
    JOIN `fubotv-prod.data_insights.daily_status_static_update` t2 ON t1.user_id = t2.account_code
    WHERE day >= '2022-02-15' --- after 28th Feb
    AND LOWER(add_ons) LIKE ('%news-plus%')
    ORDER BY 1,5
    )
    WHERE first_take_date =1
)

, viewership1 AS (
    SELECT t1.user_id, both_open_click, Open_Time, Click_Time, start_time AS first_stream_time, ROW_NUMBER() OVER (partition by t1.user_id order by start_time ASC) AS first_stream
    FROM `fubotv-prod.data_insights.video_in_progress_via_va_view` t1
    JOIN current_subs_who_opened_clicked t2 ON t1.user_id = t2.user_id AND Open_time <= start_time
    JOIN `fubotv-dev.business_analytics.station_mapping_table_51220` t3 ON t1.channel= t3.station_name
    WHERE station_mapping IN ('Africanews', 'BBC World News', 'Bloomberg Television', 'Cheddar Stream', 'CNBC World', 'Euronews','i24NEWS', 'Law & Crime', 'NewsNet') --- All News Plus Channels
    ORDER BY 1,5
)

, viewership2 AS (
    SELECT t1.user_id, SUM(duration)/3600 AS Hours
    FROM `fubotv-prod.data_insights.video_in_progress_via_va_view` t1
    JOIN current_subs_who_opened_clicked t2 ON t1.user_id = t2.user_id AND Open_time <= start_time
    JOIN `fubotv-dev.business_analytics.station_mapping_table_51220` t3 ON t1.channel= t3.station_name
    WHERE station_mapping IN ('Africanews', 'BBC World News', 'Bloomberg Television', 'Cheddar Stream', 'CNBC World', 'Euronews','i24NEWS', 'Law & Crime', 'NewsNet') --- All News Plus Channels
    AND DATE(start_time) >= '2022-02-14'
    GROUP BY 1
    ORDER BY 1
)

, viewership AS (
    SELECT t1.user_id, both_open_click,  Open_Time, Click_Time, first_stream_time, Hours
    FROM viewership1 t1
    JOIN viewership2 t2 ON t1.user_id = t2.user_id
    WHERE first_stream = 1
    ORDER BY 1,5
    
)

SELECT t1.*,CASE WHEN t1.user_id = t2.user_id THEN "yes" ELSE "no" END AS got_the_add_on, take_date AS add_on_take_date
FROM viewership t1
JOIN news_plus_take t2 ON t1.user_id = t2.user_id
ORDER BY 6 DESC,7 DESC

---------------------------------------------------- News Plus EMAIL + CAROUSEL --------------------------------------------------------
WITH email_banner_stats AS (
    SELECT DISTINCT 
    t4.user_id, 
    CASE WHEN t1.email= t2.email THEN "yes" ELSE "no" END AS both_open_click,
    t1.Recorded_ON as Open_Time, 
    t2.Recorded_On AS Click_Time, 
    FROM `fubotv-prod.business_analytics.NN_News_Plus_Email_Opens` t1
    LEFT JOIN `fubotv-dev.business_analytics.NN_News_Plus_Email_Clicks` t2 ON t1.email = t2.Email
    INNER JOIN `fubotv-dev.business_analytics.email_mapping` t3 ON t3.email = t1.email
    inner join `fubotv-dev.business_analytics.account_code_mapping` t4 on t3.account_code=t4.account_code
    ORDER BY 2 DESC
)

, product_marketing_cohort AS (
    Select distinct t1.user_id
    from `fubotv-dev.business_analytics.NN_NewsPlus_Freeview_Carousel_Cohort` t1
    UNION ALL 
    SELECT DISTINCT t2.user_id
    FROM email_banner_stats t2
)

, distinct_cohort AS (
    SELECT DISTINCT user_id
    FROM product_marketing_cohort
)

, current_subs_without_add_on AS (
    Select distinct t2.user_id
    from `fubotv-prod.data_insights.daily_status_static_update` t1
    inner join `fubotv-dev.business_analytics.account_code_mapping` t2 on t1.account_code=t2.account_code
    INNER JOIN distinct_cohort t3 ON t3.user_id = t2.user_id
    where final_status_v2_sql like ('paid%')
    and day = '2022-02-11'
    AND LOWER(add_ons) NOT LIKE ('%news-plus%')
  
)

--, freeview_viewership AS (
    SELECT station_mapping, COUNT(DISTINCT t1.user_id) as uniques, SUM(duration)/3600 AS Hours
    FROM `fubotv-prod.data_insights.video_in_progress_via_va_view` t1
    JOIN current_subs_without_add_on  t2 ON t1.user_id = t2.user_id 
    JOIN `fubotv-dev.business_analytics.station_mapping_table_51220` t3 ON t1.channel= t3.station_name
    WHERE station_mapping IN ('Africanews', 'BBC World News', 'Bloomberg Television', 'Cheddar Stream', 'CNBC World', 'Euronews','i24NEWS', 'Law & Crime', 'NewsNet') --- All News Plus Channels
    AND DATE(start_time, "EST") >= '2022-02-14'
    AND DATE(start_time, "EST") <= '2022-02-28'
    GROUP BY 1
    ORDER BY 3 DESC
)

SELECT COUNT (DISTINCT user_id)
FROM freeview_viewership t1
JOIN `fubotv-prod.data_insights.daily_status_static_update` t2 ON t1.user_id = t2.account_code
where final_status_v2_sql like ('paid%')
and day = '2022-03-08'
AND LOWER(add_ons) LIKE ('%news-plus%')