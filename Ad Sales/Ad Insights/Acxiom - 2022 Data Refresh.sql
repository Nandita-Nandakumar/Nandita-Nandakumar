--- ACXIOM DATA REFERSH PULL 
-- 1. English only
-- 2. ALL = English + Latino
Select distinct t3.user_id as fubo_account_code,UPPER(to_hex(md5(lower(t2.email)))) as md5_hashed_email
from `fubotv-prod.data_insights.daily_status_static_update` t1 
inner join `fubotv-dev.business_analytics.email_mapping` t2 on t1.account_code=t2.account_code
inner join `fubotv-dev.business_analytics.account_code_mapping` t3 on t1.account_code=t3.account_code
where t1.day = current_date()-1 ---25th feb 2022
and final_status_restated IN  ('paid','paid but canceled','paid and scheduled pause')
and (plan_code in ('fubotv-basic','fubo-extra') -- remove to include latino
or plan_code like '%Bundle%' or plan_code like '%quarter%' or plan_code like '%qrtr%') -- remove to include latino
and lower(plan_code) not like '%latino%' -- remove to include latino
and lower(plan_code) not like '%spain%'
and lower(plan_code) not like '%cana%'


---------------------- FINDING MATCH BETWEEN LAST RUN AND THIS ------------------- SUBS MATCH
WITH june_eng_subs AS (
    Select distinct t3.user_id as fubo_account_code,UPPER(to_hex(md5(lower(t2.email)))) as md5_hashed_email
from `fubotv-prod.data_insights.daily_status_static_update` t1 
inner join `fubotv-dev.business_analytics.email_mapping` t2 on t1.account_code=t2.account_code
inner join `fubotv-dev.business_analytics.account_code_mapping` t3 on t1.account_code=t3.account_code
where t1.day = '2021-06-15'
and final_status_restated IN  ('paid','paid but canceled','paid and scheduled pause')
-- English Plans Only
and (plan_code in ('fubotv-basic','fubo-extra')
or plan_code like '%Bundle%' or plan_code like '%quarter%' or plan_code like '%qrtr%')
and lower(plan_code) not like '%latino%'
and lower(plan_code) not like '%spain%'
and lower(plan_code) not like '%canad%'
)

, eng_subs_22 AS (
    Select distinct t3.user_id as fubo_account_code,UPPER(to_hex(md5(lower(t2.email)))) as md5_hashed_email
from `fubotv-prod.data_insights.daily_status_static_update` t1 
inner join `fubotv-dev.business_analytics.email_mapping` t2 on t1.account_code=t2.account_code
inner join `fubotv-dev.business_analytics.account_code_mapping` t3 on t1.account_code=t3.account_code
where t1.day = '2022-02-25'
and final_status_restated IN  ('paid','paid but canceled','paid and scheduled pause')
-- English Plans Only
and (plan_code in ('fubotv-basic','fubo-extra')
or plan_code like '%Bundle%' or plan_code like '%quarter%' or plan_code like '%qrtr%')
and lower(plan_code) not like '%latino%'
and lower(plan_code) not like '%spain%'
and lower(plan_code) not like '%can%'
)

SELECT CASE WHEN t1.fubo_account_code = t2.fubo_account_code THEN "match" ELSE "no" END AS group_i, COUNT(DISTINCT t1.fubo_account_code)
FROM eng_subs_22 t1 
LEFT JOIN june_eng_subs t2 USING (fubo_account_code)
GROUP BY 1

