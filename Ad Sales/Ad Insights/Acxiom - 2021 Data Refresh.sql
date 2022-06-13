Select distinct t3.user_id as fubo_account_code,UPPER(to_hex(md5(lower(t2.email)))) as md5_hashed_email
from `fubotv-prod.data_insights.daily_status_static_update` t1 
inner join `fubotv-dev.business_analytics.email_mapping` t2 on t1.account_code=t2.account_code
inner join `fubotv-dev.business_analytics.account_code_mapping` t3 on t1.account_code=t3.account_code
where t1.day = '2021-09-30'
and final_status_restated IN  ('paid','paid but canceled','paid and scheduled pause')
and LOWER(plan_code) LIKE '%latino%' -- Latino

-- English Plans Only
and (plan_code in ('fubotv-basic','fubo-extra')
or plan_code like '%Bundle%' or plan_code like '%quarter%' or plan_code like '%qrtr%')
and lower(plan_code) not like '%latino%'
and lower(plan_code) not like '%spain%'
and lower(plan_code) not like '%canad%'