Select distinct t3.user_id as fubo_account_code,to_hex(md5(lower(t2.email))) as md5_hashed_email, plan_code
from `fubotv-prod.data_insights.daily_status_static_update` t1 
inner join `fubotv-dev.business_analytics.email_mapping` t2 on t1.account_code=t2.account_code
inner join `fubotv-dev.business_analytics.account_code_mapping` t3 on t1.account_code=t3.account_code
where t1.day = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
AND LOWER(final_status_v2_sql) IN ('paid', 'paid and scheduled pause','paid but canceled')
and (plan_code in ('fubotv-basic','fubo-extra')or plan_code like '%Bundle%')
and lower(plan_code) not like '%latino%'
and lower(plan_code_invoice) in ('fubotv-basic','fubo-extra')
