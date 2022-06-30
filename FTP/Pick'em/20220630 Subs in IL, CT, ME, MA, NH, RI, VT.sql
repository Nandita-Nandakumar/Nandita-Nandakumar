select distinct d.account_code, state
from `fubotv-prod.data_insights.daily_status_static_update` d
inner join `fubotv-dev.business_analytics.email_mapping` e using(account_code)
inner join `fubotv-dev.business_analytics.ref_current_paid_users_state_zip` f using (account_code)
where d.day = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) -- June 29th
and d.final_status_v2_sql not in ('expired','paused')
and (d.plan_code in ('fubotv-basic','fubo-extra','family-quarter')
or d.plan_code like '%Bundle%' or lower(d.plan_code) like '%quarter%' or lower(d.plan_code) like '%qrtr%')
and lower(d.plan_code) not like '%latino%' and lower(d.plan_code) not like '%spain%' and lower(d.plan_code) not like '%can%'
and lower(d.plan_code_invoice) in ('fubotv-basic','fubo-extra','fubotv-basic-qrtr')
and e.email not like '%@apple.com'
and e.email not like '%@roku.com'
and e.email is not null
and e.email <> ""
and state IN ('IL','CT', 'ME', 'MA', 'NH', 'RI', 'VT') -- Group 1