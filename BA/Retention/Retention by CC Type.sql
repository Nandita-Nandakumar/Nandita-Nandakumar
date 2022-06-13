
-- M1 , M2 retention by CC Type for Adyden data

with account_map as (
Select t2.account_code,t1.Merchant_Reference,t1.Payment_Method_Variant,t1.Creation_Date
from `fubotv-prod.dmp.de_adyen_transactions` t1
inner join `fubotv-prod.dmp.recurly_transactions` t2 on t1.Merchant_Reference=t2.transaction_id
),

transaction_order_1 as (
Select distinct account_code,Payment_Method_Variant,row_number() over (partition by account_code order by Creation_Date desc) as transaction_order
from account_map
order by account_code, transaction_order desc
),

latest_transaction as (
Select * from transaction_order_1
where transaction_order = 1
),

buckets as (
Select distinct account_code,
case when lower(Payment_Method_Variant) like '%mc%' and lower(Payment_Method_Variant) like '%debit%' then 'mc_debit'
when lower(Payment_Method_Variant) like '%mc%' and lower(Payment_Method_Variant) like '%credit%' then 'mc_credit'
when lower(Payment_Method_Variant) like '%visa%' and lower(Payment_Method_Variant) like '%debit%' then 'visa_debit'
when lower(Payment_Method_Variant) like '%visa%' and lower(Payment_Method_Variant) like '%credit%' then 'visa_credit' 
-- added this on 11/10
when lower(Payment_Method_Variant) like '%pulse%' then 'other_debit'
when lower(Payment_Method_Variant) like '%discover%' then 'discover_credit'
when lower(Payment_Method_Variant) like '%diners%' then 'dinersclub_credit'
when lower(Payment_Method_Variant) like '%electron%' then 'other_debit'
when lower(Payment_Method_Variant) like '%star%' then 'other_debit'
when lower(Payment_Method_Variant) like '%nyce%' then 'other_debit' end as cc_bucket
-- updated until here
from latest_transaction
),

all_plans as (
SELECT cc_bucket,date_trunc(sub_first_dt,MONTH) as start_month, sub_nth_month_by_firstlast_plan_sql, count(distinct t1.account_code) as freq
FROM `fubotv-prod.data_insights.daily_status_static_update` t1
inner join buckets t2 on t1.account_code=t2.account_code
where final_status_restated like '%paid%'
AND sub_current_nth_day_sql >= 5
GROUP BY 1, 2,3
)

Select * from all_plans
