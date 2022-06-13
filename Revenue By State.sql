with
--run just refunds to get the refund info
refunds as (  
  Select * from `fubotv-prod.dmp.recurly_transactions` 
  where type = 'refund'
  and date >= '2018-01-01'
  and status = 'success'
  and record_action = 'created'
),

--join to adjustments on account_code and invoice_id and then pull plancode where adjustment_origin='plan' 
---or invoice summary and use originalinvoicenumber and link back to adjustments to see what plan that invoice was for
userstate as (
select max(day) as maxday, account_code, case when LOWER(plan_code) like '%bundle%' then 'fubotv-basic' else plan_code end as plan_code
from `fubotv-prod.data_insights.daily_status_static_update`
where 1=1
--and final_status_v2_sql in ('paid','paid but canceled')
and day>='2019-12-31'
group by 2,3),

userstate_partition as (
select maxday, account_code, plan_code, ROW_NUMBER() OVER (PARTITION BY account_code ORDER BY maxday DESC ) AS r
from userstate),

userstate_max_plan as (
select account_code, plan_code
from userstate_partition
where r=1),

refund_plan as (
select t1.*, plan_code
from refunds t1
INNER JOIN userstate_max_plan t2 on t1.account_code=t2.account_code --this should give same results as left join if data is correct, 11 users missing so just removing them
),
refund_plan_2 as (
Select t1.*,t2.state_abbrev
from refund_plan t1
left outer join `fubotv-dev.business_analytics.user_billing_state_3` t2 on t1.account_code=t2.account_code
),

--non PPP subs all time
remove_subs as (
select distinct account_code 
from `fubotv-prod.data_insights.daily_status_static_update`
where 1=1
and ((lower(coupon_code)  like '%csteam%' 
    or lower(coupon_code)  like '%aff2018%'
    or lower(coupon_code)  like '%voxmedia%'
    or lower(coupon_code)  like '%bengrad%'
    or lower(coupon_code)  like '%bd%'
    or lower(coupon_code)  like '%nctcpartner%'
    or lower(coupon_code)  like '%mincoupon%'
    or lower(coupon_code)  like '%1yearofffubo%'
    or lower(coupon_code)  like '%fubotvemployee%'
    or lower(coupon_code)  like '%nbcdemoaccounts%'
    or lower(coupon_code)  like '%testing100off%'
    or lower(coupon_code)  like '%fan100%'
    or lower(coupon_code)  like '%fubotvemployee%')
    )
  ---or lower(email)  like '%fubo.tv%'
  or (collection_method='manual' and plan_code='fubo-extra')
  or prepay_promo='Y'),
  
  
successful_payments_and_removals as (
select  
distinct account_code, 
date(closed_at) as day,
case when (adjustment_plan_code='None' or adjustment_plan_code is null or adjustment_plan_code='') then adjustment_product_code else adjustment_plan_code end as plan_code, 
adjustment_product_code, 
adjustment_amount, 
adjustment_origin,
invoice_subtotal,
collection_method,
id,
invoice_number
from `fubotv-prod.data_insights.closed_and_paid_invoices_view` 
where 1=1
  and closed_at >= '2020-01-01'
  and invoice_subtotal>0
  and adjustment_amount>0
  and adjustment_origin in ('plan','add_on')
),

successful_payments_1 as (
select t1.*
from successful_payments_and_removals t1
left join remove_subs t2 on t1.account_code=t2.account_code
where t2.account_code is null
),
/*
successful_payments_IAB as (
select 
distinct account_code, 
day,
plan_code, 
adjustment_product_code, 
adjustment_amount, 
adjustment_origin,
invoice_subtotal,
id,
invoice_number,
adjustment_amount as transaction_amount, 
0 as credit_redeemed
from successful_payments_1
where collection_method='manual'
),
*/
succesful_payments_web as (
select *
from successful_payments_1
where collection_method='automatic'
),

succesful_transactions_web_1 as (
select 
distinct t1.account_code, 
t1.day,
t1.plan_code, 
t1.adjustment_product_code, 
t1.adjustment_amount, 
t1.adjustment_origin,
t1.invoice_subtotal,
t1.id,
t1.invoice_number,
t2.amount-t2.tax_amount as transaction_amount,
t2.tax_amount
from succesful_payments_web t1
inner join `fubotv-prod.dmp.recurly_transactions` t2 on t1.account_code=t2.account_code and t1.id=t2.invoice_id
where t2.status='success'
and t2.type='purchase'
and record_action='created'
),

web_voids as (
select 
distinct t1.account_code, 
invoice_id
from succesful_transactions_web_1 t1
inner join `fubotv-prod.dmp.recurly_transactions` t2 on t1.account_code=t2.account_code and t1.id=t2.invoice_id
where t2.status='void'
and t2.type='purchase'
--and record_action='created'
),

succesful_transactions_web_2 as (
select t1.*
from succesful_transactions_web_1 t1
left join web_voids t2 on t1.account_code=t2.account_code and t1.id=t2.invoice_id
where t2.invoice_id is null
),

succesful_transactions_web as (
select t1.*, invoice_subtotal-transaction_amount as credit_redeemed
from succesful_transactions_web_2 t1
),

successful_payments as (
---select * from successful_payments_IAB
---UNION ALL
select * from succesful_transactions_web
),

----PPP subs


PPP_subs as (
select distinct account_code 
from `fubotv-prod.data_insights.daily_status_static_update`
where 1=1
and prepay_promo='Y'),
  

successful_payments_and_removals_PPP as (
select  
distinct account_code, 
date(closed_at) as day,
case when (adjustment_plan_code='None' or adjustment_plan_code is null or adjustment_plan_code='') then adjustment_product_code else adjustment_plan_code end as plan_code, 
adjustment_product_code, 
adjustment_amount, 
adjustment_origin,
invoice_subtotal,
collection_method,
id,
invoice_number,
coupon_code
from `fubotv-prod.data_insights.closed_and_paid_invoices_view` 
where 1=1
  and closed_at >= '2018-01-01'
  and invoice_subtotal>0
  and adjustment_amount>0
  and adjustment_origin in ('plan','add_on')
),


successful_payments_1_PPP as (
select t1.*
from successful_payments_and_removals_PPP t1
join PPP_subs t2 on t1.account_code=t2.account_code
),
/*
successful_payments_IAB_PPP as (
select 
distinct account_code, 
day,
plan_code, 
adjustment_product_code, 
adjustment_amount, 
adjustment_origin,
invoice_subtotal,
id,
invoice_number,
coupon_code,
adjustment_amount as transaction_amount, 
0 as credit_redeemed
from successful_payments_1_PPP
where collection_method='manual'
),
*/
succesful_payments_web_PPP as (  
select *
from successful_payments_1_PPP
where collection_method='automatic'
),

succesful_transactions_web_1_PPP as (------THIS IS WHERE IS STOPPED
select 
distinct t1.account_code, 
t1.day,
t1.plan_code, 
t1.adjustment_product_code, 
t1.adjustment_amount, 
t1.adjustment_origin,
t1.invoice_subtotal,
t1.id,
t1.invoice_number,
t1.coupon_code,
t2.amount-t2.tax_amount as transaction_amount
from succesful_payments_web_PPP t1
inner join `fubotv-prod.dmp.recurly_transactions` t2 on t1.account_code=t2.account_code and t1.id=t2.invoice_id
where t2.status='success'
and t2.type='purchase'
and record_action='created'
),

web_voids_PPP as (
select 
distinct t1.account_code, 
invoice_id
from succesful_transactions_web_1_PPP t1
inner join `fubotv-prod.dmp.recurly_transactions` t2 on t1.account_code=t2.account_code and t1.id=t2.invoice_id
where t2.status='void'
and t2.type='purchase'
--and record_action='created'
),

succesful_transactions_web_2_PPP as (
select t1.*
from succesful_transactions_web_1_PPP t1
left join web_voids_PPP t2 on t1.account_code=t2.account_code and t1.id=t2.invoice_id
where t2.invoice_id is null
),

succesful_transactions_web_PPP as (
select t1.*, invoice_subtotal-transaction_amount as credit_redeemed
from succesful_transactions_web_2_PPP t1
),

successful_payments_PPP as (
---select * from successful_payments_IAB_PPP
---UNION ALL
select * from succesful_transactions_web_PPP
),

successful_payments_PPP_reg as (
select * 
from successful_payments_PPP
where lower(coupon_code) not like '%prepay-promo%' or coupon_code is null),

successful_payments_PPP_promo as (
select * 
from successful_payments_PPP
where lower(coupon_code) like '%prepay-promo%'),

charge_breakout_PPP as (
select *, 
DATE_ADD(day, INTERVAL 30 DAY) as second_payment, 
transaction_amount/2 as half_transaction  
from successful_payments_PPP_promo
where adjustment_origin='plan'),

first_charge_PPP as (
select day, 
account_code,
plan_code,
adjustment_product_code,
adjustment_origin,
half_transaction
from charge_breakout_PPP),

second_charge_PPP as (
select 
second_payment as day, 
account_code,
plan_code,
adjustment_product_code,
adjustment_origin,
half_transaction
from charge_breakout_PPP),


PPP_breakout as (
select t1.*,t2.state_abbrev from first_charge_PPP t1
left outer join `fubotv-dev.business_analytics.user_billing_state_3` t2 on t1.account_code=t2.account_code
UNION ALL
select t1.*,t2.state_abbrev from second_charge_PPP t1
left outer join `fubotv-dev.business_analytics.user_billing_state_3` t2 on t1.account_code=t2.account_code
),

non_PPP_success as (
select day,
plan_code, 
adjustment_product_code, 
adjustment_origin,
invoice_subtotal,
adjustment_amount, 
credit_redeemed,
t1.account_code,
t2.state_abbrev
from successful_payments t1
left outer join `fubotv-dev.business_analytics.user_billing_state_3` t2 on t1.account_code=t2.account_code
UNION ALL
select  day,
plan_code, 
adjustment_product_code, 
adjustment_origin,
invoice_subtotal,
adjustment_amount, 
credit_redeemed,
t1.account_code,
t2.state_abbrev
from successful_payments_PPP_reg t1
left outer join `fubotv-dev.business_analytics.user_billing_state_3` t2 on t1.account_code=t2.account_code
),

---summarized from here on

paid_summary as (
select 
day,
plan_code, 
adjustment_product_code, 
adjustment_origin,case when state_abbrev is NULL then 'Unknown' when state_abbrev = '' then 'Unknown' else state_abbrev end as state_abbrev,
--sum(invoice_subtotal) as invoice_total,
sum(adjustment_amount) as Payments, 
sum(credit_redeemed) as credit_redeemed,
count(distinct account_code) as paidsubs --- then only sum up account codes when the adjustment_origin is a plan
from non_PPP_success
group by 1,2,3,4,5),

refund_summary as (
select 
date(modified_at) as day,
plan_code,case when state_abbrev is NULL then 'Unknown' when state_abbrev = '' then 'Unknown' else state_abbrev end as state_abbrev,
count(distinct account_code) as refundsubs, 
(sum(amount)*-1) as Refunds,
'Eligible' as Type
from refund_plan_2 
group by 1,2,3),

agg_reg as (
select t1.*,
case when adjustment_origin='add_on' then Payments
when adjustment_origin='plan' then Payments-credit_redeemed end as TOTAL_PAID,
'Eligible' as Type
from paid_summary t1),

agg_ppp as (
select 
day, 
plan_code,
adjustment_product_code,
adjustment_origin,case when state_abbrev is NULL then 'Unknown' when state_abbrev = '' then 'Unknown' else state_abbrev end as state_abbrev,
SUM(half_transaction) as Payments,
0 as credit_redeemed,
count(distinct account_code) as paidsubs,
SUM(half_transaction) as TOTAL_PAID,
'Not Eligible' as Type
from PPP_breakout
group by 1,2,3,4,5), 

agg_total as (
select * from agg_reg
UNION ALL 
select * from agg_ppp),

final_pull as (
select t1.* except (Type),refundsubs, Refunds
from agg_total t1
left join refund_summary t2 on (t1.day=t2.day and t1.adjustment_product_code=t2.plan_code and t1.plan_code=t2.plan_code and t1.Type=t2.Type and t1.state_abbrev=t2.state_abbrev))

select state_abbrev,sum(payments) as payments_2,sum(credit_redeemed) as credit_redeemed_2,sum(refunds) as refunds_2
from final_pull where day >= '2020-01-01'
and day <= '2020-12-31'
and plan_code not in ('canadian_basic_month','fubotv-spain','fubotv-spain-year','fubotv-spain-quarter','canadian_basic_year')
group by 1
order by 1 asc