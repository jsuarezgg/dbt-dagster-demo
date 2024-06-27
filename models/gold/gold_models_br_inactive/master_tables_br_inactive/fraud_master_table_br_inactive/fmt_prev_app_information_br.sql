{{
    config(
        materialized='table',
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

with preapps as (
  select *,
  max(application_date) over(partition by prospect_id) max_preapp_date,
  max(preapproval_expiration_date) over(partition by prospect_id) max_preapproval_expiration_date
  from {{ ref('rmt_application_preapproval_br') }}
  where rn=1
),
prev_app_date as (
  select app.application_id,
  orig.loan_id,
  coalesce(origination_date,application_date) as date_time,
  lag(coalesce(origination_date,application_date)) over (partition by app.client_id order by coalesce(origination_date,application_date) asc) prev_app_date
  from {{ ref('f_pii_applications_br') }} app
  left join {{ ref('f_originations_bnpl_br') }} orig
      on app.application_id = orig.application_id
),
preapproval_application as (
  SELECT a.application_id,
  a.client_id,
  orig.loan_id,
  a.application_date, 
  a.requested_amount,
  orig.approved_amount,
  a.preapproval_amount,
  preapps.max_preapp_date preapp_date,
  case 
    when (a.application_date > preapps.application_date and a.requested_amount <= preapps.preapproval_amount) then 'ADDI Preapproval'
    else 'Not Preapproval'
    end as preapproval_application
  FROM {{ ref('f_pii_applications_br') }} a
  left join {{ ref('f_originations_bnpl_br') }} orig
      on a.application_id = orig.application_id
  left join preapps on a.client_id = preapps.prospect_id
  and a.application_date between preapps.application_date and preapps.preapproval_expiration_date 
),
final_preapps as (
  select 
  application_id,
  case when approved_amount >= preapproval_amount*0.9 and (unix_timestamp(application_date) - unix_timestamp(preapp_date))/60>=720 then 1 else 0 end preapp_hesitation, 
  (unix_timestamp(application_date) - unix_timestamp(preapp_date))/60 preapp_to_orig_minutes,
  requested_amount / preapproval_amount req_preapp_rel
  from preapproval_application
  where preapp_date is not null
  and preapproval_application in ('ADDI Preapproval')
)
select 
a.application_id,
a.loan_id, 
a.prev_app_date,
(unix_timestamp(a.date_time) - unix_timestamp(a.prev_app_date))/60 prev_app_minutes,
b.preapp_hesitation,
b.preapp_to_orig_minutes,
b.req_preapp_rel
from prev_app_date a
left join final_preapps b on a.application_id=b.application_id

-- validar si se puede mirar a fmt_changed_app_co estas mas varialbles preapp_hesitation, preapp_to_orig_minutes req_preapp_rel
