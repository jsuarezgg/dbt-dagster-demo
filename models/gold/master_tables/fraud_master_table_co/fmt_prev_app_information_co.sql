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
  max(application_date) over(partition by prospect_id) as max_preapp_date,
  max(preapproval_expiration_date) over(partition by prospect_id) as max_preapproval_expiration_date
  from {{ ref('rmt_application_preapproval_co') }}
  where rn = 1
),
prev_app_date as (
  select apps.application_id,
  loan_id,
  coalesce(origination_date,application_date) as date_time,
  lag(coalesce(origination_date,application_date)) over (partition by apps.client_id order by coalesce(origination_date,application_date) asc) prev_app_date
  from {{ ref('f_pii_applications_co') }} apps
  left join {{ ref('f_originations_bnpl_co')}} orig
  on apps.application_id = orig.application_id

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
    when ( a.application_date > preapps.application_date and a.requested_amount <= preapps.preapproval_amount and preapps.ally_preapproval like ('%addi%') ) then 'ADDI Preapproval'
    when ( a.application_date > preapps.application_date and a.requested_amount <= preapps.preapproval_amount and preapps.ally_preapproval is not null ) then 'Widget' 
    when ( a.application_date > preapps.application_date and a.requested_amount <= preapps.preapproval_amount ) then 'ADDI Preapproval'
    else 'Not Preapproval'
    end as preapproval_application
  from {{ ref('f_pii_applications_co') }} a
  left join {{ ref('f_originations_bnpl_co')}} orig
  on a.application_id = orig.application_id
  left join preapps 
    on a.client_id = preapps.prospect_id
    and a.application_date between preapps.application_date and preapps.preapproval_expiration_date
    where a.channel not in ('PRE_APPROVAL')
),
final_preapps as (
  select 
  application_id,
  case when requested_amount >= preapproval_amount*0.9 and (unix_timestamp(application_date) - unix_timestamp(preapp_date))/60>=720 then 1 else 0 end preapp_hesitation, 
  datediff(MINUTE,preapp_date, application_date) as preapp_to_orig_minutes,
  requested_amount / preapproval_amount req_preapp_rel
  from preapproval_application
  where preapp_date is not null
  and preapproval_application in ('ADDI Preapproval', 'Widget')
)
select 
a.application_id,
a.loan_id, 
a.prev_app_date,
datediff(MINUTE,prev_app_date, date_time) as prev_app_minutes,
b.preapp_hesitation,
b.preapp_to_orig_minutes,
b.req_preapp_rel
from prev_app_date a
left join final_preapps b 
  on a.application_id = b.application_id