{{
    config(
        materialized='table',
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

with clean_application_emails as 
(
  select app.application_id, 
  app.client_id, 
  app.application_date, 
  orig.loan_id, 
  upper(split(application_email,'[@]')[0]) clean_email, 
  application_cellphone
  from {{ ref('f_pii_applications_br') }} app
  left join {{ ref('f_originations_bnpl_br') }} orig
      on app.application_id = orig.application_id
),
clean_email_prev_changed as (
  select 
  a.application_id,
  a.clean_email,
  max(case when a.clean_email != b.clean_email then 1 else 0 end) clean_email_prev_changed
  from clean_application_emails a
  left join clean_application_emails b on a.client_id = b.client_id and b.application_date < a.application_date
  group by 1,2
),
clean_email_prev_used as (
  select  
  a.application_id,
  a.clean_email,
  max(case when a.client_id != b.client_id then 1 else 0 end) clean_email_prev_used
  from clean_application_emails a
  left join clean_application_emails b on a.clean_email = b.clean_email and b.application_date < a.application_date
  group by 1,2
),
phone_prev_used as (
  select  
  a.application_id,
  a.clean_email, 
  a.application_cellphone, 
  a.application_date,
  max(case when a.client_id != b.client_id then 1 else 0 end) phone_prev_used
  from clean_application_emails a
  left join clean_application_emails b on a.application_cellphone = b.application_cellphone and b.application_date < a.application_date
  group by 1,2,3,4
)
select a.application_id,
a.clean_email, 
a.application_cellphone, 
a.application_date,
a.phone_prev_used,
b.clean_email_prev_changed,
c.clean_email_prev_used
from phone_prev_used a 
left join clean_email_prev_changed b on a.application_id = b.application_id
left join clean_email_prev_used c on a.application_id = c.application_id
