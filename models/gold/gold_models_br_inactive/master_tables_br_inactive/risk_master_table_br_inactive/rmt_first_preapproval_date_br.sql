{{
    config(
        materialized='table',
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

  with first_preapproval_date as (
    select
      a.client_id,
      min(a.application_date) as min_preapproval_date
    from
      {{ ref('f_applications_br') }}  a
    where
      a.channel = 'PRE_APPROVAL'
      and a.custom_is_preapproval_completed
    group by
      1
  )
  select
    *
  from
    first_preapproval_date
