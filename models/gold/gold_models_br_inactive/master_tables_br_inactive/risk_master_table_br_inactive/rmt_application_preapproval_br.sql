{{
    config(
        materialized='table',
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

  select
    client_id as prospect_id,
    application_id,
    ally_slug as ally_preapproval,
    application_date,
    preapproval_expiration_date,
    preapproval_amount,
    row_number() over(partition by client_id, from_utc_timestamp(application_date, 'America/Sao_Paulo')::date order by application_date) as rn
  from
    {{ ref('f_applications_br') }}
  where
    channel = 'PRE_APPROVAL'
    AND custom_is_preapproval_completed
