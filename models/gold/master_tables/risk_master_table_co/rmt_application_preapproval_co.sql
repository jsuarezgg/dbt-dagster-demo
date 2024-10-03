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
    row_number() over(partition by client_id, from_utc_timestamp(application_date, 'America/Bogota')::date order by application_date) as rn
  from
    {{ source('silver_live', 'f_applications_co') }}
  where
    channel = 'PRE_APPROVAL'
    AND custom_is_preapproval_completed
