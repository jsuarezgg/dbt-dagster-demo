{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

-- raw.ally_management_ally_payments_co

SELECT
    id,
    ally_group_id,
    ally_slug,
    type,
    total,
    anticipation_fee,
    cast(anticipated as BOOLEAN) as anticipated,
    status,
    occurred_on,
    start_date,
    end_date,
    scheduled_payment_date,
    payment_date,
    data,
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at
FROM {{ source('raw', 'ally_management_ally_payments_co') }}
