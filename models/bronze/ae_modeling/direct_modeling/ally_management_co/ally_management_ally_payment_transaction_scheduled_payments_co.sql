{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

-- raw.ally_management_ally_payment_transaction_scheduled_payments_co

SELECT
    id,
    type,
    status,
    transaction_id,
    payment_id,
    ally_slug,
    application_id,
    product,
    subproduct,
    loan_id,
    order_id,
    total,
    mdf,
    fee,
    lead_gen_mdf,
    lead_gen_fee,
    occurred_on,
    scheduled_payment_date,
    data,
    cancellation_id,
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at
FROM {{ source('raw', 'ally_management_ally_payment_transaction_scheduled_payments_co') }}
