{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}


SELECT
    -- MAPPED FIELDS - DIRECT ATTRIBUTES
    calculation_date,
    client_id,
    positive_balance,
    balance_before_payment,
    update_date,
    payments,
    directed_payment,
    condonations,
    -- CUSTOM ATTRIBUTES
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at

-- DBT SOURCE REFERENCE
FROM {{ source('raw', 'snc_positive_balance_report_co') }}
