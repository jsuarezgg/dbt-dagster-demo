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
    -- MANDATORY FIELDS
    id,
    status,
    loan_id,
    client_id,
    ownership,
    sale_price,
    external_key,
    days_past_due,
    loan_metadata,
    loan_validation,
    sell_process_id,
    unpaid_principal,
    is_contract_moved,
    is_notification_sent,
    is_loan_sold_to_trust_generated,
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at

-- DBT SOURCE REFERENCE
FROM {{ ref('client_acceptances_loan_sell_detail_co') }}
