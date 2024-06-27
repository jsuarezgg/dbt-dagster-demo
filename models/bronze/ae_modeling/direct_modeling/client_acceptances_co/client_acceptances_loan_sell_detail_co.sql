{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

--raw.client_acceptances_loan_sell_detail_co
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
    cast(is_contract_moved as BOOLEAN) as is_contract_moved,
    cast(is_notification_sent as BOOLEAN) as is_notification_sent,
    cast(is_loan_sold_to_trust_generated as BOOLEAN) as is_loan_sold_to_trust_generated,
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at

-- DBT SOURCE REFERENCE
FROM {{ source('raw', 'client_acceptances_loan_sell_detail_co') }}
