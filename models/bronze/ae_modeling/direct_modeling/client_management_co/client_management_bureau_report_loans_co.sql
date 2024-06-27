{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

--raw.client_management_bureau_report_loans_co
SELECT
    -- MANDATORY FIELDS
    loan_id,
    client_id,
    days_past_due,
    bureau_report_id,
    delinquency_balance,
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at
-- DBT SOURCE REFERENCE
FROM {{ source('raw', 'client_management_bureau_report_loans_co') }}
