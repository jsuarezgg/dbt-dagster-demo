{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

--bronze.snc_positive_balance_report_co
SELECT
    -- DIRECT MODELING FIELDS
    calculation_date,
    client_id,
    positive_balance,
    balance_before_payment,
    update_date,
    payments,
    directed_payment,
    condonations,
    -- MANDATORY FIELDS
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at
-- DBT SOURCE REFERENCE
from {{ ref('snc_positive_balance_report_co') }}
