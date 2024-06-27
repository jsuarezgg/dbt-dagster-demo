{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

--bronze.ally_management_transactions_co
SELECT
    -- MANDATORY FIELDS
    transaction_id,
    transaction_amount,
    transaction_created_at,
    ally_slug,
    store_slug,
    loan_id,
    channel,
    stage,
    status,
    cancellation_id,
    cancellation_reason,
    cancellation_created_at,
    cancellation_requested_at,
    cancellation_source,
    cancellation_status,
    cancellation_type,
    cancellation_user_email,
    cancellation_user_id,
    cancellation_user_name,
    data,
    journey,
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at
    -- CUSTOM ATTRIBUTES
    -- Fill with your custom attributes
-- DBT SOURCE REFERENCE
from {{ ref('ally_management_transactions_co') }}
