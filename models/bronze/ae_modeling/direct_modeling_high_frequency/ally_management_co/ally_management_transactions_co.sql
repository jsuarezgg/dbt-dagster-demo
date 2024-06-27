{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

--raw.ally_management_transactions_co
SELECT
    -- MANDATORY FIELDS
    id AS transaction_id,
    amount::DOUBLE AS transaction_amount,
    created_at::TIMESTAMP AS transaction_created_at,
    ally_slug,
    store_slug,
    loan_id,
    channel,
    stage,
    status,
    cancellation_id,
    cancellation_reason,
    cancellation_created_at::TIMESTAMP AS cancellation_created_at,
    data:cancellation.requestedAt::TIMESTAMP AS cancellation_requested_at,
    data:cancellation.source AS cancellation_source,
    data:cancellation.status AS cancellation_status,
    data:cancellation.type AS cancellation_type,
    data:cancellation.userEmail AS cancellation_user_email,
    data:cancellation.userId AS cancellation_user_id,
    cancellation_user_name AS cancellation_user_name,
    data AS data,
    journey,
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at
    -- CUSTOM ATTRIBUTES
    -- Fill with your custom attributes
-- DBT SOURCE REFERENCE
from {{ source('raw', 'ally_management_transactions_co') }}
