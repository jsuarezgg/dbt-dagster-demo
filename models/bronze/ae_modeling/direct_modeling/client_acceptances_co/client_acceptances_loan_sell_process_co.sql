{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

--raw.client_acceptances_loan_sell_process_co
SELECT
    -- MANDATORY FIELDS
    stage,
    create_at,
    sell_process_id,
    external_message_id,
    total_loans_processed,
    total_loans_to_process,
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at
    -- CUSTOM ATTRIBUTES
      -- Fill with your custom attributes
-- DBT SOURCE REFERENCE
from {{ source('raw', 'client_acceptances_loan_sell_process_co') }}
