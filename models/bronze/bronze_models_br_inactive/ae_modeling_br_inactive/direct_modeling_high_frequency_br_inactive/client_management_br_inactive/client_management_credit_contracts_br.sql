{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

--raw_modeling.client_management_credit_contracts_br
SELECT
    -- MANDATORY FIELDS
    client_id,
    loan_id,
    storage_location,
    loan_origination_date,
    loan_ownership,
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at
    -- CUSTOM ATTRIBUTES
      -- Fill with your custom attributes
-- DBT SOURCE REFERENCE
from {{ source('raw', 'client_management_credit_contracts_br') }}
