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
    client_id,
    loan_id,
    storage_location,
    loan_origination_date,
    loan_ownership,
    ingested_at,
    updated_at
    -- CUSTOM ATTRIBUTES
      -- Fill with your custom attributes
-- DBT SOURCE REFERENCE
FROM {{ ref('client_management_credit_contracts_co') }} AS credit_contracts
