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
	client_id,
    eligibility_list_id,
	valid_from,
    valid_to,
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at
    -- CUSTOM ATTRIBUTES
      -- Fill with your custom attributes
-- DBT SOURCE REFERENCE
FROM {{ ref('risk_eligibility_lists_members_co') }} 
