{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

--raw.risk_eligibility_lists_members_co
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
from {{ source('raw', 'risk_eligibility_lists_members_co') }}
