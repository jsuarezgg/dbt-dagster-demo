{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

--raw.risk_service_decision_unit_logs_co
SELECT
    -- DIRECT MODELING FIELDS
	id as decision_id,
	execution_id,
	context_id,
	created_at,
	payload,
	decision_unit,
	flow,
    -- MANDATORY FIELDS
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at
-- DBT SOURCE REFERENCE
from {{ source('raw', 'risk_service_decision_unit_logs_co') }}
