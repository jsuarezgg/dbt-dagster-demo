{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

--raw.risk_service_decision_unit_segments_co
SELECT
    -- DIRECT MODELING FIELDS
	id,
    valid_from,
    valid_to,
    type,
    value,
    flow,
    module,
    decision_unit,
    configs,
    -- MANDATORY FIELDS
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at
-- DBT SOURCE REFERENCE
from {{ source('raw', 'risk_service_decision_unit_segments_co') }}
