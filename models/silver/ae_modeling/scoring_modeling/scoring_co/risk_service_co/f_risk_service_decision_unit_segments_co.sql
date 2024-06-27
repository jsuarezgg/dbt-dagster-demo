{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

--bronze.risk_service_decision_unit_segments_co
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
from {{ ref('risk_service_decision_unit_segments_co') }}
