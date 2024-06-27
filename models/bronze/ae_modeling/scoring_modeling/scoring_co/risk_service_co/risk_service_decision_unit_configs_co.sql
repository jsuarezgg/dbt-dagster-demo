{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

--raw.risk_service_decision_unit_configs_co
SELECT
    -- DIRECT MODELING FIELDS
	id,
    name,
    valid_from,
    valid_to,
    module,
    decision_unit,
    configuration,
    flow,
    -- MANDATORY FIELDS
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at
-- DBT SOURCE REFERENCE
from {{ source('raw', 'risk_service_decision_unit_configs_co') }}
