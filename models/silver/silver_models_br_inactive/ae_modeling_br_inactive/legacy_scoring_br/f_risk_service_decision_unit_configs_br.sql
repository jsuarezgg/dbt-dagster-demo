{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

--bronze.risk_service_decision_unit_configs_br
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
from {{ ref('risk_service_decision_unit_configs_br') }}
