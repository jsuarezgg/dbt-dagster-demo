

--bronze.models_service_model_configs_br
SELECT
    -- DIRECT MODELING FIELDS
	id,
    type,
    flow,
    name,
    internal_name,
    created_at,
    is_active,
    is_shadow,
    -- MANDATORY FIELDS
    NOW() AS ingested_at,
    to_timestamp('2022-01-01') AS updated_at
-- DBT SOURCE REFERENCE
from bronze.models_service_model_configs_br