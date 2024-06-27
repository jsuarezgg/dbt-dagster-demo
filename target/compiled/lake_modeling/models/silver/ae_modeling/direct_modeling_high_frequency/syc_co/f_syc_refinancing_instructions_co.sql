

--bronze.syc_refinancing_instructions_co
SELECT
    -- DIRECT MODELING FIELDS
    id,
    client_id,
    loan_id,
    type,
    version,
    data,
    start_date,
    end_date,
    created_at,
    updated_at as updated_at_source,
    annulled,
    annulment_reason,
    -- MANDATORY FIELDS
    NOW() AS ingested_at,
    to_timestamp('2022-01-01') AS updated_at
-- DBT SOURCE REFERENCE
from bronze.syc_refinancing_instructions_co