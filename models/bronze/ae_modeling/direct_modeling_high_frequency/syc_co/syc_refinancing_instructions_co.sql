{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

--raw.syc_refinancing_instructions_co
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
    cast(annulled as BOOLEAN) as annulled,
    annulment_reason,
    -- MANDATORY FIELDS
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at
-- DBT SOURCE REFERENCE
from {{ source('raw', 'syc_refinancing_instructions_co') }}