{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

--raw.syc_clients_co
SELECT
    -- DIRECT MODELING FIELDS
    id AS client_id,
    total_addicupo,
    remaining_addicupo,
    addicupo_state,
    addicupo_last_update,
    cast(static_addicupo as BOOLEAN) as static_addicupo,
    last_update_addicupo_reason,
    last_update_addicupo_source,
    addicupo_state_v2,
    addicupo_state_v2_reason,
    ignore_progression,
    initial_addicupo,
    cast(is_transactional_based as BOOLEAN) as is_transactional_based,
    preferences,
    -- MANDATORY FIELDS
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at
-- DBT SOURCE REFERENCE
from {{ source('raw', 'syc_clients_co') }}