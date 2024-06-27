{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}


--bronze.syc_clients_addicupo_history_co
SELECT
    -- MAPPED FIELDS - DIRECT ATTRIBUTES
    id,
    client_id,
    total_addicupo,
    update_addicupo_reason,
    addicupo_state,
    addicupo_last_update,
    update_addicupo_source,
    static_addicupo,
    remaining_addicupo,
    addicupo_state_v2,
    addicupo_state_v2_reason,
    ingested_at,
    updated_at

-- DBT SOURCE REFERENCE
FROM {{ ref('syc_clients_addicupo_history_co') }}
