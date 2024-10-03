{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

--bronze.syc_clients_co
SELECT
    -- DIRECT MODELING FIELDS
    sc.client_id as client_id,
    total_addicupo,
    remaining_addicupo,
    addicupo_state,
    addicupo_last_update,
    static_addicupo,
    last_update_addicupo_reason,
    last_update_addicupo_source,
    addicupo_state_v2,
    addicupo_state_v2_reason,
    ignore_progression,
    initial_addicupo,
    is_transactional_based,
    preferences,
    CASE WHEN scms.client_id is null then 'lms'
                                     else 'kordev'
                                     end as loan_tape_source,
    -- MANDATORY FIELDS
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at
-- DBT SOURCE REFERENCE
FROM {{ ref('syc_clients_co') }} sc
LEFT JOIN {{ ref('syc_client_migration_segments_co') }} scms
ON sc.client_id  = scms.client_id