{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

SELECT
    surrogate_key,
    ally_slug,
    step_pseudo_idx,
    step_name,
    step_module,
    step_status,
    step_is_required,
    step_is_automatic,
    step_executed_at,
    step_completed_at,
    step_executable_by,
    step_completable_by,
    ally_activation_errors,
    ally_activation_final_status,
    ally_activation_created_at,
    ally_activation_updated_at,
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at
FROM {{ ref('ally_management_ally_activation_unnested_by_steps_co') }}
