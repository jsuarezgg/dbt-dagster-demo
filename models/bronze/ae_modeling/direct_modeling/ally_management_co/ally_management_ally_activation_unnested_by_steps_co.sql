{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

WITH select_explode AS (

SELECT
    slug,
    explode(from_json(data:['steps'],'array<string>')) AS step,
    data:['errors'] AS errors,
    data:['status'] AS final_status,
    created_at,
    updated_at
FROM {{ source('raw', 'ally_management_ally_activation_co')}}

)

SELECT
    -- STEP FIELDS
    CONCAT('SLG_',trim(slug), '_STP_', ROW_NUMBER() OVER (PARTITION BY slug ORDER BY 'A')) AS surrogate_key,
    trim(slug) AS ally_slug,
    ROW_NUMBER() OVER (PARTITION BY slug ORDER BY 'A') AS step_pseudo_idx,
    step:name AS step_name,
    step:module AS step_module,
    step:status AS step_status,
    step:required::boolean AS step_is_required,
    step:automatic::boolean AS step_is_automatic,
    step:executedAt::timestamp AS step_executed_at,
    step:completedAt::timestamp AS step_completed_at,
    step:executableBy AS step_executable_by,
    step:completableBy AS step_completable_by,
    COALESCE(errors, '[]') AS ally_activation_errors,
    final_status AS ally_activation_final_status,
    created_at::timestamp AS ally_activation_created_at,
    updated_at::timestamp AS ally_activation_updated_at,
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at
FROM select_explode
