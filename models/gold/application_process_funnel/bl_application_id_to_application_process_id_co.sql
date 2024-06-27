{{
    config(
        materialized='table',
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}
WITH
stg_funnel_step2_application_process_representation_co AS (
    SELECT *
    FROM {{ ref('stg_funnel_step2_application_process_representation_co') }}
)
,
stg_2_explode AS (
    SELECT
        application_process_id,
        application_process_baseline_id,
        EXPLODE(debug_application_ids_array) AS application
    FROM stg_funnel_step2_application_process_representation_co
)

SELECT
    application.application_id AS application_id,
    application_process_id,
    application_process_baseline_id AS debug_application_process_baseline_id,
    -- DATA PLATFORM COLUMNS
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at
FROM stg_2_explode