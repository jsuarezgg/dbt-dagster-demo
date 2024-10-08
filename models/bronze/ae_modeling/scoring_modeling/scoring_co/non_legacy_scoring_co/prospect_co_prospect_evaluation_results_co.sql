{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}


--raw.prospect_co_prospect_evaluation_results_co
SELECT
    id,
    application_id,
    created_at,
    request,
    response
-- DBT SOURCE REFERENCE
FROM {{ source('raw', 'prospect_co_prospect_evaluation_results_co') }}
