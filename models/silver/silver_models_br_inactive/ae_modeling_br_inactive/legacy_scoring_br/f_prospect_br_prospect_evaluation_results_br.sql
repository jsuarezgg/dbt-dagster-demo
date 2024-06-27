{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}


--raw.prospect_br_prospect_evaluation_results_br
SELECT
    id,
    application_id,
    created_at,
    request,
    response
-- DBT SOURCE REFERENCE
FROM {{ ref('prospect_br_prospect_evaluation_results_br') }}
