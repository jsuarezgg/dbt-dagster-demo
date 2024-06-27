{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}


--raw.pre_approvals_pago_co_pre_approval_evaluation_results_co
SELECT
    id,
    application_id,
    created_at,
    request,
    response
-- DBT SOURCE REFERENCE
FROM {{ ref('pre_approvals_pago_co_pre_approval_evaluation_results_co') }}
