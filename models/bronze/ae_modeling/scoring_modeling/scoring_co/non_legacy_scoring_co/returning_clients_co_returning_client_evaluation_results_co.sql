{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}


--raw.returning_clients_co_returning_client_evaluation_results
SELECT
    -- DIRECT MODELING FIELDS
    id,
    application_id,
    created_at,
    request,
    response,
    -- CUSTOM ATTRIBUTES
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at
-- DBT SOURCE REFERENCE
FROM {{ source('raw', 'returning_clients_co_returning_client_evaluation_results_co') }}



