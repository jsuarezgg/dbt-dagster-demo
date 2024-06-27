{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}
--bronze.engineering_metrics_services
SELECT *
FROM {{ source('bronze', 'engineering_metrics_services') }}