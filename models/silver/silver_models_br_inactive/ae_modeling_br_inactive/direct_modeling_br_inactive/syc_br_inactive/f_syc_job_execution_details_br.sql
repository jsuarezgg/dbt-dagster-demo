{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}


--raw.syc_job_execution_details_br
SELECT
    -- MAPPED FIELDS - DIRECT ATTRIBUTES
id,
name,
execution_date,
target_items,
ingested_at,
updated_at

-- DBT SOURCE REFERENCE
FROM {{ ref('syc_job_execution_details_br') }}