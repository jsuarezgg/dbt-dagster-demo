
{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}


--bronze.ally_management_disbursements_co
SELECT 
        id,
        ally_slug,
        start_date,
        end_date,
        occurred_on,
        type,
        calculated_amount,
        status,
        data,
    -- CUSTOM ATTRIBUTES
        NOW() AS ingested_at,
        to_timestamp('{{ var("execution_date") }}') AS updated_at
-- DBT SOURCE REFERENCE
FROM {{ ref('ally_management_disbursements_co') }}
