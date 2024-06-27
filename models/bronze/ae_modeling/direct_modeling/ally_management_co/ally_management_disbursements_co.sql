
{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}


--raw.ally_management_disbursements_co
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
FROM {{ source('raw', 'ally_management_disbursements_co') }}
