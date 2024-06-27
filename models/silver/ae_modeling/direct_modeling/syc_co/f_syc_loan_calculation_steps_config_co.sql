{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}



--bronze.syc_loan_calculation_steps_config_co
SELECT
    -- MAPPED FIELDS - DIRECT ATTRIBUTES
id,
client_id,
loan_id,
type,
version,
start_date,
end_date,
ingested_at,
updated_at

-- DBT SOURCE REFERENCE
FROM {{ ref('syc_loan_calculation_steps_config_co') }}
