{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}


--raw.syc_condonations_co
SELECT
    -- MAPPED FIELDS - DIRECT ATTRIBUTES
id,
client_id,
loan_id,
bucket,
percentage,
amount,
date,
reason,
agent_id,
type,
ingested_at,
updated_at

-- DBT SOURCE REFERENCE
FROM {{ ref('syc_condonations_co') }}