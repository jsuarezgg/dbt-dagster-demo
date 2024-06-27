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

 -- CUSTOM ATTRIBUTES
NOW() AS ingested_at,
to_timestamp('{{ var("execution_date") }}') AS updated_at

-- DBT SOURCE REFERENCE
FROM {{ source('raw', 'syc_condonations_co') }}