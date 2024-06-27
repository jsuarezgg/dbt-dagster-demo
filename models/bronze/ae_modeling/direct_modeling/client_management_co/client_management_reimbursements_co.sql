{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

--raw.client_management_reimbursements_co
SELECT
    -- MANDATORY FIELDS
    id AS reimbursement_id,
    client_id,
    reimbursement_date,
    amount,
    created_at,
    cast(annulled as BOOLEAN) as annulled,
    annulment_reason,
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at

-- DBT SOURCE REFERENCE
FROM {{ source('raw', 'client_management_reimbursements_co') }}
