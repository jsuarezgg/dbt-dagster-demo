{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}


SELECT
    -- MANDATORY FIELDS
    client_id,
    id_type,
    id_number,
    full_name,
    email,
    cellphone_number,
    total_addicupo,
    addicupo_remaining_balance,
    addi_select,
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at

-- DBT SOURCE REFERENCE
FROM {{ ref('client_management_clients_br') }}
