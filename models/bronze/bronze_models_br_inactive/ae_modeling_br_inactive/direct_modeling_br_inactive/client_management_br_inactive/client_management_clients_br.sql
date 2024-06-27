{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

--raw.client_management_clients_br
SELECT
    -- MANDATORY FIELDS
    id AS client_id,
    id_type,
    id_number,
    full_name,
    email,
    cell_phone_number AS cellphone_number,
    total_addicupo,
    addicupo_remaining_balance,
    addi_select,
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at

-- DBT SOURCE REFERENCE
FROM {{ source('raw', 'client_management_clients_br') }}
