{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

-- raw.ally_management_ally_seller_co

SELECT
    id,
    created_at,
    ally_slug,
    store_slug,
    data:firstname AS firstname,
    data:lastname AS lastname,
    data:cellphone AS cellphone, 
    data:email AS email, 
    national_id_type,
    national_id_number,
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at
FROM {{ source('raw', 'ally_management_ally_seller_co') }}