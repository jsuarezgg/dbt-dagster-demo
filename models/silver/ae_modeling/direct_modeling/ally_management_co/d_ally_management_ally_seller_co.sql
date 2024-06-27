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
    id AS ally_seller_id,
    ally_slug,
    store_slug,
    firstname,
    lastname,
    cellphone, 
    email, 
    national_id_type,
    national_id_number,
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at
-- DBT SOURCE REFERENCE
FROM {{ ref('ally_management_ally_seller_co') }} 

