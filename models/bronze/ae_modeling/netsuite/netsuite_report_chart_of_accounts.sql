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
    idExterno AS external_id,
    numero AS account_number,
    ccsCoSubcuenta AS account_name,
    nivel1 AS level_1,
    nivel2 AS level_2,
    nivel3 AS level_3,
    nivel4 AS level_4,
    -- CUSTOM ATTRIBUTES
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at
-- DBT SOURCE REFERENCE
FROM {{ source('raw', 'netsuite_report_chart_of_accounts') }}
