{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

--raw.syc_usury_rates_br
SELECT
    -- DIRECT MODELING FIELDS
    id,
    CAST(official_usury_rate AS DECIMAL(19,4)) as official_usury_rate,
    CAST(usury_rate AS DECIMAL(19,4)) as usury_rate,
    CAST(on_overdue_principal_rate AS DECIMAL(19,4)) as on_overdue_principal_rate,
    CAST(official_low_balance_usury_rate AS DECIMAL(19,4)) as official_low_balance_usury_rate,
    CAST(low_balance_usury_rate AS DECIMAL(19,4)) as low_balance_usury_rate,
    CAST(start_date AS TIMESTAMP) as start_date,
    CAST(end_date AS TIMESTAMP) as end_date,
    CAST(created_at AS TIMESTAMP) as created_at,
    created_by,
    updated_at as updated_at_source,
    updated_by,
    -- MANDATORY FIELDS
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at
-- DBT SOURCE REFERENCE
from {{ source('raw', 'syc_usury_rates_br') }}
