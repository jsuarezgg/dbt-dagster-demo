{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

-- bronze.ally_management_ally_invoices_co
SELECT
    -- MAPPED FIELDS - DIRECT ATTRIBUTES
    invoice_id,
    type,
    categories,
    ally_slug,
    occurred_on,
    start_date,
    end_date,
    total,
    fee,
    lead_gen_fee,
    report_term,
    payment_term,
    `data`,
    -- CUSTOM ATTRIBUTES
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at
-- DBT SOURCE REFERENCE
FROM {{ ref('ally_management_ally_invoices_co') }}

