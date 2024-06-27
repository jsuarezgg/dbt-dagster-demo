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
    nr.netsuite_id,
    nr.account_number,
    nr.account_name,
    nr.account_number_mirror,
    nr.account_name_mirror,
    nr.transaction_type,
    nr.document_number,
    nr.transaction_date,
    nr.supplier_id,
    nr.supplier_number,
    nr.supplier_name,
    nr.debit_local_currency,
    nr.credit_local_currency,
    nr.net_amount_local_currency,
    nr.currency,
    nr.trx_fx,
    nr.note,
    nr.principal_note,
    nr.ally_slug,
    nr.area,
    nr.reporting,
    nr.product_subcategory,
    nr.country,
    nr.project,
    nr.legal_entity,
    nr.fx_rate,
    nr.net_amount_local_currency / nr.fx_rate AS amount_usd,
    nr.report_type,
    nrcoa.level_1,
    nrcoa.level_2,
    nrcoa.level_3,
    nrcoa.level_4,
    -- CUSTOM ATTRIBUTES
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at
-- DBT SOURCE REFERENCE
FROM {{ ref('netsuite_report') }} nr
LEFT JOIN {{ ref('netsuite_report_chart_of_accounts') }} nrcoa   ON nr.account_number = nrcoa.account_number