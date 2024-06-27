{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

{%- set netsuite_fx_rate_br = 5.2 -%}
{%- set netsuite_fx_rate_us = 1 -%}

WITH fx_rate AS (
    SELECT price 
    FROM {{ source('silver', 'd_fx_rate') }}
    WHERE is_active is true and country_code='CO'
),

raw_extraction AS (
    SELECT
        -- MANDATORY FIELDS
        -- MAPPED FIELDS - DIRECT ATTRIBUTES
        idInterno AS netsuite_id,
        SUBSTRING(cuenta, 1, 8) AS account_number,
        SUBSTRING(cuenta, 9, 200) AS account_name,
        SUBSTRING(cuentaPrincipal, 1, 8) AS account_number_mirror,
        SUBSTRING(cuentaPrincipal, 9, 200) AS account_name_mirror,
        tipo AS transaction_type,
        numeroDocumento AS document_number,
        fecha AS transaction_date,
        idInternoCliente AS supplier_id,
        CASE WHEN nombreEmpresa IS NULL OR nombreEmpresa = ''
                THEN REGEXP_EXTRACT(nombreEmpresaLinea, '(\\d+)', 1)
                ELSE REGEXP_EXTRACT(nombreEmpresa, '(\\d+)', 1) END AS supplier_number,
        CASE WHEN nombreEmpresa IS NULL OR nombreEmpresa = ''
                THEN TRIM(REGEXP_EXTRACT(nombreEmpresaLinea, '(\\D+)', 1))
                ELSE TRIM(REGEXP_EXTRACT(nombreEmpresa, '(\\D+)', 1)) END AS supplier_name,
        CAST(importeDebito AS DOUBLE) AS debit_local_currency,
        CAST(importeCredito AS DOUBLE) AS credit_local_currency,
        CAST(importeDebito AS DOUBLE)-CAST(importeCredito AS DOUBLE) AS net_amount_local_currency,
        moneda AS currency,
        tipoCambio AS trx_fx,
        nota as note,
        notaPrincipal as principal_note,
        departmento AS area,
        reporting,
        product,
        country,
        project,
        subsidiaria AS legal_entity,
        CASE
            WHEN subsidiaria = 'Adelante Soluções Financeiras Ltda' THEN  {{ netsuite_fx_rate_br }}
            WHEN subsidiaria = 'Adelante Soluciones Financieras SAS' THEN rate.price
            WHEN subsidiaria = 'Adelante Securitizadora S.A.' THEN {{ netsuite_fx_rate_br }}
            WHEN subsidiaria = 'Adelante Financial Holdings Limited' THEN {{ netsuite_fx_rate_us }}
            WHEN subsidiaria = 'Adelante US operation INC' THEN {{ netsuite_fx_rate_us }}
            WHEN subsidiaria = 'ADDI Pagamentos LTDA' THEN {{ netsuite_fx_rate_br }}
            WHEN subsidiaria = 'Empresa principal' THEN {{ netsuite_fx_rate_us }}
            WHEN subsidiaria = 'Adelante Financial Intermediate Holdings LLC' THEN {{ netsuite_fx_rate_us }}
            WHEN subsidiaria = 'ADDI Brasil Participações LTDA' THEN {{ netsuite_fx_rate_br }}
        END AS fx_rate,
        -- CUSTOM ATTRIBUTES
        NOW() AS ingested_at,
        to_timestamp('{{ var("execution_date") }}') AS updated_at
    -- DBT SOURCE REFERENCE
    FROM {{ source('raw', 'netsuite_report') }}
    LEFT JOIN fx_rate rate ON 1=1
)
SELECT
    netsuite_id,
    account_number,
    account_name,
    account_number_mirror,
    account_name_mirror,
    transaction_type,
    document_number,
    transaction_date,
    supplier_id,
    supplier_number,
    supplier_name,
    debit_local_currency,
    credit_local_currency,
    net_amount_local_currency,
    currency,
    trx_fx,
    note,
    principal_note,
    CASE WHEN account_number ILIKE '41503001%' THEN substring(note, 1, position(' ' IN note) - 1) END AS ally_slug,
    area,
    reporting,
    product as product_subcategory,
    country,
    project,
    legal_entity,
    fx_rate,
    'ACTUALS' AS report_type,
    ingested_at
FROM raw_extraction
WHERE transaction_type NOT ILIKE '%orden de compra%'


