{{
    config(
        materialized='table',
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

 unified_tables as (
SELECT "Netsuite pnl" as source,
account_number,
account_name,
document_number,
transaction_date as date,
transaction_type,
ally_slug ,
supplier_name,
note,
principal_note,
product_subcategory,
null as product_category,
area,
reporting,
country,
project,
legal_entity,
net_amount_local_currency*(-1) as net_amount_lc,
amount_usd*(-1) as net_amount_usd,
report_type,
level_1,
level_2,
level_3,
level_4
FROM {{ ref('f_netsuite_pnl')}}

UNION ALL

SELECT
"Databricks Actuals" AS Source,
null as account_number,
null as account_name,
null as document_number,
date_trunc('month',origination_date) date,
null as transaction_type,
null as ally_slug,
null as supplier_name,
null as note,
null as principal_note,
synthetic_product_subcategory as product_subcategory,
synthetic_product_category as product_category,
"General" AS area,
"General" AS reporting,
"CO" AS country, 
"General" AS project,
"General" AS legal_entity,
sum(gmv) AS net_amount_lc,
sum(gmv)/max(price) AS net_amount_usd,
"Actuals" AS report_type,
"Total GMV" AS level_1,
"GMV Colombia" AS level_2,
"GMV" AS level_3,
"GMV" AS level_4
FROM {{ ref('dm_originations') }}
LEFT JOIN (select price from {{ source('silver','d_fx_rate') }}
where country_code='CO' and is_active is true) ON 1=1
WHERE country_code='CO'
GROUP BY 1,2,3,4,5,6,7,8,11,12,13,14,15

UNION

SELECT
"Databricks Actuals" AS Source,
null as account_number,
null as account_name,
null as document_number,
date_trunc('month',origination_date) date,
null as transaction_type,
null as ally_slug,
null as supplier_name,
null as note,
null as principal_note,
synthetic_product_subcategory as product_subcategory,
synthetic_product_category as product_category,
"General" AS area,
"General" AS reporting,
"CO" AS country, 
"General" AS project,
"General" AS legal_entity,
avg(gmv) AS net_amount_lc,
avg(gmv)/max(price) AS net_amount_usd,
"Actuals" AS report_type,
"Total AOV" AS level_1,
"AOV Colombia" AS level_2,
"AOV" AS level_3,
"AOV" AS level_4
FROM {{ ref('dm_originations') }}
LEFT JOIN (select price from {{ source('silver','d_fx_rate') }}
where country_code='CO' and is_active is true) ON 1=1
WHERE country_code='CO'
GROUP BY 1,2,3,4,5,6,7,8,11,12,13,14,15

UNION

SELECT
"Databricks Actuals" AS Source,
null as account_number,
null as account_name,
null as document_number,
date_trunc('month',origination_date) date,
null as transaction_type,
null as ally_slug,
null as supplier_name,
null as note,
null as principal_note,
synthetic_product_subcategory as product_subcategory,
synthetic_product_category as product_category,
"General" AS area,
"General" AS reporting,
"CO" AS country, 
"General" AS project,
"General" AS legal_entity,
count(*) AS net_amount_lc,
count(*) AS net_amount_usd,
"Actuals" AS report_type,
"Total Transactions" AS level_1,
"Transactions Colombia" AS level_2,
"Transactions" AS level_3,
"Transactions" AS level_4
FROM {{ ref('dm_originations') }}
WHERE country_code='CO'
GROUP BY 1,2,3,4,5,6,7,8,11,12,13,14,15

UNION

SELECT "Actuals adjustments" as source,
adj.Cuenta as account_number,
adj.Nombre_cuenta as account_name,
null as document_number,
adj.Date as date,
null as transaction_type,
null as ally_slug ,
adj.Proveedor as supplier_name,
adj.notas as note,
null as principal_note,
adj.product as product_subcategory,
null as product_category,
adj.Area as area,
adj.Reporting as reporting,
adj.Country as country,
adj.Project as project,
adj.Entidad as legal_entity,
adj.net_amount_LC as net_amount_lc,
0 as net_amount_usd,
adj.Report_type as report_type,
adj.Level_1 as level_1,
adj.Level_2 as level_2,
adj.Level_3 as level_3,
adj.Level_4 as level_4
FROM {{ source('sandbox','netsuite_actuals_adjustments_gsheets') }} AS adj

UNION ALL

SELECT "Budgets" as source,
bud.Cuenta as account_number,
bud.Nombre_cuenta as account_name,
null as document_number,
bud.attribute as date,
null as transaction_type,
null as ally_slug ,
bud.Proveedor as supplier_name,
bud.notas as note,
null as principal_note,
bud.product as product_subcategory,
null as product_category,
bud.Area as area,
bud.Reporting as reporting,
bud.Country as country,
bud.Project as project,
bud.Entidad as legal_entity,
bud.net_amount_lc,
CASE
            WHEN bud.Entidad = 'Adelante Soluções Financeiras Ltda' THEN  bud.net_amount_LC/{{ netsuite_fx_rate_br }}
            WHEN bud.Entidad = 'Adelante Soluciones Financieras SAS' THEN bud.net_amount_LC/rate.price
            WHEN bud.Entidad = 'Adelante Securitizadora S.A.' THEN bud.net_amount_LC/{{ netsuite_fx_rate_br }}
            WHEN bud.Entidad = 'Adelante Financial Holdings Limited' THEN bud.net_amount_LC/{{ netsuite_fx_rate_us }}
            WHEN bud.Entidad = 'Adelante US operation INC' THEN bud.net_amount_LC/{{ netsuite_fx_rate_us }}
            WHEN bud.Entidad = 'ADDI Pagamentos LTDA' THEN bud.net_amount_LC/{{ netsuite_fx_rate_br }}
            WHEN bud.Entidad = 'Empresa principal' THEN bud.net_amount_LC/{{ netsuite_fx_rate_us }}
            WHEN bud.Entidad = 'Adelante Financial Intermediate Holdings LLC' THEN bud.net_amount_LC/{{ netsuite_fx_rate_us }}
            WHEN bud.Entidad = 'ADDI Brasil Participações LTDA' THEN bud.net_amount_LC/{{ netsuite_fx_rate_br }}
END as net_amount_usd,
bud.Report_type as report_type,
bud.Level_1 as level_1,
bud.Level_2 as level_2,
bud.Level_3 as level_3,
bud.Level_4 as level_4
FROM {{ source('sandbox','netsuite_budget_gsheets') }} AS bud
LEFT JOIN fx_rate rate ON 1=1)

SELECT *
FROM unified_tables