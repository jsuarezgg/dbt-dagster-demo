{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

{%- set experian_reports_list = [
    'experian_report_rep_201911_co',  
    'experian_report_rep_201912_co',  
    'experian_report_rep_202001_co',  
    'experian_report_rep_202002_co',  
    'experian_report_rep_202003_co',  
    'experian_report_rep_202004_co',  
    'experian_report_rep_202005_co',  
    'experian_report_rep_202006_co',  
    'experian_report_rep_202007_co',  
    'experian_report_rep_202008_co',  
    'experian_report_rep_202009_co',  
    'experian_report_rep_202010_co',  
    'experian_report_rep_202011_co',  
    'experian_report_rep_202012_co',  
    'experian_report_rep_202101_co',  
    'experian_report_rep_202102_co',  
    'experian_report_rep_202103_co',  
    'experian_report_rep_202104_co',  
    'experian_report_rep_202105_co',  
    'experian_report_rep_202106_co',  
    'experian_report_rep_202107_co',  
    'experian_report_rep_202108_co',  
    'experian_report_rep_202109_co',  
    'experian_report_rep_202110_co',  
    'experian_report_rep_202111_v2_co',
    'experian_report_rep_202112_v2_co',  
    'experian_report_rep_202201_v2_co',  
    'experian_report_rep_202202_co',  
    'experian_report_rep_202203_co',  
    'experian_report_rep_202204_co',  
    'experian_report_rep_202205_co',  
    'experian_report_rep_202206_co',  
    'experian_report_rep_202207_co',  
    'experian_report_rep_202208_co',
    'experian_report_rep_202209_co',
    'experian_report_rep_202210_co',
    'experian_report_rep_202211_co',
    'experian_report_rep_202212_co',
    'experian_report_rep_202301_co',
    'experian_report_rep_202302_co',
    'experian_report_rep_202303_co',
    'experian_report_rep_202304_co',
    'experian_report_rep_202305_co',
    'experian_report_rep_202306_co',
    'experian_report_rep_202307_co',
    'experian_report_rep_202308_co',
    'experian_report_rep_202309_co',
    'experian_report_rep_202310_co'
] -%}

WITH full_source AS (

{%- for report in experian_reports_list -%}

SELECT
    id_number,
    num_obl,
    name,
    dpd,
    novedad,
    saldo_mora,
    fecha_estado,
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at,
    {{ homolog_report_date("fecha_estado") }},
    {{ homolog_cut_off_month("fecha_estado") }}
FROM {{ ref(report) }}

{% if not loop.last -%}

UNION ALL

{% endif %}
{% endfor -%}

)

SELECT * FROM full_source;