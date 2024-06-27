{{
    config(
        materialized='incremental',
        unique_key='pix_payment_number',
        incremental_strategy='merge',
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}
-- SILVER SQL
{%- set config_dict = return_config_br_f_pix_payment_refunds_br_custom() -%}
{{ silver_sql_builder_alternative(config_dict) }}