{{
    config(
        materialized='incremental',
        unique_key='application_product_policy_id',
        incremental_strategy='merge',
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}
-- SILVER SQL
{%- set config_dict = return_config_co_f_allies_product_policies_co_custom() -%}
{{ silver_sql_builder_alternative(config_dict) }}