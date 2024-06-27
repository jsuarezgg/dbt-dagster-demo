{{
    config(
        materialized='incremental',
        unique_key='store_slug',
        incremental_strategy='merge',
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}
-- SILVER SQL
{%- set config_dict = return_config_co_d_ally_stores_co_custom() -%}
{{ silver_sql_builder_alternative(config_dict) }}