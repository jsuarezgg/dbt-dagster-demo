{{
    config(
        materialized='incremental',
        unique_key='ally_slug',
        incremental_strategy='merge',
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}
-- SILVER SQL
{%- set config_dict = return_config_co_f_ally_store_health_checks_co_custom() -%}
{{ silver_sql_builder_alternative(config_dict) }}