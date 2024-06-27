{{
    config(
        materialized='incremental',
        unique_key='loan_id',
        incremental_strategy='merge',
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}
-- SILVER SQL
{%- set config_dict = return_config_co_f_refinance_loans_co_custom() -%}
{{ silver_sql_builder_alternative(config_dict) }}