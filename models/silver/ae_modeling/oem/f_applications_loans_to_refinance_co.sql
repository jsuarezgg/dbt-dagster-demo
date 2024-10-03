{{
    config(
        materialized='incremental',
        unique_key='custom_loan_refinance_id',
        incremental_strategy='merge',
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}
-- SILVER SQL
{%- set config_dict = return_config_co_f_applications_loans_to_refinance_co_custom() -%}
{{ silver_sql_builder_alternative(config_dict) }}