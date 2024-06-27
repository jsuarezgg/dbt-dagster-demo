{{
    config(
        materialized='table',
        unique_key='loan_id',
        incremental_strategy='merge',
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}
-- SILVER SQL
{%- set config_dict = return_config_br_f_loan_cancellation_br_custom() -%}
{{ silver_sql_builder_alternative(config_dict) }}