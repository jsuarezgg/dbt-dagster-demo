{{
    config(
        materialized='incremental',
        unique_key='event_id',
        incremental_strategy='merge',
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}
-- SILVER SQL
{%- set config_dict = return_config_co_f_loan_cancellations_v2_co_logs_custom() -%}
{{ silver_sql_builder_alternative(config_dict) }}