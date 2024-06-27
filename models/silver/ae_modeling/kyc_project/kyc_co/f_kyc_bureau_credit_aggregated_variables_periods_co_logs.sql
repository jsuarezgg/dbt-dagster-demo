{{
    config(
        materialized='incremental',
        unique_key='surrogate_key',
        incremental_strategy='merge',
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}
-- SILVER SQL
{%- set config_dict = return_config_co_f_kyc_bureau_credit_aggregated_variables_periods_co_logs_custom() -%}
{{ silver_sql_builder_alternative(config_dict, slack_time_variable='incremental_slack_time_in_days') }}