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
{%- set config_dict = return_config_br_f_kyc_iovation_v1v2_result_rules_br_logs_custom() -%}
{{ silver_sql_builder_alternative(config_dict, slack_time_variable='incremental_slack_time_in_days') }}