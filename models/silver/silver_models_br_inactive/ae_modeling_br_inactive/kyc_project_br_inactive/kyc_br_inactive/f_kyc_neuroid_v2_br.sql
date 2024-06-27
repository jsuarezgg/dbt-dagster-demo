{{
    config(
        materialized='incremental',
        unique_key='application_id',
        incremental_strategy='merge',
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}
-- SILVER SQL
{%- set config_dict = return_config_br_f_kyc_neuroid_v2_br_custom() -%}
{{ silver_sql_builder_alternative(config_dict, slack_time_variable='incremental_slack_time_in_days') }}