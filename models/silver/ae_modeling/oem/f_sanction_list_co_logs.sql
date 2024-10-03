{{
    config(
        materialized='incremental',
        unique_key='custom_event_application_santion_list_id_number_pairing_id',
        incremental_strategy='merge',
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}
-- SILVER SQL
{%- set config_dict = return_config_co_f_sanction_list_co_logs_custom() -%}
{{ silver_sql_builder_alternative(config_dict) }}
