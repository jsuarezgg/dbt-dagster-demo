{{
    config(
        materialized='incremental',
        unique_key='custom_application_reference_id',
        incremental_strategy='merge',
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}
-- SILVER SQL
{%- set config_dict = return_config_co_f_applications_previous_applications_reference_co_custom() -%}
{{ silver_sql_builder_alternative(config_dict) }}