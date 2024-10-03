{{
    config(
        materialized='incremental',
        unique_key='custom_event_discount_idx_pairing_id',
        incremental_strategy='merge',
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}
-- SILVER SQL
{%- set config_dict = return_config_co_f_applications_discounts_co_logs_custom() -%}
{{ silver_sql_builder_alternative(config_dict) }}