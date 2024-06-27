{{
    config(
        materialized='incremental',
        unique_key='payment_promise_id',
        incremental_strategy='merge',
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}
-- SILVER SQL
{%- set config_dict = return_config_co_f_client_payment_promises_co_custom() -%}
{{ silver_sql_builder_alternative(config_dict) }}