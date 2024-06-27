{{
    config(
        materialized='incremental',
        unique_key='shipping_order_id',
        incremental_strategy='merge',
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}
-- SILVER SQL
{%- set config_dict = return_config_co_f_ally_pop_material_shipping_orders_co_custom() -%}
{{ silver_sql_builder_alternative(config_dict) }}