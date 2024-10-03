{{
    config(
        materialized='incremental',
        unique_key='truora_event_validation_id',
        incremental_strategy='merge',
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}
-- SILVER SQL
{%- set config_dict = return_config_co_f_identity_photos_third_party_truora_events_co_custom() -%}
{{ silver_sql_builder_alternative(config_dict) }}