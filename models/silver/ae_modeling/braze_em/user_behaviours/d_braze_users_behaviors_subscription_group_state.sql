{#- Update macro to call, accordingly-#}
{%- set config_dict = return_config_d_braze_users_behaviors_subscription_group_state_custom() -%}

{#- Template -#}
{%- set table_single_pk = config_dict.get('relevant_properties',{}).get('files_db_table_pks',[])[0] -%}
{{
    config(
        materialized='incremental',
        unique_key= table_single_pk ,
        incremental_strategy='merge',
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}
-- DEBUG `table_single_pk`: `{{table_single_pk}}`
-- DEBUG `this`: `{{this}}`
{{ silver_sql_main_builder_braze(config_dict) }}