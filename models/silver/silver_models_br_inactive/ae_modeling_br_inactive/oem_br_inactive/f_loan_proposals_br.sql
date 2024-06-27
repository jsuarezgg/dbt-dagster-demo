{{
    config(
        materialized='incremental',
        unique_key='loan_proposal_id',
        incremental_strategy='merge',
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}
-- SILVER SQL
{%- set config_dict = return_config_br_f_loan_proposals_br_custom() -%}
{{ silver_sql_builder_alternative(config_dict) }}