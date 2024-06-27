{% macro scoring_previous_tables_fields() -%}
    -- MANDATORY FIELDS
    -- MAPPED FIELDS - DIRECT ATTRIBUTES
    id,
    application_id,
    created_at,
    evaluation_data,
    evaluation_result,
    -- CUSTOM ATTRIBUTES
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at
{%- endmacro %}