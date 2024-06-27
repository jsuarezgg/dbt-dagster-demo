{% macro scoring_current_tables_fields() -%}
    -- MANDATORY FIELDS
    -- MAPPED FIELDS - DIRECT ATTRIBUTES
    decision_id,
    execution_id,
    context_id,
    created_at,
    payload,
    decision_unit,
    flow,
    -- CUSTOM ATTRIBUTES
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at
{%- endmacro %}
