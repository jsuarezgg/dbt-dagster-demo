{% macro bureau_report_experian_co_fields() -%}
    -- MANDATORY FIELDS
    id_number,
    num_obl,
    name,
    dpd,
    novedad,
    saldo_mora,
    fecha_estado,
    -- CUSTOM ATTRIBUTES
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at
{%- endmacro %}