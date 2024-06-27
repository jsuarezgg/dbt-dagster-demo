{% macro temp_field_for_silver(field_bronze=none, field_silver=none,bronze_table_alias=none, silver_table_alias=none) %}
{#- ocurred_on is mandatory for bronze_tables and last_event_ocurred_on_processed for silver tables -#}
{%- if bronze_table_alias is not none and field_bronze is not none -%}
CASE WHEN {{silver_table_alias}}.{{field_silver}} IS NULL OR {{bronze_table_alias}}.ocurred_on > {{silver_table_alias}}.last_event_ocurred_on_processed
    THEN {{bronze_table_alias}}.{{field_bronze}}
    ELSE {{silver_table_alias}}.{{field_silver}}
END AS {{field_silver}}
{%- else -%}
{{silver_table_alias}}.{{field_silver}} AS {{field_silver}}
{%- endif -%}
{% endmacro %}