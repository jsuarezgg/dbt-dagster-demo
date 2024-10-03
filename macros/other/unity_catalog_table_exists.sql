{% macro unity_catalog_table_exists(schema = '_NO_VALUE_', table = '_NO_VALUE_', catalog ='addi_prod')%}
    {# Custom Macro: 1) Build query on metadata table #}
    {% set query %}
    --DBT Custom Macro call to validate if a table exists. Uses system catalog to assess it
    SELECT TRUE AS row_exists
    FROM system.information_schema.tables
    WHERE table_catalog = '{{catalog}}' AND table_schema = '{{schema}}' AND table_name = '{{table}}'
    {% endset %}
    {# 2) Evaluates if at least one row was returned, meaning there is a coincidence in the search
       Requires read permissions over `system` catalog and also over `information_schema.tables` table #}
    {% set table_exists = (run_query(query)|length) > 0 %}
    {{ return(table_exists) }}
{% endmacro %}