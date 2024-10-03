{% macro soft_filter_user_attribute_rules(column_name='__EMPTY__') %}
    {#- Transformations for array values -#}
    {% if column_name in ['top_categories'] %}
        {%- set return_column_expr = "ARRAY_JOIN(" ~ column_name ~ ",';;')" -%}
    {% else %}
        {%- set return_column_expr = column_name -%}
    {% endif %}

    {#- Soft filtering (nullification) rules -#}
    {% if column_name == 'cupo_status' %}
        {{ return(return_column_expr) }}
    {% else %}
        {%- set case_statement = "CASE WHEN cupo_status = 'FROZEN' THEN NULL ELSE " ~ return_column_expr ~ " END" -%}
        {{ return(case_statement) }}
    {% endif %}
{% endmacro %}