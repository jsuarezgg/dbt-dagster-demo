{% macro generate_wide_to_long_format_statements(data_sources_config_dict) %}
    {%- for data_source_dict in data_sources_config_dict.sources %}
    ( -- SOURCE N
        SELECT
        {{ data_sources_config_dict.primary_key.expr }} AS {{ data_sources_config_dict.primary_key.alias }},
        STACK({{ data_source_dict['columns'] | length }},
                {%- for column_dict in data_source_dict['columns'] %}
                '{{ column_dict.alias }}',
                {{ column_dict.expr }}::STRING{% if not loop.last %},{% endif %}
                {% endfor -%}
        ) AS (column_name, column_value)
        FROM
        ( -- Subquery to apply WHERE AND/OR LIMIT before taking it from wide to long format
            SELECT *
            FROM {% if data_source_dict.type == 'model' -%}{{ ref(data_source_dict.name) }}
                 {% elif data_source_dict.type == 'source' -%}{{ source(data_source_dict.schema, data_source_dict.name) }}
                 {%- endif %}
            {{ data_source_dict.where_or_limit_expr }}
        )
    )
        {% if not loop.last %}UNION ALL{% endif %}
    {% endfor -%}
{% endmacro %}
