{% macro column_catalog_request(table_=none, schema_='bronze') %}
{# Macro develop to request and save relevant metadata of the requested table (Databricks). By Carlos Daniel A. Puerto Niño at 2022-04-19 #}
    { log("(*ADDI_LOG*) Call to the macro: `column_catalog_request(table_={},schema_={})`".format(table_,schema_),True) }}
    -- Case target_table (IF)
    {%- if table_ is not none -%}
        {% set target_table = table_ %}
    {%- else -%}
        {% set target_table = '*' %}
    {%- endif -%}
    { log("(*ADDI_LOG*) Target_table: "~target_table,True) }}
    -- Get relations /tables in scope defined (dbt_utils.get_relations_by_pattern())
    {%- set relations = dbt_utils.get_relations_by_pattern(schema_, target_table) -%}
    { log("(*ADDI_LOG*) relations: "~relations,True) }}
    -- Iterate over relations if on execute mode
    {% if execute %}
        {% for relation in relations %}
            {%- set describe_query -%}  DESCRIBE {{ relation.include(database=false) }} {%- endset -%}
                {{ log("(*ADDI_LOG*) /// Get table metadata query: " ~ describe_query, True) }}
                -- DESCRIBE Results for a relation/table & Save relevant results
                {% set results = run_query(describe_query) %}
                {% set results_list = results.rows %}
                {% set results_cn = results.column_names %}
                {{ log("(*ADDI_LOG*) >>>>>> Column names: " ~results_cn, True) }}
                -- MERGE QUERY
                {%- set merge_query -%}
                WITH cte_results AS (
                    SELECT '{{ relation.schema }}' AS table_schema, '{{relation.identifier}}' AS table_name, column_name, data_type, comment, now() AS db_created_at, now() AS db_updated_at 
                    FROM (
                    {%- for item in results_list -%}
                        {% if (' ' not in (item['col_name']|trim) and (item['data_type']!=''))|default(False) %}
                            {%- set col_name, data_type, comment = item['col_name'], item['data_type'], item['comment'] -%}
                            {{- log("(*ADDI_LOG*) >>>>>>>>> Valid row (included): " ~item, True) -}}
                            {%- if not loop.first %} UNION ALL {% endif %}
                            SELECT '{{col_name}}' AS column_name, '{{data_type}}' AS data_type, '{{comment}}' AS comment
                        {%- else -%}
                            {{- log("(*ADDI_LOG*) >>>>>>>>> Invalid row (not included): " ~item, True) -}}
                        {% endif -%}
                    {%- endfor -%}
                ))
                MERGE INTO bronze.unified_column_catalog AS destination
                USING cte_results AS source
                ON destination.table_schema = source.table_schema AND destination.table_name = source.table_name AND destination.column_name = source.column_name
                WHEN MATCHED AND (source.data_type != destination.data_type OR source.comment != destination.comment) THEN 
                UPDATE SET  destination.data_type = source.data_type,
                            destination.comment = source.comment,
                            destination.db_updated_at = source.db_updated_at
                WHEN NOT MATCHED THEN 
                INSERT * 
            {%- endset -%}
            {{ log("(*ADDI_LOG*) MERGE sentence:\n"~merge_query, True) }}
            {{ log("(*ADDI_LOG*) >>> RUNNING QUERY...", True) }}
            {%- do run_query(merge_query) -%}
            {{ log("(*ADDI_LOG*) >>> RUN SUCCESSFUL...\n -+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+", True) }}
        {% endfor %}
    {% else %}
        {{ log("(*ADDI_LOG*) NOT IN EXECUTION MODE - No actions taken in this macro.", True) }}
    {% endif %}
    {{ log("(*ADDI_LOG*) Macro completed.", True) }}
{% endmacro %}