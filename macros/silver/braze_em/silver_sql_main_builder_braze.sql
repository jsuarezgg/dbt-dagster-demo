{%- macro silver_sql_main_builder_braze(config_dict=none, slack_time_variable='incremental_slack_braze_em_hours', timestamp_sorting_key='ocurred_on') %}

{#- Validation: Config dict -#}
{%- if config_dict is none -%}
    {{ exceptions.raise_compiler_error("No config_dict provided") }}
{%- endif -%}

{#- INCREMENTAL RUNS - Validating slack_time_variable  -#}
{%- if not slack_time_variable in ['incremental_slack_braze_em_hours','incremental_slack_time_in_days'] -%}
    {{ exceptions.raise_compiler_error("Macro custom error >>> silver_sql_builder_alternative() >>> argument `slack_time_variable` must be one of the following: `incremental_slack_braze_em_hours`,`incremental_slack_time_in_days`") }}
{%- endif -%}

{#- SPECIFIC -#}
{%- set sorting_key = timestamp_sorting_key %}
{%- set country = config_dict.get('relevant_properties',{}).get('schema_country','') -%}
{%- set silver_table_name = config_dict.get('relevant_properties',{}).get('files_db_table_name','MISSING_TABLE_NAME') -%}
{%- set table_pk_fields = config_dict.get('relevant_properties',{}).get('files_db_table_pks',[]) -%}
{%- set table_pk_amount = (table_pk_fields | length) -%}
{%- set fields_direct = config_dict.get('unique_db_fields',{}).get('direct',[]) -%}
{%- set mandatory_fields = ["event_name", "event_id"] -%}
{%- set events_dict = config_dict.get('events', {}) %}
{%- set events_keys = config_dict.get('events', {}).keys() | list %}
{%- set flag_group_feature_active = config_dict.get('is_group_feature_active', False) %}

{#- VALIDATION: Missing key args -#}
{% if silver_table_name == 'MISSING_TABLE_NAME' or (fields_custom | length == 0 and fields_direct | length == 0) %}
    {{ exceptions.raise_compiler_error("NO SILVER TABLE NAME FOUND OR fields_direct is EMPTY") }}
{% endif %}

-- SECTION 1 -> CALLING BRONCE ref's
WITH
{% for event_key, event_value in events_dict.items() -%}
    {#- Event Key handle -#}
    {%- if country != '' -%} {%- set ref_table_and_cte_name = (event_key | lower)+'_'+(country) -%}
    {%- else -%}             {%- set ref_table_and_cte_name = (event_key | lower) -%}
    {%- endif -%}

    {%- if not loop.first -%}, {%- endif -%}
    {{ ref_table_and_cte_name }} AS (
    SELECT *
    FROM {{ ref(ref_table_and_cte_name) }}
    {%- if is_incremental() %}
        {%- if slack_time_variable == 'incremental_slack_braze_em_hours' %}
    WHERE {{ sorting_key }}_date BETWEEN (to_date('{{ var("start_date","placeholder_prev_exec_date") }}'- INTERVAL "{{var('incremental_slack_braze_em_hours')}}" HOUR)) AND to_date('{{ var("end_date","placeholder_end_date") }}') AND
        {{ sorting_key }} BETWEEN (to_timestamp('{{ var("start_date","placeholder_prev_exec_date") }}'- INTERVAL "{{var('incremental_slack_braze_em_hours')}}" HOUR)) AND to_timestamp('{{ var("end_date","placeholder_end_date") }}')
        {%- elif slack_time_variable == 'incremental_slack_time_in_days' %}
    WHERE {{ sorting_key }}_date BETWEEN (to_date('{{ var("start_date","placeholder_prev_exec_date") }}'- INTERVAL "{{var('incremental_slack_time_in_days')}}" DAY)) AND to_date('{{ var("end_date","placeholder_end_date") }}') AND
        {{ sorting_key }} BETWEEN (to_timestamp('{{ var("start_date","placeholder_prev_exec_date") }}'- INTERVAL "{{var('incremental_slack_time_in_days')}}" DAY)) AND to_timestamp('{{ var("end_date","placeholder_end_date") }}')
        {%- endif -%}
    {%- endif -%} {# When in prod: {{ addi_prod.bronze.{{event_key | lower}} }} )#}
)
{% endfor %}

--SECTION 1B -> UNION BRONCE ref's
, union_bronze AS (
    {% for event_key, event_value in events_dict.items() -%}
    {#- Event Key handle -#}
    {%- if country != '' -%} {%- set ref_cte_name = (event_key | lower)+'_'+(country) -%}
    {%- else -%}             {%- set ref_cte_name = (event_key | lower) -%}
    {%- endif -%}
    SELECT 
        {{ silver_sql_fields_builder_braze(fields_direct, event_value.get('direct_attributes', []), mandatory_fields, sorting_key, flag_group_feature_active) }}
    FROM {{ ref_cte_name }}
    {%- if not loop.last %}
    UNION ALL
    {%- endif %}
    {% endfor %}
) 
-- SECTION 2 -> UNION of prepared CTE's in Section 1
, union_all_events AS (
    (
    SELECT
    {% if flag_group_feature_active -%}
        {{ silver_sql_fields_builder_braze(fields_direct, fields_direct ,mandatory_fields, sorting_key, flag_group_feature_active, True) }}, last_event_{{sorting_key}}_processed AS first_event_{{sorting_key}}_processed
    {% else -%}
        {%- for table_pk_field in table_pk_fields -%} {{ table_pk_field }}, {%- endfor -%}
        {{ silver_sql_fields_builder_braze(fields_direct, fields_direct ,mandatory_fields, sorting_key, flag_group_feature_active, False, table_pk_fields) }}
    {%- endif %}
    FROM union_bronze
    )
    {% if is_incremental() and flag_group_feature_active -%}
    UNION ALL
    (
    SELECT 
    {{ silver_sql_fields_builder_braze(fields_direct, fields_direct, mandatory_fields, sorting_key, flag_group_feature_active, True) }}, first_event_{{sorting_key}}_processed
    FROM {{ this }}
    WHERE 
    {% for table_pk_field in table_pk_fields -%}
        {{ table_pk_field }} IN (SELECT DISTINCT {{ table_pk_field }} FROM union_bronze) {% if not loop.last  %} AND {% endif %}
    {% endfor -%}
    )
    {%- endif %}
)

{% if flag_group_feature_active -%}
 -- SECTION 3 -> merge events by key and keep last not null data in each field
  , grouped_events AS (
  select
    {% for table_pk_field in table_pk_fields -%}
        {{ table_pk_field }},
    {% endfor -%}
    {%- for field in fields_direct -%}
    {%- if field not in mandatory_fields and (field != sorting_key) and field not in table_pk_fields -%}
    element_at(array_sort(array_agg(CASE WHEN {{ field }} is not null then struct({{ sorting_key }}, {{ field }}) else NULL end), (left, right) -> case when left.{{ sorting_key }} < right.{{ sorting_key }} then 1 when left.{{ sorting_key }} > right.{{ sorting_key }} then -1 when left.{{ sorting_key }} == right.{{ sorting_key }} then 0 end), 1).{{ field }} as {{ field }},
    {% endif -%}
    {% endfor -%}
    {%- for mandatory_field in mandatory_fields -%}
    {%- if (mandatory_field != sorting_key) -%}
    {%- if flag_group_feature_active -%}
    {%- set silver_mandatory_field = 'last_' ~ mandatory_field ~ '_processed' -%}
    element_at(array_sort(array_agg(CASE WHEN {{ silver_mandatory_field }} is not null then struct({{ sorting_key }}, {{ silver_mandatory_field }}) else NULL end), (left, right) -> case when left.{{ sorting_key }} < right.{{ sorting_key }} then 1 when left.{{ sorting_key }} > right.{{ sorting_key }} then -1 when left.{{ sorting_key }} == right.{{ sorting_key }} then 0 end), 1).{{ silver_mandatory_field }} as {{ silver_mandatory_field }},
    {% else -%}
    element_at(array_sort(array_agg(CASE WHEN {{ mandatory_field }} is not null then struct({{ sorting_key }}, {{ mandatory_field }}) else NULL end), (left, right) -> case when left.{{ sorting_key }} < right.{{ sorting_key }} then 1 when left.{{ sorting_key }} > right.{{ sorting_key }} then -1 when left.{{ sorting_key }} == right.{{ sorting_key }} then 0 end), 1).{{ mandatory_field }} as {{ mandatory_field }},
    {% endif -%}
    {% endif -%}
    {% endfor -%}
    MIN(first_event_{{sorting_key}}_processed) AS first_event_{{ sorting_key }}_processed,
    MAX({{ sorting_key }}) AS last_event_{{ sorting_key }}_processed
  FROM union_all_events
  GROUP BY {% for table_pk_field in table_pk_fields -%}
                {% if loop.length > 1 %}
                    {% if loop.last  %}
                        {{ table_pk_field }}
                    {% else %}
                        {{ table_pk_field }},
                    {% endif %}  
                {% else %}
                    {{ table_pk_field }}
                {% endif %}       
           {% endfor -%}
)
{% endif %}

, final AS (
    SELECT 
        *,
        DATE({% if flag_group_feature_active -%} last_event_{{ sorting_key }}_processed {% else -%} {{ sorting_key }} {% endif %}) AS {% if flag_group_feature_active -%} last_event_{{ sorting_key }}_date_processed {% else -%} {{ sorting_key }}_date {% endif %},
        NOW() AS ingested_at,
        to_timestamp('{{ var("execution_date", "placeholder_exec_date") }}') updated_at
    FROM {% if flag_group_feature_active -%} grouped_events {% else -%} union_all_events {% endif %}
)

select * from final;

/* DEBUGGING SECTION
is_incremental: {{ is_incremental() }}
flag_group_feature_active: {{ flag_group_feature_active }}
this: {{ this }}
country: {{ country }}
silver_table_name: {{ silver_table_name }}
table_pk_fields: {{ table_pk_fields }}
table_pk_amount: {{ table_pk_amount }}
fields_direct: {{ fields_direct }}
mandatory_fields: {{ mandatory_fields }}
events_dict: {{ events_dict }}
events_keys: {{ events_keys }}
sorting_key: {{ sorting_key }}
macros_version: silver_sql_main_builder_braze & silver_sql_fields_builder_braze
*/
{% endmacro %}
