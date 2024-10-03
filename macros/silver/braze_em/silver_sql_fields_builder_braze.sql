{%- macro silver_sql_fields_builder_braze(target_fields=[], event_fields=[], mandatory_fields=[], sorting_key='ocurred_on', flag_group_feature_active=False, is_silver=False, ignore_fields=[]) -%}
{%- for target_field in target_fields if target_field not in ignore_fields -%}
    {%- if flag_group_feature_active and is_silver and target_field == sorting_key -%}
        last_event_{{ sorting_key }}_processed AS {{ sorting_key }},
    {%- elif flag_group_feature_active and (not is_silver) and target_field == sorting_key -%}
        {{ sorting_key }} AS last_event_{{ sorting_key }}_processed,
    {%- else -%}
        {%- if target_field not in event_fields -%}
            NULL as {{ target_field }}, 
        {%- elif target_field not in mandatory_fields -%}
            {{ target_field }}, 
        {%- endif -%}
    {%- endif %}
{%- endfor %}
{%- for mandatory_field in mandatory_fields if mandatory_field not in ignore_fields -%}
    {%- if flag_group_feature_active and is_silver -%}
        last_{{ mandatory_field }}_processed
    {%- elif flag_group_feature_active -%}
        {{ mandatory_field }} AS last_{{ mandatory_field }}_processed
    {%- else -%}
        {{ mandatory_field }}
    {%- endif %}
    {%- if not loop.last -%},{%- endif %}
{%- endfor -%}

{%- endmacro -%}