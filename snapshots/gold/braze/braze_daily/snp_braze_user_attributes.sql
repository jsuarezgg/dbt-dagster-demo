{% snapshot snp_braze_user_attributes %}

{%- set data_sources_config_dict = return_data_sources_config_braze('snp_braze_user_attributes') -%}
{%- set select_statements = generate_wide_to_long_format_statements(data_sources_config_dict) -%}

{{
    config(
      target_schema='gold',
      file_format='delta',
      strategy='check',
      check_cols=['column_value'],
      unique_key= 'custom_' ~ data_sources_config_dict.primary_key.alias ~ '_column_name_pairing_id'
      )
}}
SELECT
    {{ dbt_utils.surrogate_key([data_sources_config_dict.primary_key.alias, 'column_name']) }}
      AS custom_{{ data_sources_config_dict.primary_key.alias }}_column_name_pairing_id,
    {{ data_sources_config_dict.primary_key.alias }},
    column_name,
    column_value
FROM (
{{ select_statements }}
) AS final
WHERE column_value IS NOT NULL

{% endsnapshot %}
