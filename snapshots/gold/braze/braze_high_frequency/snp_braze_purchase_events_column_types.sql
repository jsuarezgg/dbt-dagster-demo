{% snapshot snp_braze_purchase_events_column_types %}

{{
    config(
      target_schema='gold',
      file_format='delta',
      strategy='check',
      check_cols=['column_type'],
      unique_key="custom_column_name_id")
}}

{%- set data_sources_config_dict = return_data_sources_config_braze('snp_braze_purchase_events_column_types') -%}
{%- set select_statements = generate_wide_to_long_format_statements(data_sources_config_dict) -%}

SELECT
    DISTINCT
    MD5(column_name) AS custom_column_name_id,
    column_name,
    column_value AS column_type
FROM (
{{ select_statements }}
) AS final
WHERE column_value IS NOT NULL

{% endsnapshot %}
